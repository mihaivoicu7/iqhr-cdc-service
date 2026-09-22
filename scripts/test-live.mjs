#!/usr/bin/env node
/** Synthetic TEST-only CDC probe; never writes HR records or drops schema objects. */
import { randomUUID } from 'node:crypto';
import { writeFileSync, readFileSync } from 'node:fs';
import assert from 'node:assert/strict';
import { sql, connectTest, testConfig } from './lib/test-sql.mjs';

const config = testConfig();
const pool = await connectTest(config);
const mode = process.argv[2] || 'inspect';
try {
  if (mode === 'inspect') {
    for (const [name, query] of Object.entries({
      database: 'select db_name() database_name, is_cdc_enabled from sys.databases where database_id=db_id()',
      jobs: 'exec sys.sp_cdc_help_jobs',
      volume: 'select total_bytes,available_bytes,volume_mount_point from sys.dm_os_volume_stats(db_id(),1)',
      files: "select name,type_desc,size*8.0/1024 allocated_mb,fileproperty(name,'SpaceUsed')*8.0/1024 used_mb,max_size,growth,is_percent_growth from sys.database_files",
      captureSize: "select sum(reserved_page_count)*8.0/1024 reserved_mb from sys.dm_db_partition_stats where object_id=object_id('cdc.dbo_HR_EmployeeContractInfo_CT')",
    })) {
      try { console.log(JSON.stringify({ name, rows: (await pool.request().query(query)).recordset })); }
      catch (error) { console.log(JSON.stringify({ name, unavailable: error.message })); }
    }
  } else if (mode === 'provision') {
    await pool.request().query(`
      if db_name() <> 'tdev_technophar' throw 51000,'TEST database required',1;
      if object_id('dbo.CodexCdcProbe','U') is null
      begin
        create table dbo.CodexCdcProbe (
          ProbeId uniqueidentifier not null primary key,
          RunId uniqueidentifier not null,
          Label nvarchar(100) not null,
          Amount decimal(19,4) not null,
          Enabled bit not null,
          OptionalValue nvarchar(100) null,
          HappenedAt datetime2(7) not null
        );
      end;
      if col_length('dbo.CodexCdcProbe','RunId') is null or col_length('dbo.CodexCdcProbe','OptionalValue') is null
        throw 51001,'Existing probe shape is incompatible; review a forward migration',1;
      if not exists(select 1 from cdc.change_tables where source_object_id=object_id('dbo.CodexCdcProbe'))
        exec sys.sp_cdc_enable_table @source_schema=N'dbo',@source_name=N'CodexCdcProbe',@role_name=null,@supports_net_changes=0;
      if object_id('dbo.CodexCdcProbeOther','U') is null
        create table dbo.CodexCdcProbeOther (
          ProbeId uniqueidentifier not null primary key,
          RunId uniqueidentifier not null,
          Label nvarchar(100) not null
        );
      if col_length('dbo.CodexCdcProbeOther','RunId') is null
        throw 51002,'Existing secondary probe shape is incompatible',1;
      if not exists(select 1 from cdc.change_tables where source_object_id=object_id('dbo.CodexCdcProbeOther'))
        exec sys.sp_cdc_enable_table @source_schema=N'dbo',@source_name=N'CodexCdcProbeOther',@role_name=null,@supports_net_changes=0;
      exec sys.sp_cdc_change_job @job_type=N'cleanup', @retention=20160;
      exec sys.sp_cdc_help_jobs;
    `);
    console.log('Provisioned guarded TEST probe and fourteen-day CDC retention.');
  } else if (mode === 'seed') {
    const path = process.env.CDC_TEST_RUN_FILE;
    if (!path) throw new Error('Set CDC_TEST_RUN_FILE before running seed so identifiers are saved.');
    const runId = randomUUID();
    const ids = Array.from({ length: 65 }, () => randomUUID());
    const transaction = new sql.Transaction(pool);
    await transaction.begin();
    try {
      for (const [index, id] of ids.entries()) {
        await new sql.Request(transaction).input('id', sql.UniqueIdentifier, id).input('run', sql.UniqueIdentifier, runId)
          .input('label', sql.NVarChar, 'CDC acceptance ' + index).query(`
            insert into dbo.CodexCdcProbe(ProbeId,RunId,Label,Amount,Enabled,OptionalValue,HappenedAt)
            values(@id,@run,@label,0,0,null,sysutcdatetime());
          `);
      }
      await new sql.Request(transaction).input('run', sql.UniqueIdentifier, runId).query(`
        update dbo.CodexCdcProbe set Amount=123.4500,Enabled=1,OptionalValue=N'' where RunId=@run;
        delete from dbo.CodexCdcProbe where RunId=@run;
      `);
      await new sql.Request(transaction).input('id', sql.UniqueIdentifier, randomUUID()).input('run', sql.UniqueIdentifier, runId).query(`
        insert into dbo.CodexCdcProbeOther(ProbeId,RunId,Label) values(@id,@run,N'Second captured table');
        update dbo.CodexCdcProbeOther set Label=N'Updated second table' where RunId=@run;
        delete from dbo.CodexCdcProbeOther where RunId=@run;
      `);
      await transaction.commit();
    } catch (error) { await transaction.rollback(); throw error; }
    const evidence = { runId, ids, expected: { INSERT: 65, UPDATE: 65, DELETE: 65 }, createdAt: new Date().toISOString() };
    writeFileSync(path, JSON.stringify(evidence, null, 2), { mode: 0o600 });
    console.log(JSON.stringify({ runId, expectedEvents: 198, runFile: path }));
  } else if (mode === 'status') {
    const result = await pool.request().query(`
      select source_id,state,message,updated_at,capture_state,consumer_state,last_commit_lsn,
        retention_minutes,retention_headroom_seconds from dbo.IQHR_CdcStatus;
      select offset_key,offset_val from dbo.IQHR_CdcOffset;
      select schema_name,table_name,operation,count_big(*) event_count from dbo.IQHR_CdcEvent
        group by schema_name,table_name,operation;
    `);
    console.log(JSON.stringify(result.recordsets));
  } else if (mode === 'verify-activation') {
    const run = JSON.parse(readFileSync(process.env.CDC_TEST_RUN_FILE, 'utf8'));
    const counts = (await pool.request().input('run', sql.NVarChar, run.runId).query(`
      select
        (select count_big(*) from dbo.IQHR_CdcEvent
          where json_value(before_json,'$.RunId')=@run or json_value(after_json,'$.RunId')=@run) as prior_events,
        (select count_big(*) from dbo.IQHR_CdcEvent where table_name='IQHR_CdcHeartbeat') as public_heartbeats;
    `)).recordset[0];
    assert.equal(Number(counts.prior_events), 0, 'Pre-activation changes must not be backfilled');
    assert.equal(Number(counts.public_heartbeats), 0, 'Internal heartbeats must not enter public history');
    console.log(JSON.stringify({ runId: run.runId, activationBoundary: 'PASS', internalHeartbeatSuppression: 'PASS' }));
  } else if (mode === 'verify') {
    const run = JSON.parse(readFileSync(process.env.CDC_TEST_RUN_FILE, 'utf8'));
    const deadline = Date.now() + 90000;
    let rows = [];
    do {
      rows = (await pool.request().input('run', sql.NVarChar, run.runId).input('tenant', sql.NVarChar, config.tenantId).query(`
        select event_id,operation,record_key,before_json,after_json,changed_properties
        from dbo.IQHR_CdcEvent
        where tenant_id=@tenant and schema_name='dbo' and table_name='CodexCdcProbe'
          and (json_value(before_json,'$.RunId')=@run or json_value(after_json,'$.RunId')=@run)
      `)).recordset;
      if (rows.length >= 195) break;
      await new Promise(resolve => setTimeout(resolve, 1500));
    } while (Date.now() < deadline);
    assert.equal(rows.length, 195, 'All committed probe changes must arrive exactly once');
    assert.equal(new Set(rows.map(row => row.event_id)).size, 195, 'No duplicate event identity');
    for (const [operation, count] of Object.entries(run.expected)) assert.equal(rows.filter(row => row.operation === operation).length, count, operation);
    for (const row of rows.filter(row => row.operation === 'INSERT')) {
      assert.equal(row.before_json, null);
      const after = JSON.parse(row.after_json);
      assert.equal(after.Enabled, false); assert.equal(after.OptionalValue, null); assert.equal(Number(after.Amount), 0);
    }
    for (const row of rows.filter(row => row.operation === 'DELETE')) {
      assert.equal(row.after_json, null);
      const before = JSON.parse(row.before_json);
      assert.equal(before.Enabled, true); assert.equal(before.OptionalValue, ''); assert.equal(Number(before.Amount), 123.45);
    }
    for (const row of rows.filter(row => row.operation === 'UPDATE')) {
      const before = JSON.parse(row.before_json), after = JSON.parse(row.after_json);
      assert.equal(before.Enabled, false); assert.equal(after.Enabled, true);
      assert.equal(before.OptionalValue, null); assert.equal(after.OptionalValue, '');
      assert.equal(Number(before.Amount), 0); assert.equal(Number(after.Amount), 123.45);
      assert.deepEqual(JSON.parse(row.changed_properties).sort(), ['Amount','Enabled','OptionalValue']);
    }
    const other = (await pool.request().input('run', sql.NVarChar, run.runId).input('tenant', sql.NVarChar, config.tenantId).query(`
      select operation from dbo.IQHR_CdcEvent
      where tenant_id=@tenant and schema_name='dbo' and table_name='CodexCdcProbeOther'
        and (json_value(before_json,'$.RunId')=@run or json_value(after_json,'$.RunId')=@run)
    `)).recordset;
    assert.deepEqual(other.map(row => row.operation).sort(), ['DELETE','INSERT','UPDATE']);
    console.log(JSON.stringify({ runId: run.runId, verified: rows.length + other.length, operations: run.expected,
      secondTableOperations: 3, typedBeforeAfter: 'PASS', deduplication: 'PASS' }));
  } else { throw new Error('Mode must be inspect, provision, seed, status, verify-activation, or verify'); }
} finally { await pool.close(); }
