#!/usr/bin/env node
/** Explicit TECHNO pilot cutover; never runs at service startup. See docs/test-cutover.md. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { connectTest } from './lib/test-sql.mjs';

const oldTables = ['dbo.HR_EmployeeContractInfo', 'dbo.CodexCdcProbe', 'dbo.CodexCdcProbeOther'];
const newTables = ['dbo.HR_Employee', 'dbo.HR_EmployeeContractInfo'];
const hash = tables => createHash('sha256').update([
  'TECHNO', 'employee-contract-info', '192.168.99.133:1433', 'tdev_technophar',
  [...tables, 'dbo.IQHR_CdcHeartbeat'].sort().join(','),
].join('\n')).digest('hex');
const pool = await connectTest();
try {
  const before = (await pool.request().query(`
    select * from dbo.IQHR_CdcActivation;
    select * from dbo.IQHR_CdcOffset;
    select * from dbo.IQHR_CdcSchemaHistory order by storage_sequence;
    select * from dbo.IQHR_CdcEvent where tenant_id=N'TECHNO' and source_id=N'employee-contract-info'
      and schema_name=N'dbo' and table_name in (N'CodexCdcProbe',N'CodexCdcProbeOther');
    select sys.fn_cdc_get_min_lsn(N'dbo_HR_Employee') as employee_min;
  `)).recordsets;
  assert.equal(before[0].length, 1); assert.equal(before[1].length, 1);
  assert.equal(before[0][0].tenant_id, 'TECHNO');
  assert.equal(before[0][0].source_id, 'employee-contract-info');
  assert.equal(before[0][0].configuration_hash, hash(oldTables), 'Unexpected configuration: review instead of resetting state');
  const offset = JSON.parse(before[1][0].offset_val);
  assert.match(offset.commit_lsn, /^[0-9a-f]{8}:[0-9a-f]{8}:[0-9a-f]{4}$/i);
  const saved = Buffer.from(offset.commit_lsn.replaceAll(':', ''), 'hex');
  const minimum = before[4][0].employee_min;
  assert.ok(minimum && minimum.some(byte => byte !== 0), 'Enable HR_Employee CDC first');
  assert.ok(Buffer.compare(saved, minimum) >= 0, 'Wait for a real durable heartbeat checkpoint past new capture start');
  console.log(JSON.stringify({ oldHash: hash(oldTables), newHash: hash(newTables),
    onboardingOffset: offset, syntheticEvents: before[3].length, schemaParts: before[2].length }));
  if (process.argv[2] !== '--apply') process.exitCode = 0;
  else {
    const backup = process.env.CDC_CUTOVER_BACKUP;
    assert.ok(backup, 'Set a new private CDC_CUTOVER_BACKUP path');
    const receipt = JSON.parse(readFileSync(process.env.CDC_DRAIN_RECEIPT, 'utf8'));
    assert.equal(receipt.queue, 'iqhr.cdc.TECHNO.history');
    assert.equal(receipt.serviceState, 'exited');
    assert.equal(receipt.MessageCount, 0); assert.equal(receipt.DeliveringCount, 0);
    assert.equal(receipt.ConsumerCount, 0);
    const age = Date.now() - Date.parse(receipt.checkedAt);
    assert.ok(age >= 0 && age < 120000, 'Fresh stopped-service/empty-queue receipt required');
    writeFileSync(backup, JSON.stringify(before), { mode: 0o600, flag: 'wx' });
    const runner = fileURLToPath(new URL('./maintenance/run-cleanup.sh', import.meta.url));
    const result = spawnSync(runner, ['--apply'], { stdio: 'inherit', env: process.env });
    if (result.error) throw result.error;
    assert.equal(result.status, 0, 'JPA cleanup did not commit; inspect fixed diagnostic and preserve backup');
    console.log('Cutover committed: probe objects/history removed; checkpoint/schema/incarnation preserved.');
  }
} finally { await pool.close(); }
