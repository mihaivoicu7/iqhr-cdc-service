#!/usr/bin/env node
/** Read-only TEST proof that retained address/allocation CDC records reached tenant history. */
import assert from 'node:assert/strict';
import { connectTest, sql } from './lib/test-sql.mjs';

const pool = await connectTest();
try {
  let expected = 0;
  for (const table of ['HR_EmployeeAddress', 'HR_EmployeeAllocation']) {
    // Identifiers come only from the fixed TEST allowlist; no row values are selected or printed.
    const counts = (await pool.request().input('table', sql.NVarChar, table).query(`
      select count_big(*) expected,
        coalesce(sum(case when matches.n=0 then 1 else 0 end),0) missing,
        coalesce(sum(case when matches.n>1 then 1 else 0 end),0) duplicated
      from cdc.dbo_${table}_CT c
      cross apply (
        select count_big(*) n from dbo.IQHR_CdcEvent e
        where e.tenant_id=N'TECHNO' and e.source_id=N'employee-contract-info'
          and e.schema_name=N'dbo' and e.table_name=@table
          and convert(binary(10),replace(e.commit_lsn,':',''),2)=c.__$start_lsn
          and convert(binary(10),replace(e.change_lsn,':',''),2)=c.__$seqval
          and e.operation=case c.__$operation when 1 then 'DELETE' when 2 then 'INSERT' when 4 then 'UPDATE' end
      ) matches where c.__$operation in(1,2,4);
    `)).recordset[0];
    console.log(JSON.stringify({ table, ...counts }));
    expected += Number(counts.expected);
    assert.equal(Number(counts.missing), 0, `${table}: retained CDC changes missing from history`);
    assert.equal(Number(counts.duplicated), 0, `${table}: duplicate history coordinates`);
  }
  assert.ok(expected > 0, 'Expected the retained address insert; an empty source is not a passing test');
  console.log('PASS: retained new-table events have exactly one history match.');
} finally { await pool.close(); }
