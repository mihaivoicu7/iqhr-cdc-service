#!/usr/bin/env node
/** Reviewed TEST-only recovery of the 2026-09-22 address insert. Never a startup action. */
import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { connectTest } from './lib/test-sql.mjs';

const oldTables = ['dbo.HR_Employee', 'dbo.HR_EmployeeContractInfo'];
const nextTables = [...oldTables, 'dbo.HR_EmployeeAddress', 'dbo.HR_EmployeeAllocation'];
const configurationHash = tables => createHash('sha256').update([
  'TECHNO', 'employee-contract-info', '192.168.99.133:1433', 'tdev_technophar',
  [...tables, 'dbo.IQHR_CdcHeartbeat'].sort().join(','),
].join('\n')).digest('hex');
const pool = await connectTest();
try {
  const backup = (await pool.request().query(`
    select * from dbo.IQHR_CdcActivation;
    select * from dbo.IQHR_CdcOffset;
    select * from dbo.IQHR_CdcSchemaHistory order by storage_sequence;
    select * from dbo.IQHR_CdcEvent where tenant_id=N'TECHNO' and source_id=N'employee-contract-info';
  `)).recordsets;
  assert.equal(backup[0].length, 1); assert.equal(backup[1].length, 1);
  assert.equal(backup[0][0].tenant_id, 'TECHNO');
  assert.equal(backup[0][0].source_id, 'employee-contract-info');
  assert.equal(backup[0][0].configuration_hash, configurationHash(oldTables), 'Unexpected configuration: review instead of resetting state');
  console.log(JSON.stringify({ oldHash: configurationHash(oldTables), nextHash: configurationHash(nextTables),
    currentOffset: JSON.parse(backup[1][0].offset_val), historyRows: backup[3].length,
    replayOffset: { commit_lsn: '0006da1a:00000130:0003', change_lsn: '0006da1a:00000130:0002', event_serial_no: 2 } }));
  if (process.argv[2] === '--apply') {
    const output = process.env.CDC_CUTOVER_BACKUP;
    assert.ok(output, 'Set a new private CDC_CUTOVER_BACKUP path');
    const receipt = JSON.parse(readFileSync(process.env.CDC_DRAIN_RECEIPT, 'utf8'));
    assert.equal(receipt.queue, 'iqhr.cdc.TECHNO.history');
    assert.equal(receipt.serviceState, 'exited');
    assert.equal(receipt.MessageCount, 0); assert.equal(receipt.DeliveringCount, 0);
    assert.equal(receipt.ConsumerCount, 0);
    const age = Date.now() - Date.parse(receipt.checkedAt);
    assert.ok(age >= 0 && age < 120000, 'Fresh stopped-service/empty-queue receipt required');
    writeFileSync(output, JSON.stringify(backup), { mode: 0o600, flag: 'wx' });
    const runner = fileURLToPath(new URL('./maintenance/run-onboard.sh', import.meta.url));
    const result = spawnSync(runner, ['--apply'], { stdio: 'inherit', env: process.env });
    if (result.error) throw result.error;
    assert.equal(result.status, 0, 'Onboarding did not commit; preserve backup and inspect the fixed diagnostic');
    console.log('Reviewed onboarding committed; restart with the four-table allowlist.');
  }
} finally { await pool.close(); }
