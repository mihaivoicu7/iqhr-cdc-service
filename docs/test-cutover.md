# TECHNO pilot cleanup and employee onboarding

The user requested removal of the synthetic CDC tables and narrowed business capture to `dbo.HR_Employee` and `dbo.HR_EmployeeContractInfo`. The initially requested name `HR_EmployeeContractDetails` does not exist in this database; the user confirmed `HR_EmployeeContractInfo`.

The internal heartbeat, history, activation, status, offset, schema-history and Flyway tables remain required infrastructure. The heartbeat never appears in public history. Only the two synthetic `CodexCdcProbe` tables, their SQL Server capture instances/functions/change tables, and their 594 synthetic history rows are removed. Existing business tables and business history are preserved.

## Reviewed cutover

1. Enable CDC on `HR_Employee` without altering employee records. Its composite primary key is `ID,StartDate`.
2. While the old service is running, wait for an actual persisted heartbeat checkpoint at or after the new capture instance's minimum LSN. This saved replay tuple defines the newly added table's onboarding boundary; earlier employee changes are not backfilled.
3. Stop the service cleanly. Read back that its durable queue has zero messages, deliveries and consumers. Do not discard genuine pending messages.
4. Back up activation, complete offset rows, schema history and synthetic events to a private file. The explicit maintenance command checks the stopped-service queue receipt is recent and obtains the database-wide pipeline lock.
5. In one JPA transaction, compare the existing activation hash/fence and exact saved offset against the backup, remove only mapped synthetic history records, change the configured-table hash and increment the fence. Execute only the fixed CDC-disable/table-removal DDL through the same transaction. The empty synthetic source tables are permanently removed, never recreated by this operation.
6. Set `CDC_TECHNO_TABLES=dbo.HR_Employee,dbo.HR_EmployeeContractInfo` and restart normally. Keep source ID, incarnation, creation time, offsets and schema-history records unchanged. Verify employee schema discovery and real heartbeat checkpoint advancement.

Pinned Debezium 3.2.7 `SqlServerStreamingChangeEventSource.getChangeTablesToQuery()` adds an included table absent from recovered schema by reading its metadata and recording a CREATE schema event before consuming changes. Normal `no_data` restart therefore preserves existing contract-table continuity. No schema recovery snapshot, latest-position reset or offset deletion is used. Avoid table DDL during this short onboarding procedure.

## Operator command

This is an explicitly invoked TEST maintenance tool, not a Flyway migration or startup action. `scripts/cleanup-test-probes.mjs` previews by default; `--apply` requires a private backup path and a fresh broker-drain receipt. It refuses unexpected configuration/state and does not silently rerun a completed cutover. The generic synthetic acceptance provisioning script must not be rerun against this cleaned deployment.

Native SQL is retained only for SQL Server CDC setup/removal, empty-fixture schema checks and pipeline locking; JPA handles activation and history changes using existing mappings. The Node wrapper performs read-only state backup/validation and delegates the atomic mutation to the standalone Java maintenance command.

## Execution evidence

Completed on 2026-09-22 in TECHNO TEST:

- Enabled `dbo.HR_Employee` CDC at minimum LSN `0006d9f6:000000c0:00f0`. No employee records were changed for testing. The cutover guard verified there were no employee changes at or before its onboarding checkpoint.
- After a clean stop, broker readback at 10:57:08 UTC confirmed zero queued messages, deliveries and consumers. A private backup preserved activation, complete offsets, schema history and all 594 synthetic history records before mutation.
- The atomic maintenance operation removed exactly 594 synthetic history records and the two empty probe source tables plus their CDC objects. Source incarnation `a6b245d4-53b2-4831-9ccd-ef1fb897f7a4` and activation time `2026-09-22T10:03:15.444Z` remained unchanged. The complete saved replay tuple was preserved: commit `0006d9fe:00000180:0006`, change `0006d9fe:00000180:0005`, serial `2`.
- Configuration hash changed from `ea61d07baef73a2d46c09606217383d8a8b830596e98d075da33d9a2930eeae6` to `2a82a9ea73fbb3190db2aa82ba5ec20af27d862c6ba48385c656e025b91ead48`; maintenance advanced fence 4 to 5, and the restarted service acquired fence 6.
- The service restarted at 11:01 UTC with the two confirmed business tables. Schema-history records increased from four to five as Debezium discovered `HR_Employee`. The saved commit advanced to `0006da0b:00000140:0003`, proving resumed source progress without a reset. Only the two business capture instances and required internal heartbeat remain.
- The refreshed deployed Admin viewer shows zero history rows after removing the fixtures and the menu label `Istoric modificări CDC`. Its existing SQL Agent monitoring-permission warning remains visible; it is not a capture failure.
- Admin backend and frontend were rebuilt and deployed as `cdc-cleanup-20260922`; backend health returned `UP`. Image IDs: backend `9a94500404d8c633203eabf3eba8946970e7e3846ee80f6daacc32ccd7163830`, frontend `bc2e2aad079e26ffb60da30058889a414445d3c73f7cfee5c8856153ef390f6a`.
- The Copy fix supports browsers without the Clipboard API and rejected API requests, preserving focus/selection. If both automatic methods fail, an accessible selected-text fallback remains available. Before deleting fixtures, a deployed copy-and-paste check reproduced the exact serialized value `"123.4500"`. All 42 focused CDC frontend tests, focused ESLint, the production frontend build and three menu backend tests passed.
- The standalone maintenance command passed compilation and H2/JPA safety tests for scoped deletion, rollback, stale activation/checkpoint refusal, preserved schema/incarnation, bigint-string parsing and broker-receipt guards. Actual SQL Server cleanup and restart supplied the live DDL/recovery evidence above.
