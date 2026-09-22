# Adding SQL Server CDC tables

## Admin workflow

Open **CDC → Tabele CDC** for the selected tenant. The service discovers business tables already enabled in SQL Server; internal service tables are excluded. Discovery does not automatically activate capture. A CDC administrator can choose **Enable capture** for an eligible, freshly observed table. View-only users can inspect status without changing the allowlist.

The API records a durable request in the tenant database. The service validates the capture instance and retained position, stops its old reader cleanly, and waits for an internal heartbeat written after the request to appear in SQL Server CDC, then atomically records the new allowlist and activation boundary before restarting. The existing global checkpoint is unchanged, so previously active tables catch up normally. `PENDING` and `ONBOARDING` are not active capture. `ACTIVE` appears only after the reader has started and made durable progress past that table’s activation boundary. Missing/replaced capture instances or unproven continuity remain visibly blocked rather than silently resetting history.

**Enable starts future capture at the recorded activation boundary. It does not import earlier SQL Server CDC history.** This is deliberately separate from the incident-specific recovery below. Events at or before the table's activation commit are excluded. Do not treat a pending request as an active guarantee; wait for `ACTIVE` before relying on new-table capture.

Existing configured tables bootstrap into the durable registry without changing source identity, incarnation or checkpoint. Once initialized, the registry owns the explicit table allowlist; the environment table list is for initial provisioning. Admin neither enables SQL Server CDC nor creates its storage tables. The service owns guarded schema migrations.

## SQL Server setup and operational recovery

`sys.sp_cdc_enable_table` starts SQL Server capture for that table. It does not change this service's explicit tenant table allowlist. Both layers must include the table. Use the Admin workflow after SQL enablement; do not edit activation hashes, clear offsets, or change the environment allowlist to bypass an established source's identity/configuration guard.

Configure the complete list before initial activation whenever possible. For an existing source requiring recovery of already-retained events, use an explicitly reviewed recovery operation (the normal Admin enable workflow does not rewind):

1. Enable the intended SQL Server table and verify its capture instance, primary key, retained minimum position and schema. Freeze table DDL during onboarding.
2. Identify whether any events already occurred while the table was excluded. If so, preserve a retained replay position before its first event, covered by all configured tables. Do not jump to the latest position or use a current-row snapshot as a substitute for past inserts, updates and deletes.
3. Stop the service cleanly and verify its durable queue is drained. Back up activation, the complete offset row, schema history and existing history privately.
4. Hold the same database-wide pipeline lock as the service. Validate the old configuration, incarnation, fence and exact checkpoint against the backup. Validate source coverage at the proposed position. Use one JPA transaction to update the allowlist hash and fence and, only when explicitly reviewed, the replay coordinates. Preserve source identity, incarnation, schemas and existing history.
5. Apply the new tenant allowlist and restart normally. Verify new-table schema discovery, source progress, queued delivery and matching history events, including the already-retained changes. Replay can redeliver events, so downstream subscribers must be idempotent.

Refresh the history list and clear filters when checking a new event. A table with no stored events may not yet appear among history-based table suggestions; **Tabele CDC** independently lists source inventory, including quiet tables.

## TECHNO address/allocation onboarding, 2026-09-22

The user enabled `HR_EmployeeAllocation` and `HR_EmployeeAddress` after the initial two-table rollout. SQL Server retained one address insert at `0006da1a:00000188:0008`, but the service excluded both tables and had advanced past that position. This was a configuration omission, not a failure of SQL Server CDC.

The reviewed replay boundary is a real retained internal heartbeat update: commit `0006da1a:00000130:0003`, change `0006da1a:00000130:0002`, serial `2`. Both new capture instances cover it. Before applying the operation, guards require no new-table changes at or before that boundary and no existing business history in the replay interval. The earlier contract event is preserved. No business source data is edited by onboarding.

`scripts/onboard-test-tables.mjs` previews the state by default. Its explicit `--apply` delegates to `scripts/maintenance/run-onboard.sh`, using `CDC_TEST_CONFIG`, a fresh `CDC_DRAIN_RECEIPT` and a new private `CDC_CUTOVER_BACKUP`. The Java operator is an incident-specific TEST recovery tool, not a general instruction to rewind arbitrary sources. It refuses different state or a second application. `run-onboard.sh --self-test` exercises the JPA transaction guards without infrastructure access.

JPA owns the activation and reviewed offset-row changes. Native SQL is retained for SQL Server CDC position/heartbeat verification and the existing application-lock/retention diagnostics; these are database-specific control operations. The Node wrapper uses read-only queries to preserve the exact private backup.

The reviewed recovery committed successfully on 2026-09-22. The source incarnation stayed `a6b245d4-53b2-4831-9ccd-ef1fb897f7a4`; the configuration hash became `b9bb9c104d8c4872b797ece2bfd82ac5e0b9eddd8e0cc043fa889e9e2635a0a8`; the restarted reader acquired fence 8 and recorded both new table schemas (seven schema-history rows in total). `scripts/verify-test-onboarding.mjs` failed before recovery with one missing address event, then passed with exactly one matching event and no duplicate. Allocation had no source events to recover. Existing contract history was preserved.

The user separately authorized a controlled HR Manager employee edit. Changing only `PlaceOfBirth` produced an `HR_Employee` update in history after 4.447 seconds; clearing the test marker produced another update after 1.374 seconds. Because the form clears text to an empty string, a guarded JPA update restored the exact original null afterward. This confirmed that the employee pipeline works; no original employee edit existed in SQL Server's change table at the time of diagnosis.

## Durable registry rollout evidence (2026-09-22)

Deployed service image `iqhr/iqhr-cdc-service:tables-20260922` and Admin backend/frontend images tagged `cdc-tables-20260922` to `.147` TEST. The running service JAR SHA-256 is `9f225f8f1af31ba82e5ab6c9e025893870ec62fb23f77211bd7cc29eeba0db5a`.

* Service `mvn verify`: 32 tests passed, including concurrent Admin/observer writes, atomic onboarding, exact unchanged checkpoints, per-table activation filtering, capture-generation replacement and lost-registry rejection. Independent review findings about activation boundaries, ordered primary keys and first-claim initialization were corrected and covered.
* Admin: 35 focused backend tests; 72 CDC frontend tests before the status-collapse follow-up, 11 focused status/model tests after it, and three real App/AppShell routing tests. Scoped lint and production builds passed. These are separate test runs, not a summed unique-test count.
* Existing four-table state bootstrapped through guarded V4 with unchanged incarnation and history. No SQL Server business capture instance was recreated.
* A guarded, stopped-reader JPA acceptance operation temporarily made the zero-event Allocation source available for selection while preserving the exact source checkpoint and all history. Its source CDC table and history were verified empty before and after this check. No business row was modified for onboarding acceptance.
* The real Admin Enable button persisted Allocation as `PENDING` at `12:15:55.643Z` while the service was stopped. Restart applied it with activation boundary `0006da29:000001e0:0003`; the UI's automatic polling showed `ACTIVE` at `12:17:07.583Z`. The service used a post-request captured heartbeat without rewinding the existing global offset.
* An additional forced process kill and restart preserved all four active selections, the request/boundary, the original source incarnation and all five existing event IDs. No duplicate history appeared; the original address insert still had exactly one matching event. The earlier employee test field remained its original null.
* Live UI verification covered nested CDC navigation, SQL/service state separation, available→pending→active, table-name and state filters, empty results, and disabled pagination boundaries for the four-row inventory. Multi-page request arguments and cancellation are automated-test coverage; the four-table live inventory does not exercise a second inventory page. Existing history pagination was verified in the earlier fixture acceptance runs.
* The capture status panel defaults to a roughly 45-pixel summary, expands on click and collapses again. The known SQL Agent monitoring permission warning remains visible as limited monitoring. No healthy status is fabricated.
* Final checks: Admin health `UP`; durable history queue had one consumer and zero queued/in-flight messages. CDC used about 270 MiB of its 1 GiB limit at idle. These point-in-time measurements are not a seven-day outage or capacity-soak proof.

First-ever source initialization interrupted before its first complete durable offset deliberately remains fail-closed and needs reviewed recovery. Established sources resume normally from their durable state; expired source retention or lost SQL/broker storage cannot be recovered without appropriate backups.
