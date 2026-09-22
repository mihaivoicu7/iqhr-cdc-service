# TEST deployment and verification

Current deployment: the [2026-09-22 cleanup and employee onboarding](test-cutover.md) supersedes the original capture list below. Business capture now includes `HR_Employee` and `HR_EmployeeContractInfo`; the two probe tables and all 594 synthetic events have been removed. Earlier acceptance results below remain historical evidence.

## Agreed configuration

- Source tenant: TECHNO; SQL Server 2019 Developer, 192.168.99.133, tdev_technophar.
- Business capture: dbo.HR_EmployeeContractInfo (composite primary key EmployeeID + StartDate).
- Synthetic acceptance capture: dbo.CodexCdcProbe and dbo.CodexCdcProbeOther. Only synthetic rows are inserted/updated/deleted by acceptance tests. No employee contracts are changed for testing.
- Deployment host: 192.168.99.147; existing docker-network; separate iqhr-cdc-service deployment.
- History/state/schema storage: source tenant database; start from activation, no historical snapshot; retain events for 365 days.
- Recovery target: seven days offline, with source CDC retention increased from 4,320 to 20,160 minutes (fourteen days) for catch-up margin.
- Broker: existing persistent Artemis 2.39.0; multicast address iqhr.cdc.TECHNO, exclusive durable queue iqhr.cdc.TECHNO.history. Infinite retries, no expiry, no auto-delete, paging enabled. Dedicated address-scoped account; no broker restart was needed.
- The admin CDC viewer follows existing tenant membership, CDC module enablement and VIEW/ADMIN rules, including existing administrator inheritance. Disabling the viewer does not disable capture.

## Evidence collected

- The source CDC capture/cleanup jobs and source table primary key were inspected using the configured tenant connection.
- Guarded probe-table creation and CDC enablement succeeded; no object was dropped/recreated.
- Broker readback confirmed Durable=true, Exclusive=true, AutoDelete=false, PurgeOnNoConsumers=false.
- Admin backend: 21 focused tests passed (10 JPA/query, 8 controller/access, 3 menu), normal production/test compilation passed.
- Admin frontend: 15 focused tests, focused ESLint, and the design detector passed.
- The combined admin TEST build was deployed 2026-09-22 at 09:46 UTC, with health UP. Backend image 1b7a2e1653f77ff4df2b1cd5e13cba6d5b1f17f134d3180f32041c1d988ad93d; frontend image e88099409f56566061d2525ade56eae2081e2915769bfc7f63000c84e99595ed. The concurrent launcher task owns its release record in iqhr-admin-platform/docs/application-impersonation.md. No CDC tenant was provisioned in UAT.

## Completed live acceptance — 2026-09-22

- Final service jar SHA-256: `fe790f031124bf6b74dbd35cf10c5c34a5cb250192e4f2345a4aadc1282370e8`; runtime image: `bd8f60694cdb4c9bf8803b54204780cfd3f9d270dfa06c4f920ffe2642a5ecd0`, tag `iqhr/iqhr-cdc-service:test-20260922`.
- Java 21 `mvn verify`: **21 tests passed**, including persistent Artemis restart, JPA/fencing/retention and publication/ack failure boundaries. The first live initialization exposed Debezium's legitimate `change_lsn="NULL", event_serial_no=1` initial checkpoint. A regression-tested fix accepts this only with a concrete valid commit LSN; pinned Debezium source/bytecode was independently checked. Existing activation/offsets and all applied migrations were preserved.
- Guarded Flyway migrations V1–V3 applied successfully on SQL Server. No existing table/object was dropped or recreated.
- A pre-activation run of 198 changes (`d66c5d27-53bd-442b-8196-072806ffe6a3`) remained excluded, including after all restarts. Internal heartbeat events were absent from public history. Concrete durable commit/change LSNs advanced across idle heartbeat intervals.
- Normal delivery: run `829c02f8-6c45-4e8a-9b41-317cf6f53e44`, **198/198** unique events.
- Service stopped, 198 changes committed, service restarted: run `e9a6dad0-1183-4829-898e-1dd6372a9a14`, **198/198** unique events.
- Container forcibly killed immediately after another committed batch, then restarted: run `1f5bb3e4-0335-4f06-8ee4-56083d92bbf4`, **198/198** unique events. Docker's deliberate `kill` suppressed automatic restart, so the acceptance procedure explicitly started the container afterward. Normal process-failure recovery remains configured with `restart: unless-stopped`.
- Across the three post-activation runs: **594 visible events**, comprising 195 inserts/195 updates/195 deletes in the primary probe and three of each operation in the secondary probe. Decimal precision, false→true, null→empty, inserted missing-before and deleted missing-after images were asserted from persisted history.
- CDC was enabled only for TECHNO in the TEST bundle after explicit user confirmation. No new user grants were added. The live unauthenticated API returned **401**; tenant/module/permission denial and scoped detail lookup are covered by the backend tests.
- Live browser checks passed: table suggestions backed by SQL Server JPA DISTINCT pagination; table/action combinations; a record key returning its three operations; future date interval returning zero rows and clearing back to results; detail/back preserving filters; property search and changed/all fields; typed before/after values; copy success feedback.
- Pagination: 20 rows on pages 1 and 2, no duplicate event IDs between those pages, final page 581–594 with 14 rows, and changing a filter from the last page returned to page one.
- Desktop layout and 390-pixel mobile comparison were inspected. Content stayed within the viewport and the comparison table scrolled horizontally to expose after-values. Device emulation was cleared afterward.
- Final CDC frontend tests: **31 passed**, scoped ESLint passed, Docker production build passed. Final TEST frontend image `c7294481c707d1fca41f484d6c2bfd215fc365c3bccfa6a05ea1885a07c35f9f`, tag `cdc-20260922`, adds understandable monitoring messages with optional raw diagnostics.
- Current combined TEST backend image: `987dc391fd0c289aa9a58ebbd45216ff65f90a99482c70f982526ca6bc89946c` (launcher pagination release). Frontend-only CDC deployment preserved it and the impersonation feature flag. UAT was not provisioned or enabled for CDC.

This is live short-outage/crash acceptance, not a seven-day wall-clock outage or year-long capacity soak. The 14-day retained source window is configured; sustainable rates and backup/HA recovery require operational capacity measurement.

## Run synthetic SQL acceptance

Install the optional mssql Node package in an isolated directory; production does not depend on Node. Supply CDC_MSSQL_MODULE if it is outside normal Node resolution. Put TEST credentials in a private JSON file with tenantId,server,database,username,password; do not commit it. The helper refuses any source other than TECHNO/tdev_technophar on .133.

```sh
export CDC_TEST_CONFIG=/private/path/test-config.json
export CDC_MSSQL_MODULE=/private/path/node_modules/mssql
export CDC_TEST_RUN_FILE=/private/path/cdc-run.json
node scripts/test-live.mjs inspect
# Authorized TEST setup only; creates the guarded probe and sets 14-day retention:
node scripts/test-live.mjs provision
node scripts/test-live.mjs seed
node scripts/test-live.mjs verify
node scripts/test-live.mjs status
# Use the saved pre-activation run file to prove exclusion:
node scripts/test-live.mjs verify-activation
```

Seed commits 65 inserts, 65 updates, 65 deletes in a synthetic table transaction, plus three operations on a second table. Verification asserts exactly 195 primary-table events plus three secondary-table events, exact operation counts, correct changed fields, decimal values, false→true and null→empty transitions. Repeat with a separate run file while the service is stopped and after a forced process crash.

## Capacity and durability limits

At provisioning, the source data file had 644.875 MiB allocated and 520.563 MiB used; the existing capture table reserved 0.141 MiB. The TEST Linux host had approximately 20 GiB disk free and 2 GiB available RAM. These are observations, not capacity guarantees for future workloads. After acceptance, CDC used about 279 MiB of its 1 GiB container limit and 0.44% CPU at the sampled instant; the host had 18 GiB available (85% disk used). The tenant SQL account cannot read SQL Agent job-state metrics or physical volume statistics (`VIEW SERVER STATE` is absent). The service therefore reports `DEGRADED / CDC_JOB_STATE_METRICS_UNAVAILABLE`; database-scoped capacity, retention, progress and backlog checks still run. A DBA must grant the narrowly required monitoring access before health can report fully healthy. The viewer explains that capture can continue while these checks are unavailable.

Recovery depends on intact SQL Server CDC retention, durable broker data, and tenant database history/offset/schema state. A seven-day outage target does not cover destruction of all durable copies. Source-history gaps and missing established state must fail visibly, never be silently replaced with a fresh snapshot. Backup/HA provisioning is an operator responsibility.

## Native SQL disposition

- Service SQL Server control: database-specific CDC metadata, LSN validation, SQL Agent/space monitoring and a database-scoped session lock. Retained because mapped entity lifecycle operations cannot provide these SQL Server CDC/control contracts.
- Debezium JDBC offset/schema storage: retained standard connector storage protocol with fenced writes. History/status/activation/retention use JPA. Guarded schema DDL belongs to Flyway; operational TEST provisioning is separate.
- Admin viewer: new history/status reads use tenant-routed JPA. Existing BundleJdbcService membership/module/permission reads are reused and remain parameterized JDBC. A bounded future JPA conversion would map tenants,tenant_modules,user_tenant_permissions,user_tenant_module_permissions in the master persistence unit and preserve ID-or-username matching and administrator inheritance. The tenant connection bootstrap lookup remains JDBC because it is required before constructing tenant persistence access.
