# IQHR CDC service

Java 21 / Spring Boot 3.5.16 service running one independently supervised Debezium SQL Server 3.2.7.Final pipeline per configured tenant. Each tenant retains its history, source incarnation, offsets and schema history in its own source database. No row snapshot or historical backfill runs. The Admin application reads the [integration contract](docs/integration-contract.md) with tenant-routed JPA.

## Build and run

Install Java 21 and Maven 3.9+, set `JAVA_HOME`, then run `./scripts/build.sh`. This runs the unit tests, actual H2/JPA persistence tests and a persistent embedded Artemis rollback/restart test. `target/iqhr-cdc-service-0.1.0.jar` is an executable Spring Boot jar. Maven Central provides the pinned released dependencies; `pom.xml` records exact Boot/Debezium versions and Boot manages Artemis 2.40.0. The TEST broker is independently managed.

Copy `.env.example` to `.env` and fill credentials privately. Never commit it. Review `config/application.example.yml`, especially the exact table allowlist **before first activation**. For local running, copy the example to `config/application.yml`, export the relevant environment variables and run `java -jar target/iqhr-cdc-service-0.1.0.jar`.

`docker compose --env-file .env build` builds the runtime image from the verified jar. `docker compose --env-file .env up -d` deploys only the CDC service to the existing configured Docker network. No host port is published. The initial container limit is 1 GiB, Debezium queue is 1,024 records / 64 MiB, batch is 128 records and the consumer prefetch is zero. Additional tenants require capacity measurement and potentially a larger memory allocation. To configure more tenants, mount a readable complete `application.yml` at `/app/config/application.yml` with one entry per physical database.

The service exposes only generic `/actuator/health` on internal port 8095. Row/history APIs belong to Admin and its CDC authorization gates. No credentials or row values are logged. Debezium/Kafka internal logging is disabled because error paths may include source records or configuration; service failures expose fixed diagnostic codes and exception class names. Do not enable verbose library or JDBC bind logging against employee data.

## Source and broker preparation

The service never enables CDC on or changes a business table. Its sole authorized internal source is `dbo.IQHR_CdcHeartbeat`: guarded Flyway migration creates it and enables table CDC, and JPA updates one singleton row every 30 seconds. Before activation, a database administrator must confirm database CDC and SQL Agent capture/cleanup jobs are running, the explicit business source tables have CDC enabled, and cleanup retention is at least **20,160 minutes / 14 days**. This gives a seven-day outage target with catch-up margin; retention settings alone do not reserve storage or prove recovery throughput. Measure change volume and catch-up speed before claiming the target operationally.

The SQL principal needs normal Debezium source/CDC metadata reads, `sp_cdc_help_jobs`, the SQL Server application-lock procedures, schema-history/offset writes, and CRUD on the service tables. First deployment additionally needs guarded DDL permissions for Flyway. `db_owner` is convenient for this TEST pilot but a production installation should separate migration privileges from runtime privileges. Server-level volume and SQL Agent activity diagnostics need extra server/msdb permissions. If unavailable, capture continues in `DEGRADED` with an explicit metrics-unavailable diagnostic; no false healthy status is reported.

Provision the following broker resources **before capture starts** for each tenant:

* Durable MULTICAST address `iqhr.cdc.<TENANT>`.
* Durable, exclusive MULTICAST queue `iqhr.cdc.<TENANT>.history`, bound to that address; no filter, last-value, ring, purge-on-no-consumer, temporary or auto-delete behavior.
* Address settings: `max-delivery-attempts=-1`, `expiry-delay=-1`, no forced min/max expiry, `address-full-policy=PAGE`, no automatic address/queue deletion. Never configure DROP for a CDC address.
* Durable journal, bindings, paging and large-message storage; synchronous transactional journal commits enabled. Monitor free disk, paging size and message age. Add independent subscriber queues before they need their own history.
* A tenant-scoped account with send on its address, consume on its queue, and enough permission for a read-only Core queue query. No management credential is needed by the service.

The service verifies the queue's existence and durable/exclusive/non-dropping shape before activation and periodically. Broker expiry, delivery-limit and disk/journal policies are operational prerequisites, not all available from the restricted Core queue query. A new empty queue recreated after journal loss cannot prove the old backlog survived.

## Durability and activation

`IQHR_CdcActivation` records a stable source incarnation and hash of tenant, source identity, host, database and allowlisted tables. The first successful `no_data` initialization establishes the source streaming boundary; the preceding `STARTING` period is not yet active capture. Only new changes from that boundary are recorded. All real CDC insert/update/delete records, including trigger-generated records, remain visible. Composite keys are serialized without inventing a single business ID. SQL Server does not provide application actor identity.

A database-scoped `sp_getapplock` exclusive session lock permits one live reader for the shared offset/schema tables, including across processes configured with different tenant IDs or server aliases. A new owner increments the persisted fence. JPA history/status writes and the Debezium JDBC offset/schema write statements check that generation; SQL transaction locking prevents an old owner from committing offsets over the new owner. The lock connection is never transparently reconnected. Loss of ownership stops the tenant pipeline.

For each source record:

1. Parse/validate source coordinates and construct a deterministic SHA-256 event ID from tenant, source ID, persistent incarnation, table, commit/change LSN, event serial, operation and canonical full key.
2. Send a persistent JMS message with no TTL and commit the JMS producer transaction. Its confirmed durable broker commit must return before `markProcessed`/`markBatchFinished` allows the source offset to advance.
3. The history consumer commits the tenant JPA transaction before acknowledging the JMS transaction. A duplicate event ID is a no-op. A failed/ambiguous send, DB commit or ack causes retry/replay; it never skips that record.

A poison event remains queued and reports a consumer failure. Other tenants run independently. After dependencies recover, supervised restart resumes the durable state. Clean shutdown closes capture before releasing ownership; shutdown that cannot prove worker termination exits the process rather than releasing a live reader. Container restart replays any unconfirmed boundary.

This is at-least-once delivery with one visible history row per source event, **not a guarantee against lost SQL/broker storage, expired source positions, external queue deletion or missing backups**. Back up the tenant DB (including activation/offset/schema tables) and durable broker storage; use appropriate SQL and broker HA. Do not independently restore these stores to inconsistent points without a recovery review.

## Recovery and monitoring

Status is written every 30 seconds to `IQHR_CdcStatus`, including separate `capture_state` and `consumer_state`. Admin should mark it stale after missed updates. The internal health endpoint is DOWN if any enabled tenant is uninitialized, failing, degraded or stale. A hard DB outage may prevent status writes; process health/log codes and stale status provide that signal.

Monitoring checks CDC cleanup retention, saved-position coverage across capture-instance rollover, capture job state, recent CDC scan errors, CDC scan staleness, physical volume free space when permitted, capped database-file capacity, retained-position headroom, source delivery progress and broker backlog. History retention deletes at most 500 JPA-managed rows older than 365 days per monitor iteration, scoped to the tenant. Assess capacity for both 14 days of source CDC and 365 days of JSON history; current free space cannot predict an unmeasured change rate.

`SOURCE_OFFSETS_MISSING_OR_AMBIGUOUS`, `SOURCE_INITIALIZATION_INCOMPLETE`, `SCHEMA_HISTORY_MISSING`, `SOURCE_POSITION_UNAVAILABLE`, `SOURCE_IDENTITY_CHANGED` and `SOURCE_CONFIGURATION_CHANGED` deliberately fail closed. An existing activation without complete offsets never silently starts at latest. Initial setup gets up to two minutes to establish complete durable offsets; a crash during initialization still requires review on restart.

Debezium's valid initial `no_data` boundary can contain a concrete `commit_lsn`, `change_lsn="NULL"` and `event_serial_no=1` without snapshot markers. The service accepts that exact form and resumes the saved commit boundary; it still validates source retention and schema history. Missing replay fields or a missing/invalid commit LSN remain errors.

Operator procedure for those failures:

1. Stop this tenant's capture deployment and preserve the tenant DB/broker state and fixed diagnostic code. Check source CDC retention, current min/max LSNs and backups; do not clear offset or activation tables, drop/recreate tables, rename the source ID or switch snapshot mode as a shortcut.
2. If a consistent retained source position and corresponding schema/activation state can be recovered, restore them through an explicitly reviewed recovery operation and restart with unchanged identity/configuration.
3. If the source range has expired or durable state cannot be recovered, report a history gap. Any intentional new activation/incarnation requires a separately reviewed migration and explicit acceptance of that gap. This service does not automate rebaselining.

Debezium's ordinary heartbeat records do not reliably advance a quiet SQL Server table's saved LSN. The internal `dbo.IQHR_CdcHeartbeat` row solves this by producing real committed CDC changes during business-table inactivity. The connector's effective allowlist includes that exact internal table; the parser validates its database, source coordinates and singleton key, then suppresses public history/broker output. Ordered source acknowledgement occurs only after earlier business records have confirmed persistent publication. Recovery uses the resulting actual durable source LSN, never a fabricated wall-clock offset. The heartbeat is the only service-owned capture exception; event, status, activation, offset and schema-history tables remain excluded.

Table-list and source-identity changes are intentionally blocked after activation. Adding a table at an existing checkpoint requires reviewing CDC start positions and schema history; removing the TEST probe later also needs a reviewed configuration-hash migration. Do not mutate real employee rows as a test. TEST acceptance tooling uses `dbo.CodexCdcProbe` and `dbo.CodexCdcProbeOther`; see [live deployment evidence](docs/deployment-evidence.md).

## Native SQL assessment

Application history inserts, deduplication, activation/fence lifecycle, status writes and bounded one-year retention use mapped JPA entities and entity transactions. Native SQL remains only for these concrete SQL Server/library contracts:

* `SqlServerControl`: `sp_getapplock`/`APPLOCK_MODE`, CDC metadata and LSN functions, documented `sp_cdc_help_jobs`, SQL Agent activity and storage/scan diagnostics. These server procedures/DMVs have no equivalent stable table-backed JPA lifecycle. Table names are bound as parameters.
* `DebeziumConfiguration`: standard Debezium JDBC offset/schema protocol DML, customized for SQL Server and transaction-scoped fence enforcement. Replacing the library's storage SPI with application JPA would introduce a bespoke checkpoint implementation. Identifiers are fixed; source IDs are strictly allowlisted before guard SQL is assembled. Offset delete-and-insert is an atomic library transaction, never a table drop.
* `V1__cdc_history_and_durable_state.sql`: guarded forward schema DDL and schema compatibility validation before JPA starts; no business source table DDL and no drop/recreate operation. Schema history orders by a database-generated sequence to avoid restart clock ordering errors. `V2__durable_idle_source_heartbeat.sql` guard-creates and enables CDC only on the explicitly authorized internal heartbeat table using SQL Server's required CDC setup procedure; heartbeat data updates use JPA.

Primary implementation references: [Debezium state storage](https://debezium.io/documentation/reference/3.2/configuration/storage.html), [Debezium SQL Server](https://debezium.io/documentation/reference/3.2/connectors/sqlserver.html), [Debezium Engine](https://debezium.io/documentation/reference/3.2/development/engine.html), [Artemis send/ack guarantees](https://activemq.apache.org/components/artemis/documentation/latest/send-guarantees.html). Released source jars were inspected for offset JSON encoding, atomic JDBC flushes and SQL Server partition names.

## Verification boundary

Automated verification covers typed insert/update/delete parsing, composite keys, deterministic replay IDs, tenant mismatch, missing/corrupt/foreign/incomplete source state, overlapping CDC instances, producer confirmation failure, DB/ack failure ordering, real JPA deduplication after a factory restart, fencing and bounded retention, internal heartbeat JPA updates/filtering/ordered acknowledgement, queue provisioning validation, and a real persistent Artemis rollback/restart. H2 verifies JPA behavior; it does **not** prove SQL Server lock/DMV/DDL behavior. The separate TEST deployment evidence must record live SQL Server CDC, restart/catch-up, authorization, UI and resource checks. No seven-day wall-clock outage or year-long capacity soak is simulated by the unit suite.
