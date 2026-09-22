# CDC pilot contract

Agreed scope: one separately deployable multi-tenant service, initially TECHNO / SQL Server 192.168.99.133 / tdev_technophar / dbo.HR_EmployeeContractInfo, deployed to TEST .147. SQL Server CDC is already enabled. History, durable source state, and schema history belong in each source tenant database. Start at service activation, without a data snapshot or historical backfill. History retention is one year; tolerate seven days offline with additional catch-up retention margin. All values are visible to authorized CDC readers. Existing admin membership, administrator inheritance, CDC module enablement and VIEW/ADMIN permissions apply. Disabling the viewer module does not stop capture; capture is independently configured. Never capture history/status/offset/schema/activation tables. The sole internal exception is dbo.IQHR_CdcHeartbeat, updated through JPA every30seconds and captured only to advance durable source positions during idle periods; its changes are never published as public history.

## Ownership

The CDC service owns its SQL Server schema migrations and all history/status writes. Admin reads history with tenant-routed JPA and exposes authorization-protected endpoints. No cross-service HTTP data call is required. One shared service runs independent configured tenant readers/consumers. Artemis provides one durable multicast address per tenant, allowing independent subscribers later.

## Tenant tables (explicit column names)

`dbo.IQHR_CdcEvent`: `event_id varchar(64)` primary key; `tenant_id nvarchar(64)`; `source_id nvarchar(100)`; `schema_name nvarchar(128)`; `table_name nvarchar(128)`; `operation varchar(16)` (INSERT, UPDATE, DELETE, SNAPSHOT); `occurred_at datetime2(7)` UTC; `captured_at datetime2(7)` UTC; `record_key nvarchar(max)` JSON; nullable `before_json nvarchar(max)`; nullable `after_json nvarchar(max)`; `changed_properties nvarchar(max)` JSON string array; nullable `commit_lsn varchar(32)`; nullable `change_lsn varchar(32)`; nullable `event_serial_no bigint`. Non-null unless stated. Index time/event ID, table/time/event ID, operation/time/event ID. Event identity is deterministic from tenant/source incarnation and source event coordinates, never ingestion time. Duplicate replay must not create another history row.

`dbo.IQHR_CdcStatus`: `source_id nvarchar(100)` primary key; `tenant_id nvarchar(64)`; `state varchar(32)`; nullable `message nvarchar(1000)` (no secrets or row values); `updated_at datetime2(7)` UTC; nullable `last_event_at datetime2(7)` UTC; nullable `last_commit_lsn varchar(32)`; nullable `retention_minutes int`; nullable `retention_headroom_seconds bigint`. State: STARTING, RUNNING, DEGRADED, ERROR, STOPPED. Diagnostics distinguish capture health from consumer health; service can add columns while preserving this read contract. Stale status must be displayed as stale, never unconditionally healthy.

## Admin API

All under `/api/cdc`, requiring selected tenant and `assertModulePermission(tenantId, "CDC", "VIEW")` before any access. Normal API client sends bearer and X-Tenant-ID.

- `GET /events`: page (zero-based), size (bounded), table (schema-qualified exact names joined by `||`), operation (exact enum), from/to (inclusive ISO UTC instants), recordKey (optional text search of serialized key), sort (allowlisted; default occurredAt desc, event ID as tie-breaker). Returns `{content, page, size, totalElements}`. Every query additionally restricts tenant_id to selected tenant.
- Summary: `{id, schemaName, tableName, operation, occurredAt, capturedAt, recordKey: object|null, changedProperties: string[], commitLsn, changeLsn, eventSerialNo}`. `tableName` is unqualified; table filter values are schema-qualified.
- `GET /events/{id}`: summary plus `{before: object|null, after: object|null}`. Unknown/cross-tenant event is 404.
- `GET /filter-values?field=table&query=&page=0&size=30`: `{items: ["dbo.HR_EmployeeContractInfo"], hasNext}` matching incumbent FilterValuesResponseDto.
- `GET /status`: array of `{sourceId, state, message, updatedAt, lastEventAt, lastCommitLsn, retentionMinutes, retentionHeadroomSeconds}`.

Use existing EntityCrudPage/FilterBar, read-only controls, tenant-keyed route `/cdc`. Table/action/date/record-key filters and before/after detail comparison. Changed-only toggle and field-name search in detail; distinguish absent, null, empty string, zero and false. Show all real operations including trigger-generated records. No invented actor identity: CDC alone does not identify the application user.

## Recovery requirements

Persist schema/offset state in the tenant DB. Publish persistent messages with a durable subscriber provisioned before capture starts; acknowledge source records only after broker confirmation. Commit history before acknowledging consumed messages. Durable retries and deterministic event IDs give at-least-once delivery with one visible row per event. Never skip a failed event. Explicitly detect unavailable saved positions and prevent silent resnapshot/restart-at-latest. Monitor CDC jobs, retention headroom, stale progress, storage capacity and downstream errors. No promise against destruction of all durable storage; document backup/HA boundaries. Use seven-day outage target with fourteen-day source retention for initial capacity verification/catch-up margin.

## Verification

Unit/integration coverage for duplicate replay, failures at publish/commit/ack boundaries, tenant isolation, permission/module gates, inserts/updates/deletes, exact filters and stable pagination, typed before/after comparisons. Live synthetic CDC probe tables/rows may be created on TEST for insert/update/delete/restart testing; never mutate real employee rows for testing, drop existing objects, or auto-enable additional business tables. Verify deployed UI in browser. Report test evidence and remaining limitations honestly.
