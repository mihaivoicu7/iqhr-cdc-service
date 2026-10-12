# Quality baseline

Inventory date: 2026-10-08. This is a bounded lexical baseline, not a security audit, a correctness verdict, or a complete SQL parser. Source references describe existing code; fix a query when it is changed, with behavior checks. No runtime query was rewritten by this standards rollout.

## Observed SQL candidates

| Source category | IN/NOT IN occurrence lines | Native marker lines |
| --- | ---: | ---: |
| Application sources | 2 | 31 |
| Known migration paths | 0 | 0 |
| Other SQL definitions/scripts | 0 | 0 |
| Test sources | 0 | 0 |

Counts are occurrence lines, not queries or validated violations. Runtime candidates may be JPQL. Native markers are lower-bound hints; stored procedures, helper-generated SQL, dynamic SQL, C# queries, and some ORM filters are not exhaustively counted. SQL definitions/scripts include snapshots, not just deployable migrations. Zero matches do not prove absence.

Representative membership predicates to repair when touched:

- [`scripts/maintenance/CleanupTestProbes.java:94`](../scripts/maintenance/CleanupTestProbes.java): runtime_sources; use equality or a mapped/correlated existence query, preserving nulls, cardinality, tenant and current/history filters. Applied migrations require a forward migration instead of editing history.
- [`src/main/java/com/iqhr/cdc/source/SqlServerControl.java:161`](../src/main/java/com/iqhr/cdc/source/SqlServerControl.java): runtime_sources; use equality or a mapped/correlated existence query, preserving nulls, cardinality, tenant and current/history filters. Applied migrations require a forward migration instead of editing history.

## Existing native SQL paths

The following marker-bearing files are retained unchanged. This inventory does not grant an exception: before extending one, assess the concrete JPA alternative and document the purpose and disposition. For ordinary table reads/writes, inspect existing entities and implement missing practical table-backed mappings plus derived/JPQL/Criteria repositories. For connection bootstrap, database metadata, locking, or stored-procedure contracts, verify the database-specific requirement and isolate the existing gateway. `@Subselect` current-row mappings need an explicit table-backed current/history design and tests, not a mechanical replacement.

- [`scripts/maintenance/CleanupTestProbes.java:132`](../scripts/maintenance/CleanupTestProbes.java)
- [`scripts/maintenance/OnboardTestTables.java:167`](../scripts/maintenance/OnboardTestTables.java)
- [`scripts/maintenance/OnboardTestTablesSelfTest.java:95`](../scripts/maintenance/OnboardTestTablesSelfTest.java)
- [`src/main/java/com/iqhr/cdc/source/SqlServerControl.java:21`](../src/main/java/com/iqhr/cdc/source/SqlServerControl.java)

## Scoped boundaries and follow-ups

- `src/main/java/com/iqhr/cdc/source/SqlServerControl.java` performs SQL Server-specific CDC/control operations. Retain database-specific operations in this gateway; do not replace checkpoint/acknowledgment behavior with generic CRUD.

## Verification scope

The rollout verifies policy links, preserved project contracts, self-contained distribution, Git visibility, and the predicate checker fixtures. Application builds/tests and live database validation were not run because runtime behavior was not changed. Existing working-tree changes belong to their originating tasks and are preserved. Review this baseline only when its owning persistence area matters; do not load it for every edit.
