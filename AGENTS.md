# Agent instructions: iqhr-cdc-service

## Required guidance

- Before implementation or code review, read [CODING_STANDARDS.md](CODING_STANDARDS.md): Core quality, Verification, and the sections relevant to the task. For persistence work also read SQL predicates, JVM persistence when applicable, and Database changes. Read once; reuse loaded context. For documentation-only work, read the relevant policy section.
- Keep correctness, maintainability, and safe future changes ahead of quick completion. Use cohesive modules with small useful interfaces; avoid speculative abstractions, duplicated business rules, swallowed failures, and unrelated churn.
- Do not introduce or modify SQL `IN`/`NOT IN`, including through JPQL, Criteria, or derived collection queries. Repair existing predicates when their query is changed; preserve null/empty-set behavior, result cardinality, tenant filters, and temporal IDs. Do not edit applied migrations or protected legacy paths to clear a backlog.
- JVM persistence is JPA-first. Native SQL needs a concrete justified requirement and explicit file/method disposition in the final response and PR description. Preserve repository-specific SQL-view boundaries.
- Never drop/recreate existing database objects from migrations or setup. Use guarded additive changes, fail on incompatible schemas, and keep applied migrations immutable.
- Finish authorized work through relevant verification. Check commands against current manifests, obey local execution restrictions, preserve other changes, and report unrun/blocked checks honestly. Verification does not authorize deployment or other external actions.
- Search relevant paths with `rg`, batch independent reads, and reuse findings/check results. Delegate only allowed, bounded independent work with clear ownership when total effort improves. Never create, use, or recommend Git worktrees.
- Required policy and tools live in this project. Keep them version-controlled with the code; do not depend on parent-folder instructions, absolute local policy paths, shared uncommitted files, or symlinks into another checkout.

## Production deployment approval

- Before a production rollout involving more than one application, ask the user whether each named application should be deployed and wait for explicit approval of the exact rollout scope. An earlier explicit approval of those applications for this rollout satisfies this requirement; a generic request such as "deploy next version" does not authorize adding another application.
- Prepare and verify the release first, then present the application names, versions, material changes, and production migrations or shared-service changes before asking. Do not begin the production rollout while approval is pending.
- Count applications by product/service, not by repository or container: the backend and frontend of the same application count as one; separate applications count separately even when they share a repository, build, library, configuration, or deployment script.
- Shared code, dependencies, compatibility requirements, accumulated changes, and convenience never authorize deploying additional applications. If another application must also change, explain why and obtain approval before deploying it. If the approved scope expands, obtain approval for the expanded scope.
- A rollout involving only one application may proceed under the user's existing deployment authorization without this additional confirmation. Verification alone never authorizes a production deployment.

## Code Review Rules

- Check the affected contract, failure cases, authorization/tenant boundaries, and meaningful behavior tests. Flag new or expanded unjustified native SQL and prohibited membership predicates; give a concrete JPA/query alternative. Inspect affected mappings, helpers, procedures, views, and migrations, and surface justified native exceptions and existing debt separately from correctness findings.
- For changed `.sql` files run `python3 scripts/quality/check_sql_predicates.py`; for a branch comparison pass `--base <verified-base> --head HEAD`. Manually review embedded/dynamic SQL and ORM-generated filters. The lexical check does not replace behavior verification.

## CDC contracts

- This is a standalone multi-tenant CDC service. Each source tenant database owns its history, activation, checkpoint and schema state.
- Never acknowledge a source event before confirmed durable broker delivery, or a broker event before committed history persistence. Retries must preserve deterministic event IDs.
- Missing established checkpoints or unavailable source history must fail visibly. Never silently resnapshot, reset to latest, or delete/recreate state.
- Exclude history, status, offset, schema and activation tables from capture. The deliberately captured internal `dbo.IQHR_CdcHeartbeat` table is the sole exception: it advances safe durable source positions during otherwise idle operation and must never produce public history noise.
- Guard all schema changes and retain repository-contained deployment and recovery documentation. No credentials, runtime data, or private test configuration may be committed.

## Component navigation and permissions

- New or modern IQHR components must not create legacy navigation scaffolding in `FRM_Menu`, `FRM_Menus`, `FRM_MenuLocalization`, `FRM_MenuAction`, or `FRM_Action`.
- In applications using the IQHR component permission model, define new components in `SEC_Component` and use the existing user, group, and role component permissions. Register pages and actions in the application's code component registry/navigation. Do not introduce the SEC schema into backend-only integrations or applications that do not use this model.
- Enforce the same effective component permissions in backend operations and UI navigation/actions. UI visibility alone is not an authorization check.
- New component grants and navigation must not depend on a legacy menu ID, menu code, or action. Keep `SEC_Component.LegacyMenuCode` and other legacy menu/action links unset for new components.
- Keep already-applied Flyway migrations immutable. Neutralize legacy links introduced for a modern feature with a new guarded forward migration, scoped to that feature; do not recreate legacy scaffolding or purge unrelated historic menus, actions, localizations, or grants.
- This rule supersedes guidance that would require legacy navigation records for new components. Preserve existing explicitly scoped legacy integrations and established legacy navigation unless the task authorizes changing them.

## Navigation

- Source/check entry points are in [CODING_STANDARDS.md](CODING_STANDARDS.md#repository-map-and-checks). Read the relevant project contracts before changing their behavior.
- [docs/quality-baseline.md](docs/quality-baseline.md) records existing persistence candidates and follow-ups. Read it only when that area is affected; findings are not authorization for unrelated rewrites.
