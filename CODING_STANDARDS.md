# Coding standards

This is repository-owned policy. `AGENTS.md` is the discovery entry point; this file supplies detailed rules. Read **Core quality**, **Verification**, and the sections relevant to the task once. Repository-specific contracts in `AGENTS.md` take precedence over general design advice. No rule here depends on a parent folder, a local skill installation, or another checkout.

## Core quality

- Optimize for correct behavior, safe future changes, and total engineering effort. Clear code helps humans and agents; short code, many classes, or a named methodology do not establish quality.
- Before editing, identify the affected contract, callers, invariants, and failure cases. Follow one representative path through the relevant layers and read its existing tests. Expand investigation when evidence warrants it.
- Give each module a cohesive responsibility and a small, useful interface that hides its complexity. Keep business rules in the owning domain/service and persistence details behind repositories or gateways. Reuse the shipped architecture; do not add layers or patterns without a concrete need.
- Use domain names consistently. Prefer explicit inputs, outputs, ownership, and side effects. Model invalid states with types and boundary validation where practical; avoid ambiguous booleans, loosely typed maps, unchecked casts, and generic `Utils`/`Manager` catch-alls for unrelated behavior.
- Extract repeated business knowledge when its semantics and lifecycle are shared. Do not abstract coincidentally similar code, create single-use frameworks, or split a readable operation into tiny forwarding methods just to satisfy a size rule.
- Keep changes cohesive and reviewable. Repair straightforward debt in code being changed when behavior can be preserved and verified. Remove obsolete branches, stale comments, and dead code in that surface. Separate broad architectural changes; never overwrite another task's edits or rewrite unrelated files for style.
- Validate external input at boundaries; enforce authorization and tenant isolation on the backend. Preserve API shape, versioned IDs, temporal semantics, ordering, pagination, and existing error contracts unless the task changes them explicitly.
- Make failures actionable. Preserve causes and distinguish absent data from failed operations. Never swallow exceptions, silently skip required dependencies, return fake success, or add a fallback that changes business behavior. Bound retries and make repeated side effects idempotent.
- Comments explain non-obvious intent, constraints, or tradeoffs. Keep enduring contracts in repository documentation close to their owner; do not store chat transcripts, generic tutorials, or speculative requirements as policy.
- Use existing formatters, linters, libraries, and locked dependency versions. Check manifests before choosing commands. Do not add dependencies or suppress warnings merely to bypass a problem; justify a needed dependency or a narrowly scoped suppression.

## SQL predicates and query design

- Do not introduce or modify SQL `IN` or `NOT IN` predicates. This includes SQL files, native queries, JPQL/HQL, Criteria membership filters, derived `*In`/`*NotIn` repository methods, and collection APIs intentionally generating those filters. Moving an `IN` into ORM syntax, dynamic SQL, or a stored procedure does not satisfy this rule. Inspect emitted SQL when changing collection filters.
- Fix existing membership predicates when the query is changed. Do not churn unrelated database definitions, protected legacy integrations, or applied migrations to make an inventory empty. Inventory findings are a backlog, not authorization to deploy or rewrite all queries.
- For one value use parameterized equality. For a small fixed set use explicit parameterized equality alternatives when appropriate. For relational membership prefer a correlated `EXISTS`/`NOT EXISTS` through the existing mapped relation. Preserve the query's tenant and authorization constraints inside the relation.
- A join is appropriate when the relationship and result shape require it. Do not replace membership with a join that multiplies rows, changes counts/pagination, or then masks the problem with `DISTINCT`.
- `NOT IN` and `NOT EXISTS` differ with nulls. Prove the intended behavior for nullable outer values, nullable set values, duplicates, empty sets, ordering, and current/history rows before replacing a predicate. Preserve established semantics unless a behavior change is explicitly requested.
- For large external sets, use an established bounded set-based mechanism, such as a mapped staging relation or a justified database-specific table-valued parameter gateway. Assess JPA first in JVM projects. Do not create query-per-ID loops, unbounded equality chains, or new domain tables solely to satisfy this rule without assessing the design and scope.
- Bind values; validate unavoidable dynamic identifiers against an allowlist. Select required columns, bound data volume, and use deterministic ordering for pagination. Avoid accidental cross-tenant joins, N+1 queries, unbounded fetches, and functions on indexed filter columns without a concrete reason. Do not add `NOLOCK` or change isolation/locking to hide performance problems.
- Query form is a project convention, not proof of performance. For consequential query changes, use representative result checks and the target database's plans/measurements where available. Never claim an `EXISTS` or JPA rewrite is faster without evidence.
- For changed `.sql` files, run `python3 scripts/quality/check_sql_predicates.py` from the repository root (or pass `--root`). For a branch review use `--base <verified-base> --head HEAD`. Without Git use `--files <changed-sql-files>`. This check flags static predicates overlapping added/replaced lines in SQL files; deletion-only edits can escape it. Manually inspect whole changed queries, embedded/dynamic SQL, ORM filters, and native gateways as part of verification. Never present this lexical check as full SQL validation.

## JVM persistence and native SQL

JPA applies to JVM application persistence, not database-only, .NET, Python, Swift, or shell projects.

- Evaluate mapped entities and Spring Data JPA first: derived queries, JPQL, Criteria/Specifications, projections, and entity lifecycle operations. This also covers legacy feature migrations, imports, and application-level data movement/backfills. Legacy SQL specifies behavior to preserve, not the implementation to copy.
- Before adding or extending native SQL, inspect existing mappings and repositories. Add a practical missing table-backed mapping. Missing mappings, convenience, or an existing native-query precedent alone do not justify bypassing JPA. `EntityManager.createNativeQuery` and SQL wrapped in a repository are still native SQL.
- Keep guarded DDL, triggers, schema metadata, and migrations running before JPA startup or against historical schemas in the existing SQL migration framework. Prefer JPA for application/data migration when stable mappings and a suitable transaction/lifecycle exist. Never make historical migrations depend on evolving application entities, edit applied migrations, or replace Flyway with Hibernate schema auto-update.
- Creating a procedure/function with runtime business logic is also an application-design change. The DDL exception does not justify moving new business behavior into SQL; evaluate a JPA-backed service. Preserve repository-specific SQL-view prohibitions and protected legacy integration boundaries.
- Retain native SQL only for a concrete requirement: a database-specific operation, a stored-procedure contract, proven locking needs, or measured bulk-performance constraints. Explain why the JPA alternative cannot safely preserve it. Isolate the exception in a focused gateway, bind values, and allowlist dynamic identifiers.
- Preserve tenant routing, authorization, business versus physical row IDs, current/history flags, trigger behavior, transactions, locking, pagination, and result shape. Check flush/clear/refresh needs when mixing JPA and direct SQL. Concurrency-sensitive replacements need evidence of equivalent locking behavior.
- Convert straightforward, behavior-preserving cases within the task. If replacement requires a broader domain/schema change or touches a protected legacy integration, propose a bounded follow-up with the concrete mapping/repository approach. Ask only when the decision materially changes scope or correctness; a review-only task does not authorize code changes.
- Every task that changes, relies on, or reviews a native SQL path must identify it in the final response and any PR description: file/method, purpose, and disposition (converted to JPA; retained with a specific reason; or follow-up with a concrete JPA approach and scope). Group related paths. Do not scan unrelated code merely to produce an inventory.
- Every code review must inspect affected persistence and migrations, including `nativeQuery = true`, `createNativeQuery`, JDBC/templates/connections, `@Subselect`, procedures, SQL-view dependencies, and SQL assembled/delegated through helpers. New or expanded unjustified native SQL is a policy finding with a JPA alternative. Surface justified exceptions and pre-existing opportunities separately; existing native SQL alone does not prove a correctness bug.

## Database changes

- Never drop an existing database object to recreate it from a migration or setup script. Guard creation (`if object_id(...) is null` on SQL Server) and additive changes. Fail clearly on an incompatible existing shape and require an explicit reviewed migration; do not delete objects or data automatically.
- Keep applied migrations immutable. Add forward migrations in the project's existing mechanism. Preserve triggers, grants, schema dependencies, and historical behavior; a database definition export is not automatically a safe deployment script.
- Check the behavior and recoverability of data changes, not just SQL syntax. Use a representative isolated database when available. Do not execute migrations against shared/live databases merely to validate code.

## Frontend and other runtimes

- Start from the shipped components, tokens, copy, interaction patterns, and repository CRUD conventions. Preserve accessibility, localization, permission checks, and narrow data refreshes. Reuse shared forms, tables, selectors, and error handling before adding bespoke implementations.
- Keep redesign proportional to the requested surface. Use a relevant design skill where helpful; a label/layout fix does not require product discovery, a new visual identity, or a shared-component rewrite. Verify affected interactions and representative layouts when browser access is available, fix observed defects, and stop discretionary polishing once the task works.
- Prevent stale asynchronous results from updating the wrong record/page. Handle loading, empty, validation, and failure states through existing patterns. Avoid duplicated state and unstable effects; keep expensive work and large data out of render paths.
- In Java/Kotlin, use existing DTO/mapping, validation, and transaction conventions; keep controllers thin and side-effect ownership clear. In non-JVM projects use their existing data abstractions, transaction model, framework conventions, and test tools; do not add JPA or a new ORM as a policy exercise.
- In scripts and deployment configuration, quote paths/arguments, preserve exit failures, and make destructive operations explicit. Keep credentials and private runtime data out of code, logs, and committed fixtures. Verification does not grant permission to publish images, deploy, send messages, or change live services.

## Verification

- Check commands and side effects against current manifests/scripts and obey repository-specific execution restrictions. Run applicable required checks and the smallest meaningful tests covering changed behavior. Reuse valid results; expand/repeat only after new changes, failures, integration risk, or an explicit requirement.
- New behavior and bug fixes need useful behavior/regression tests. Prefer externally observable contracts over private-method tests, implementation-mirroring mocks, incidental snapshots, and arbitrary coverage targets. Documentation and low-impact cosmetic edits do not require such tests.
- For persistence changes cover relevant result shape, authorization/tenant boundaries, current/history identity, null/empty cases, transactions, and locking. Use the actual supported database for database-specific behavior when available; an in-memory substitute or compilation alone does not prove trigger/SQL semantics.
- Before completion inspect the diff for accidental scope expansion, dead code, duplicated rules, swallowed errors, unsafe data changes, and changed SQL/ORM predicates. Check project-specific contracts and update relevant documentation/tests together.
- Report what changed, why, checks actually run, and remaining limitations. If a required check is blocked, name the command, cause, and resulting uncertainty; complete unaffected work. Never report an unrun check as passing or describe an essential unverified behavior as verified.

## Efficient agent workflow

- Complete authorized work through implementation and relevant verification. Resolve routine reversible choices from the brief/code; ask only for information changing correctness, scope, or an external commitment. Reuse prior authorization and prepare a concrete reviewable result before a necessary approval.
- Treat follow-ups as steering unless the user cancels/replaces the task. Preserve accepted decisions, completed work, and remaining requirements across interruptions/compaction. Give concise updates on meaningful outcomes and uncertainty.
- Search with `rg` in relevant paths first. Batch independent reads, limit outputs, read guidance once, and reuse findings. Prefer one end-to-end path over reading the entire repository. Do not repeatedly scan unchanged code, collect unused context, rerun successful checks, or create speculative abstractions to save immediate tokens.
- Delegate bounded independent work only when allowed by session/project rules and its benefit exceeds copied context, coordination, and verification. Start with one helper; name objective, read/edit ownership, acceptance criteria, constraints, and checks. Pass minimal context, assign one writer per resource, serialize dependent edits/shared Git operations, and integrate results. Subagents must not delegate further without the main agent assigning the split. Keep small/tightly coupled work local; avoid duplicate exploration. Use known capability/cost when selecting a model, otherwise the configured default.
- Work in the existing working tree only. Never create, use, or recommend Git worktrees, including for delegated work. Choose a non-worktree alternative; if none exists, stop and explain the boundary.
- Use the session skill catalog for names/paths; do not maintain a second static catalog. Follow named skills, otherwise use the smallest relevant set based on the workflow. Inspecting a skill does not execute it. Resolve references within its package, avoid rereads, and announce first use briefly.
- Session instructions take precedence over conflicting skill guidelines, subject to higher-priority instructions. Do not invent approval steps from recommendations or automatically treat policy exceptions as permission requests. If a skill blocks authorized work, link the exact file, quote the rule, distinguish requirement from interpretation, and continue independent work. Check a missing named skill's catalog/likely alternate path once; use a suitable available equivalent, and stop only dependent work if essential. Do not install/modify skills for stale references.
- All required guidance must live in this project and be version-controlled with its code. Update applicable project copies when changing shared policy and preserve local contracts. Verify new policy/tool files appear in the repository change; never rely on a parent AGENTS file, an absolute local policy path, an uncommitted shared folder, or a symlink into another checkout.

## Repository map and checks

These are observed entry points, not a requirement to read the whole project or run every command. Recheck the owning manifest before execution and select checks for the changed behavior.

- Source entry points: `src/main/java/`, `scripts/`.
- Consult relevant project contracts/design: [README.md](README.md).
- Existing test locations: `src/test/`.
- JVM build/test definition: `pom.xml`; candidate test command `mvn test`. Inspect profiles/plugins and narrow to relevant tests where supported; do not confuse packaging with behavioral verification.

The predicate checker and its fixtures are local and dependency-free apart from Python 3 and Git for diff mode. Verify the checker itself with `python3 -m unittest discover -s scripts/quality -p "test_*.py"`. It is not automatically wired into CI; the review rules require it for SQL changes.
