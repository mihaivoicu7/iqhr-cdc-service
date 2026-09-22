# AGENTS.md instructions for iqhr-cdc-service

## CDC contracts

- This is a standalone multi-tenant CDC service. Each source tenant database owns its history, activation, checkpoint and schema state.
- Never acknowledge a source event before confirmed durable broker delivery, or a broker event before committed history persistence. Retries must preserve deterministic event IDs.
- Missing established checkpoints or unavailable source history must fail visibly. Never silently resnapshot, reset to latest, or delete/recreate state.
- Exclude history, status, offset, schema and activation tables from capture. The deliberately captured internal `dbo.IQHR_CdcHeartbeat` table is the sole exception: it advances safe durable source positions during otherwise idle operation and must never produce public history noise.
- Guard all schema changes and retain repository-contained deployment and recovery documentation. No credentials, runtime data, or private test configuration may be committed.

## Working approach

- Treat requests to build, fix, review, or investigate as instructions to complete that work. Continue through implementation and relevant verification; do not stop at a plan or an offer to continue.
- Resolve routine, reversible choices from the brief and existing code. Ask only when missing information materially changes correctness, scope, or an external commitment, and continue independent work while waiting.
- Reuse authorization already given in this session. Before a necessary approval, prepare the concrete result and finish unaffected work. Respect actual permission boundaries; do not invent additional approval steps.
- Treat follow-up messages as steering of the active task unless the user cancels or replaces it. Preserve completed work, accepted decisions, and outstanding requirements across interruptions and compaction.
- Search and read only what the task needs. Use `rg`, batch independent reads, and summarize large outputs. Expand investigation when evidence warrants it, rather than repeatedly scanning the whole workspace.
- Communicate the outcome, meaningful progress, and material uncertainty in plain language. Finish with what changed, what was verified, and any remaining limitation; scale detail to the task.

## Skills

- Use the current session's skill catalog as the source of available names and paths. Do not maintain a second static catalog here. If two entries resolve to identical content, load one copy.
- Follow explicitly requested skills. Otherwise select the smallest set whose actual workflow helps the task; generic words such as "audit", "clarify", or "optimize" alone do not establish a match. Reviewing a skill means inspecting it, not executing its workflow.
- Read the selected `SKILL.md` once and load only the references needed for the current step. Resolve relative resources against that skill's directory. Reuse applicable context already loaded and announce first use briefly.
- Apply session instructions before conflicting skill guidelines, subject to system/developer instructions. Check whether an explicit approval requirement is already satisfied; do not turn recommendations into mandatory confirmation rounds.
- If a skill would block authorized work or require clarification, identify and link the exact file, quote the relevant rule, and distinguish its requirement from your interpretation. Continue work that does not depend on the missing answer.
- For a missing named skill, check the current catalog and a likely alternate path once. Use an available equivalent when possible; stop only the dependent work when an essential capability is unavailable. Do not install or modify skills merely to satisfy stale references.

## UI work

- For existing applications, start from the shipped components, tokens, copy, and interaction patterns. Preserve product facts, accessibility, and repository-specific CRUD conventions. Limit redesign to what the user requested.
- Use the relevant design skill for design work; keep its workflow proportional to the requested surface. A layout or label fix does not imply product discovery, a new visual identity, or a rewrite of shared components. Existing project constraints and an explicit brief take precedence over generic aesthetic defaults.
- Verify affected interactions and representative layouts when browser access is available. Batch visual inspection, fix observed defects, and confirm the changes. Stop discretionary polishing once the requested result works; report essential checks that could not be completed.

## Verification and completion

- Check commands against current manifests and scripts before running them; examples in documentation may be stale. Follow repository-specific execution restrictions.
- Run checks that exercise the changed behavior and complete applicable required checks. Add meaningful tests for new behavior or regressions; documentation and low-impact cosmetic edits do not require implementation-mirroring tests.
- Reuse valid results. Broaden or repeat checks only for new failures, subsequent changes, integration risk, or an explicit requirement. Never report an unrun check as passed.
- If verification is blocked, state the command, cause, and resulting uncertainty, then finish unaffected work. A test or build instruction does not by itself authorize deployment or other external changes.

## Database migration safety
- Never drop an existing table or other database object in order to recreate it from a migration or setup script.
- Create tables and other database objects only when they do not already exist, using guarded statements such as SQL Server's `if object_id(...) is null`.
- Evolve existing schemas additively with guarded changes. If an existing object has an incompatible shape, fail with a clear error and require an explicit reviewed migration; never delete the object or its data automatically.

## Git worktrees

- Never create, use, or recommend Git worktrees for any task, including delegated or subagent work.
- Work only in the user's existing working tree. Do not run `git worktree` commands.
- If a tool or workflow would automatically create a worktree, choose a non-worktree alternative. If none exists, stop and ask the user instead of creating one.

## Token-efficient subagent delegation

- Use subagents proactively for bounded, independent work when the expected benefit outweighs setup, copied context, coordination, and verification. Optimize total effort across all agents while preserving correctness; faster parallel execution does not imply fewer tokens. Keep small or tightly coupled tasks local; batch simple independent tool calls without agents.
- Start with one helper; add others only for distinct useful assignments while the main agent has independent work. Do not fill every slot by default. Subagents must not delegate further unless the main agent explicitly assigns that split.
- Each assignment must state the objective, read-only or edit scope, owned files/resources, acceptance criteria, relevant paths and constraints, and expected checks. Tell the agent to report missing information or scope conflicts rather than guess or expand the task.
- Pass the smallest sufficient context. Prefer no history fork or a few relevant turns over full history when supported. Include decisions and applicable instruction paths that the child would otherwise miss; require it to read guidance for its scope. Avoid copying whole conversations, files, or logs.
- Choose the least expensive available model that can reliably handle the assignment, using known capabilities and costs. Do not invent model availability or prices; keep the configured default when uncertain. Reserve stronger models/reasoning for ambiguous or consequential work. Escalate unresolved uncertainty or failed approaches instead of repeating weak attempts.
- Use the existing working tree only; never create or use Git worktrees. Assign one writer per file or shared mutable resource, including the main agent. Agree on shared interfaces before splitting edits; serialize dependent changes, conflicting test environments, and shared Git operations. Never overwrite another agent's or the user's work.
- Do not repeat an agent's exploration in the main thread. Reuse a relevant agent for follow-ups and stop obsolete work. Duplicate analysis only for a specific independent review whose risk justifies the cost.
- Return a concise result: findings or changes with file references, checks actually run and their results, and remaining risks/blockers. Distinguish verified facts from assumptions and unrun checks. Stop when the assigned deliverable is complete.
- The main agent inspects changes, reconciles interfaces, and verifies the integrated result against the user's request. Reuse valid check results; rerun when subsequent changes invalidate them or integration introduces new risk. Use a focused independent review for consequential changes such as authorization, data migrations, or concurrency when it materially reduces error risk. Agent agreement alone is not evidence.

## Policy portability

- Repository AGENTS.md files must be self-contained and version-controlled. Do not rely on a parent workspace AGENTS.md, an absolute local policy path, or a fallback to an uncommitted common policy for required project rules.
- Shared workflow and persistence rules are intentionally duplicated in repository AGENTS.md files. When changing common policy, update the applicable repository copies in the same task and preserve repository-specific rules. Verify that new project policy files are included in the repository change; do not assume an existing local file is tracked.

## JPA-first persistence and native SQL visibility

- For JVM application persistence, always evaluate mapped entities and Spring Data JPA repositories first: derived queries, JPQL, Criteria/Specifications, projections, and entity lifecycle operations. This includes migrating legacy features, imports, and application-level data migrations/backfills. Legacy SQL is evidence of behavior to preserve, not the default implementation to copy. Do not introduce JPA into non-JVM or database-only projects merely to satisfy this rule.
- Before adding or extending native SQL, inspect existing mappings and repository methods. Add a missing table-backed mapping when practical; missing mappings, convenience, or an existing native-query precedent alone do not justify bypassing JPA. Calling `createNativeQuery` through an EntityManager or wrapping SQL in a repository is still native SQL.
- Distinguish application/data migration work from versioned schema migrations. Prefer JPA for data movement when a stable mapping and suitable transaction/lifecycle are available. Keep guarded DDL, triggers, schema metadata operations, and migrations that must run before JPA startup or against historical schemas in the existing SQL migration framework. Do not make historical migrations depend on evolving application entities, replace Flyway with Hibernate schema auto-update, or edit already-applied migrations; use new forward migrations.
- A migration that creates a procedure or function containing runtime business logic is also an application-design change; the DDL exception does not justify moving new business behavior into SQL. Evaluate implementing that behavior with JPA-backed services. Existing SQL-view restrictions and protected legacy integration boundaries still apply to every exception.
- Retain native SQL only with a concrete reason, such as a required database-specific operation, stored-procedure contract, proven locking requirement, or measured bulk-performance need. Assess the JPA alternative first and document why it cannot safely preserve the required behavior. Isolate exceptions in a focused repository/gateway and bind values as parameters; validate dynamic identifiers against an allowlist.
- Preserve tenant routing, authorization filters, business versus physical row IDs, temporal/current-row semantics, trigger behavior, transactions, locking, pagination, and result shape when proposing or making a conversion. Check flush/clear/refresh needs when mixing JPA and direct SQL. Verify relevant behavior with focused tests; concurrency-sensitive replacements need evidence of equivalent locking behavior.
- In every task that touches a native SQL path (changes it, relies on it for changed behavior, or reviews it), explicitly surface the affected paths in the final response and any PR description. Give file/method references, purpose, and disposition: converted to JPA, retained with a specific reason, or proposed for follow-up with the concrete JPA approach and scope. Group related queries when useful; do not silently leave encountered native SQL unmentioned or scan unrelated code just to create an inventory.
- Convert straightforward, behavior-preserving cases within the authorized scope. If conversion needs a broader domain/schema change or touches a protected legacy integration, propose the bounded follow-up and ask only when the decision materially changes scope or correctness. Do not force a confirmation round for routine in-scope replacements or expand a review-only task into code changes.
- For every code review, inspect the affected persistence paths and migrations for native SQL, including `nativeQuery = true`, `createNativeQuery`, JDBC/templates/connections, SQL-bearing mappings such as `@Subselect`, stored procedures, and SQL assembled or delegated through helpers. Report new/expanded unjustified native SQL as a policy finding with a JPA alternative. Surface justified exceptions and pre-existing opportunities separately from correctness findings; existing native SQL alone is not evidence of a bug. An absence of correctness findings must not hide the native-SQL assessment when affected paths contain it.
