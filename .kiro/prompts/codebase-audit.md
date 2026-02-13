# Codebase Audit — Rust Best Practices & Quality

## Objective

Perform a comprehensive audit of the entire codebase. Identify and fix issues related to Rust best practices, code quality, test coverage, documentation, and maintainability. Organize remediation into parallelizable waves so multiple tasks can execute concurrently without file conflicts.

---

## Audit Scope

### 1. Compiler & Linter Hygiene

- Run `cargo clippy -- -D warnings` and fix all warnings (collapsible_else_if, unused imports, redundant clones, etc.)
- Run `cargo fmt --check` and fix all formatting violations
- Resolve all `cargo build` warnings (dead code, unused variables, deprecated APIs)
- Ensure `cargo build --release` compiles cleanly
- Audit all `#[allow(dead_code)]` annotations — if the item has zero references, remove the item entirely rather than suppressing the warning. Code kept "for the public API" that nothing actually uses is dead weight; it adds maintenance burden, increases compile time, and misleads readers into thinking it's load-bearing. If it's needed later, git history has it.

### 2. DRY Violations & Abstraction Opportunities

- Identify duplicated logic across modules (e.g., client creation patterns, config builders, parsing routines)
- Extract shared helpers into `util.rs` or purpose-specific modules
- Replace magic numbers and string literals with named constants
- Look for repeated error-handling patterns that could use a shared helper or macro
- Identify copy-pasted code blocks that differ only in a parameter — extract into generic or parameterized functions

### 3. Code Decomposition & Readability

- Flag functions exceeding ~100 lines and propose extraction of logical sub-steps into named helper methods
- Ensure each function does one thing and its name describes what it does
- Review `match` arms and nested `if/else` chains for simplification opportunities
- Identify opportunities to replace imperative loops with iterator chains where it improves clarity

### 4. Error Handling

- Audit all `.unwrap()` and `.expect()` calls — replace with proper error propagation (`?`, `map_err`, custom errors) unless the panic is intentional and documented
- Ensure `src/error.rs` covers all failure modes — no raw `String` errors or `anyhow` in library code
- Verify error messages include enough context for debugging (file paths, topic names, connection details)

### 5. Test Coverage

- Run `cargo llvm-cov --summary-only` (or `make coverage`) to identify uncovered modules
- Add unit tests for any public function lacking coverage, prioritizing:
  - Core logic (event processing, state transitions, parsing)
  - Edge cases (empty inputs, malformed data, boundary values)
  - Error paths (connection failures, invalid configs)
- Ensure existing property tests in `tests/property/` still pass and cover new code paths
- Add integration tests for any new or changed user-facing behavior

### 6. Documentation

- Ensure every public struct, enum, trait, and function has a `///` doc comment
- Ensure every module has a `//!` module-level doc comment explaining its purpose
- Review and update `README.md` to reflect the current CLI interface, features, and examples
- Review and update `AGENTS.md` architecture section if module structure changed
- Verify `--help` output matches README documentation
- Add doc examples (`/// # Examples`) for non-trivial public APIs

### 7. Dependency & Configuration Review

- Check `Cargo.toml` for unused dependencies (`cargo udeps` if available, or manual review)
- Verify feature flags are minimal — no unnecessary features enabled on dependencies
- Confirm MSRV (minimum supported Rust version) is accurate if declared
- Run `cargo outdated` (or manually review) to identify dependencies with available updates
- For each outdated dependency, assess the update path:
  - **Patch/minor updates** — typically safe, check changelogs for deprecations
  - **Major version bumps** — review breaking changes, estimate migration effort, note API differences
  - **Yanked or unmaintained crates** — flag for replacement with actively maintained alternatives
- Document any dependencies pinned to old versions and the reason (e.g., MSRV constraint, upstream bug)
- Check for security advisories via `cargo audit` if available

### 8. Safety & Security

- Audit for any `unsafe` blocks — document justification or remove if unnecessary
- Verify TLS configuration doesn't silently downgrade or skip verification unless `--tls_insecure` is set
- Ensure no secrets or credentials are logged, even at debug level
- Review file I/O for proper path validation and error handling

---

## Convergence Rules

This prompt is designed to reach a clean state where re-running it produces zero findings. To prevent an infinite loop of subjective nitpicks:

### What counts as a finding

A finding MUST be anchored to at least one objective, reproducible signal:
- A compiler warning or error (`cargo build`)
- A clippy lint (`cargo clippy -- -D warnings`)
- A formatting violation (`cargo fmt --check`)
- A failing test (`cargo test`)
- A missing doc comment on a `pub` item (verifiable by `#[warn(missing_docs)]`)
- An `.unwrap()` / `.expect()` in non-test code without a justifying comment
- A function exceeding 100 lines (measurable by line count)
- A `cargo audit` advisory
- Duplicated code blocks (>10 lines with ≥90% similarity, cite both locations)

### What does NOT count as a finding

- Subjective style preferences ("I'd write this differently")
- Renaming suggestions where the current name is accurate and clear
- Refactors that don't fix a measurable issue
- "Could also do X" alternatives to already-working code
- Suggestions that contradict existing project conventions in AGENTS.md

### Severity classification

- `error` — blocks `cargo build`, `cargo test`, or `cargo clippy -- -D warnings`
- `warning` — violates a rule above but doesn't block the build (missing docs, unwrap in non-test code, DRY violation with cited locations)
- `suggestion` — improvement idea that doesn't meet the finding threshold; include these in the Phase 1 report for visibility, but do NOT include them in the wave plan. They go in the Remaining Suggestions section of Final Deliverables.

### Idempotency check

Before reporting a finding, ask: "If this fix were applied and the audit re-ran, would this category produce zero new findings?" If the answer is "no, I'd just find something else to suggest," it's not a real finding — it's a suggestion.

---

## Output Format

For each finding, report:

1. **File & location** — `src/module.rs:L42`
2. **Category** — which audit scope item (1–8) it falls under
3. **Severity** — `error` (blocks build/tests), `warning` (quality issue), `suggestion` (improvement)
4. **Description** — what's wrong and why it matters
5. **Recommended fix** — specific code change or approach

---

## Remediation Plan

Organize all fixes into waves of parallelizable tasks. Tasks within the same wave MUST NOT modify the same files.

### Wave Structure

Each wave should list:
- **Task name** — short descriptive label
- **Files modified** — explicit list so overlap is detectable
- **Changes** — bullet list of specific fixes
- **Validation** — command(s) to confirm the fix (e.g., `cargo clippy`, `cargo test --lib`)

### Example Wave Layout

```
Wave 1 (parallel — no file overlap):
  Task A: Fix clippy warnings in util.rs, recorder.rs
  Task B: Add doc comments to csv_handler/*.rs
  Task C: Extract constants in broker.rs

Wave 2 (parallel — depends on Wave 1 completion):
  Task D: Decompose mirror.rs run() using helpers from Wave 1
  Task E: Add unit tests for recorder.rs
```

### Wave Ordering Rules

- Wave N+1 may depend on Wave N but never on a later wave
- Within a wave, tasks are fully independent

### Final Wave — Verification (mandatory, always last)

The last wave in every plan is a verification pass. It is NOT optional and must run after all other waves complete:

1. `cargo fmt --check` — zero formatting violations
2. `cargo clippy -- -D warnings` — zero warnings
3. `cargo build --release` — clean release build
4. `cargo test` — all tests pass (unit, property, integration)
5. Re-run the audit scope checks (1–8) against the modified codebase
6. Report one of:
   - **CLEAN** — zero findings remain. The audit has converged. No further runs needed.
   - **REMAINING** — list any new findings introduced by the remediation itself (regressions). These form the plan for the next run if the user chooses to re-run the prompt.

---

## Execution Model

**IMPORTANT: This audit is a two-phase process. Do NOT modify any source files during Phase 1.**

### Phase 1 — Audit & Plan (automatic)

1. Perform the full audit across all scope areas (1–8)
2. Report all findings using the Output Format above
3. Organize fixes into the parallelized Wave structure described in Remediation Plan — only `error` and `warning` severity findings go into waves. `suggestion` findings are reported for visibility but excluded from the plan.
4. Present the complete plan to the user and **STOP**

### Phase 2 — Remediation (requires explicit user confirmation)

Do NOT proceed to Phase 2 until the user explicitly confirms (e.g., "go", "approved", "run it").

Once confirmed:
1. Execute waves in order, running parallel tasks within each wave (using specialized subagents if accessible)
2. Validate each wave before proceeding to the next
3. Execute the Final Wave — Verification (always last, never skipped)
4. After verification, produce the Final Deliverables below

The user may also:
- Approve specific waves only (e.g., "run waves 1–3")
- Request changes to the plan before approving
- Skip or defer individual tasks

---

## Final Deliverables

After all waves (including verification) are complete, provide:

1. **Verification result** — CLEAN or REMAINING (with details if remaining)
2. **Summary of changes** — count of files modified, warnings fixed, tests added, functions extracted
3. **Coverage delta** — before/after coverage numbers if measurable
4. **Remaining suggestions** — improvements that didn't meet the finding threshold (see Convergence Rules), with rationale for why they're suggestions not findings
5. **AGENTS.md updates** — any lessons learned or architectural changes to capture for future sessions
