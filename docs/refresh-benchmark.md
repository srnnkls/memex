# Live refresh measurements

For the subsequent matched storage comparison, see [shared-segment measurements](shared-segments-benchmark.md). For all optimizations together versus fetched upstream, including amortized per-update cost and terminal maintenance, see the [sustained cost model](index-merge-cost-model.md#current-build-versus-upstream-85671-to-29039-ms-per-update-amortized).

2026-09-08, Apple M1 Pro. Working branch: `perf/opencode-cleanup`, based on instrumentation PR #155. Measurements use the real `~/.memex` corpus and the existing configuration. The production build has profiling compiled out.

## Final production run

- No-op calls: 111.66 ms median, 123.61 ms p95, 11 unchanged samples after four warmups. One call with two added records was excluded from the no-op cohort and took 274.03 ms.
- A separately logged assistant message was indexed and retrieved in 295.63 ms. Three records were added from one changed source file. The probe was confirmed present in the transcript before starting the timer.
- Initial post-build catch-up took 2.47 s and allocated 2,889 record IDs. It is excluded from steady-state results.
- These results miss the working budgets of 100 ms no-op p95 and 250 ms small-update p95. The small-update measurements are individual calls, not a p95 claim.

Raw receipts: `/tmp/memex-refactor.gG5Rm9/production-warm.json` and `/tmp/memex-refactor.gG5Rm9/production-message.json`.

## Verified structural wins

- No Git subprocesses in repository metadata resolution; an operation-owned gix resolver is shared by memory and analytics.
- No lexical staging, writer, commit, or publication for no-op and checkpoint-only refreshes, covered by an executable cost-contract test.
- Independent observations use one bounded operation-owned pool, reused for parsing.
- Analytics facts/labels are resolved before the SQL transaction; deletions and upserts commit together.
- Publication intent is persisted once before shared record changes. Existing recovery tests pass.
- Staging copies/links run with bounded concurrency; large published segments remain immutable.
- Previous fixes remain: actual-presence legacy cleanup, ctime-aware file checks, Codex metadata checkpoints, bounded merges, and small-batch writer sizing.

The earlier 714 ms receipt was a twenty-record update with tracing enabled. It is not a matched workload for the final three-record production call; no direct speedup ratio is claimed.

## Validation

- 658 library tests passed; two existing Markdown performance tests were ignored and two known baseline OAuth database-open failures were explicitly excluded.
- 27 integration tests passed with profiling enabled: concurrency, retrieval, memory scope/RPC, multi-machine RPC, and profiling cost/privacy contracts.
- `cargo fmt --check` and `cargo clippy -- -D warnings` passed.
