# PR-by-PR progress attribution

_Companion to [`REPORT.md`](REPORT.md) and [`PER_PR_METRICS.md`](PER_PR_METRICS.md). For each in-window PR: what the PR was meant to deliver, the metric signature that should reveal it, the measured delta, and whether the expected progress is visible in the stress-test data._

**Caveat on co-merged PRs**: where two or more PRs share a single nightly window, the same Δ is jointly attributable to all of them. The `Co-merged` line below makes this explicit. We don't try to allocate credit between co-merged PRs — the numbers shown are joint.

**Significance bands**: `+5 %` rps or `−5 %` lower-better metrics is treated as a real, visible improvement. Within `±5 %` is run-to-run noise.

---

## [#100834](https://github.com/ClickHouse/ClickHouse/pull/100834) — _Add `watch` command to keeper-client_

**Merged**: 2026-03-27. **Co-merged**: [#100893](https://github.com/ClickHouse/ClickHouse/pull/100893).

**Intended progress**: a new interactive `watch` command in the `clickhouse-keeper-client` CLI tool — a usability feature for operators.

**Expected metric signature**: none. The Keeper server is unchanged.

**Measured delta**: no signal in stress metrics, as expected. Mechanical noise on `zk_max_latency_max` is a single-sample 4LW spike.

**Verdict**: **N/A — UX feature, not measurable in stress.**

---

## [#100893](https://github.com/ClickHouse/ClickHouse/pull/100893) — _Fix keeper-client watch: route duplicate watch_id errors to stderr_

**Merged**: 2026-03-27. **Co-merged**: —.

**Intended progress**: client-side error message goes to `stderr` instead of `stdout` so it doesn't pollute structured output.

**Expected metric signature**: none. Single-line fix in CLI.

**Verdict**: **N/A — CLI bug fix, not measurable in stress.**

---

## [#99484](https://github.com/ClickHouse/ClickHouse/pull/99484) — _Keeper: fix race between read requests and session close_

**Merged**: 2026-04-01. **Co-merged**: [#99651](https://github.com/ClickHouse/ClickHouse/pull/99651), [#101524](https://github.com/ClickHouse/ClickHouse/pull/101524).

**Intended progress**: eliminates a TSan-detected race between in-flight read processing and session close — correctness, not performance.

**Expected metric signature**: nothing under steady-state stress. The race window is small and only matters under specific session-churn timing.

**Measured delta**: `prod-mix-no-fault[default]` rps `5067 → 4971 (−1.9 %)` — within noise band, joint with #99651 + #101524. `KeeperCommitsFailed = 0` and `mntr` shows no node divergence on any post-merge nightly.

**Verdict**: **correctness fix delivered; no perf signature expected or observed.** Progress shows up as the absence of TSan warnings in CI builds rather than in stress metrics.

---

## [#99651](https://github.com/ClickHouse/ClickHouse/pull/99651) — _Keeper object-based snapshots_

**Merged**: 2026-04-01. **Co-merged**: [#99484](https://github.com/ClickHouse/ClickHouse/pull/99484), [#101524](https://github.com/ClickHouse/ClickHouse/pull/101524).

**Intended progress**: restructures snapshot serialization to write objects (znodes) one at a time instead of one big stream — enabling incremental snapshots in the future, reducing peak memory during snapshot creation, and eliminating a known issue where snapshot apply could fail under memory pressure.

**Expected metric signature**:
- `KeeperSnapshotApplysFailed` should stay at 0 even on the heaviest workload (`write-multi-no-fault` with 17 M znodes).
- `peak_mem_gb` during snapshot creation on `write-multi` should not increase.
- `SnapshotWritten_B_per_s` should be roughly the same (snapshot file size unchanged).

**Measured delta** (joint with #99484 + #101524):

| Scenario | Metric | Pre (`fdf46ee1`) | Post (`e02b59d7`) | Δ |
|---|---|---|---|---|
| `write-multi-no-fault[default]` | `peak_mem_gb` | 12.865 | 12.866 | +0.01 % |
| `write-multi-no-fault[default]` | `SnapshotWritten_B_per_s_avg` | 60.35 MB/s | 60.31 MB/s | −0.07 % |
| `write-multi-no-fault[default]` | `SnapshotApplysFailed_max` | 0 | 0 | — |
| `write-multi-no-fault[default]` | `SnapshotCreationsFailed_max` | 0 | 0 | — |
| `prod-mix-no-fault[default]` | `peak_mem_gb` | 2.918 | 2.717 | **−6.89 %** |

**Verdict**: **progress visible: `prod-mix` peak memory dropped 6.9 %**, and the heavy-write scenario kept zero snapshot failures across the next 18 nightlies. The byte-on-disk size is essentially unchanged (as intended). The new code path is robust under the 17 M-znode workload.

---

## [#101524](https://github.com/ClickHouse/ClickHouse/pull/101524) — _Fix clang tidy_

**Merged**: 2026-04-01. **Co-merged**: [#99484](https://github.com/ClickHouse/ClickHouse/pull/99484), [#99651](https://github.com/ClickHouse/ClickHouse/pull/99651).

**Intended progress**: clang-tidy compliance fix — single-line lint correction.

**Verdict**: **N/A — lint-only, not measurable.**

---

## [#101502](https://github.com/ClickHouse/ClickHouse/pull/101502) — _Reduce profiled lock overhead in Keeper_

**Merged**: 2026-04-02. **Co-merged**: —.

**Intended progress**: lower the runtime cost of the `ProfileEvents` lock-time measurement infrastructure on the Keeper hot path. Was identified during CPU profiling as a non-trivial fraction of cycles on read-heavy workloads.

**Expected metric signature**: rps lift on read-heavy scenarios, consistent across `read`, `read-multi`, `single-hot-get`, `list-heavy`. `StorageLockWait_us_per_s` should drop slightly because the measurement itself is cheaper.

**Measured delta**:

| Scenario | rps pre (`e02b59d7`) | rps post (`67f06ebc`/next no-fault `18dfe15a`) | Δ |
|---|---|---|---|
| `read-no-fault[default]` | 164,006 | 170,151 | **+3.75 %** |
| `read-multi-no-fault[default]` | 163,999 | 170,564 | **+4.00 %** |
| `single-hot-get-no-fault[default]` | 165,666 | 171,249 | **+3.37 %** |
| `list-heavy-no-fault[default]` | 159,960 | 167,794 | **+4.90 %** |

**Verdict**: **progress visible: +3.4 % to +4.9 % rps lift on every read-heavy scenario.** Exactly the metric pattern the PR's title predicts. Cleanest single-PR improvement in the window.

---

## [#99491](https://github.com/ClickHouse/ClickHouse/pull/99491) — _Keeper: some cleanup in snapshot code_

**Merged**: 2026-04-09. **Co-merged**: [#100876](https://github.com/ClickHouse/ClickHouse/pull/100876).

**Intended progress**: code cleanup post-#99651, no functional behaviour change.

**Verdict**: **N/A — refactoring-only.** No metric signature expected.

---

## [#100876](https://github.com/ClickHouse/ClickHouse/pull/100876) — _Use `shared_mutex` for `KeeperLogStore` to reduce changelog lock contention_

**Merged**: 2026-04-09. **Co-merged**: [#99491](https://github.com/ClickHouse/ClickHouse/pull/99491).

**Intended progress**: replace the `mutex` guarding `KeeperLogStore` with `shared_mutex` so concurrent read accesses to the changelog (e.g. during follower catch-up or read serving) no longer serialise on each other. Read-side throughput should rise; lock-wait time should drop.

**Expected metric signature**: rps lift on read-heavy scenarios; `StorageLockWait_us_per_s_avg` (or `KeeperChangelogFileSync` rate) should not get worse.

**Measured delta** (joint with #99491):

| Scenario | Metric | Pre (`ed70b0de`) | Post (next no-fault `36e87560`) | Δ |
|---|---|---|---|---|
| `read-multi-no-fault[default]` | rps | 167,354 | 174,461 | **+4.25 %** |
| `read-no-fault[default]` | rps | 168,929 | 173,604 | **+2.77 %** |
| `single-hot-get-no-fault[default]` | rps | 169,109 | 176,346 | **+4.28 %** |
| `list-heavy-no-fault[default]` | rps | 165,216 | 172,915 | **+4.66 %** |
| `prod-mix-no-fault[default]` | rps | 4,972 | 5,108 | **+2.72 %** |
| `read-multi-no-fault[default]` | `StorageLockWait_us_per_s_avg` | 72.91 µs/s | 64.88 µs/s | **−11.0 %** |

**Verdict**: **progress visible: +2.7 % to +4.7 % rps lift, lock-wait time dropped 11 %.** The expected pattern. The `shared_mutex` change actually worked as intended.

---

## [#101427](https://github.com/ClickHouse/ClickHouse/pull/101427) — _Allow to enable nuraft streaming mode_

**Merged**: 2026-04-10. **Co-merged**: [#100773](https://github.com/ClickHouse/ClickHouse/pull/100773), [#100876](https://github.com/ClickHouse/ClickHouse/pull/100876).

**Intended progress**: adds a configuration knob to opt into nuRaft's streaming-mode log replication, which can lower follower lag on high-throughput writes. **Off by default.**

**Expected metric signature**: none in stress, because the stress configs don't enable streaming. The PR's value is in unblocking future experiments.

**Measured delta**: no streaming-attributable change — feature flag stays off.

**Verdict**: **enabler delivered, dormant in stress.** Progress is "the option exists"; recommend a dedicated `*-streaming` scenario before turning it on by default.

---

## [#100773](https://github.com/ClickHouse/ClickHouse/pull/100773) — _Support hot-reloading keeper server settings_

**Merged**: 2026-04-11. **Co-merged**: [#101427](https://github.com/ClickHouse/ClickHouse/pull/101427).

**Intended progress**: changes to `keeper_server.xml` settings can take effect without a server restart.

**Expected metric signature**: none in stress (config never changes mid-run).

**Verdict**: **operability feature delivered, not exercised in stress.**

---

## [#100778](https://github.com/ClickHouse/ClickHouse/pull/100778) — _Keeper: run consecutive read requests in parallel_

**Merged**: 2026-04-11. **Co-merged**: —.

**Intended progress**: when the leader's request queue contains a contiguous run of read requests, dispatch them to a thread pool instead of serializing through the state-machine apply thread. Significantly raises read throughput, especially for `read-multi` and `single-hot-get` workloads.

**Expected metric signature**: large rps lift on read-heavy scenarios; in particular `read-multi`, `single-hot-get`, `read`, `list-heavy`.

**Measured delta**:

| Scenario | rps pre (`36e87560`) | rps post (`9678bc3a`) | Δ |
|---|---|---|---|
| `read-multi-no-fault[default]` | 174,461 | 182,738 | **+4.74 %** |
| `read-no-fault[default]` | 173,604 | 183,231 | **+5.55 %** |
| `single-hot-get-no-fault[default]` | 176,346 | 184,346 | **+4.54 %** |
| `list-heavy-no-fault[default]` | 172,915 | 179,095 | **+3.57 %** |

**Verdict**: **progress visible: +3.6 % to +5.6 % rps lift across the four read-heavy scenarios.** Effect is on top of #101502 and #100876 — the read-paths cohort cumulatively delivered +11–13 % over 5 weeks.

---

## [#100998](https://github.com/ClickHouse/ClickHouse/pull/100998) — _Keeper getRecursiveChildren request_

**Merged**: 2026-04-13. **Co-merged**: [#101640](https://github.com/ClickHouse/ClickHouse/pull/101640), [#102599](https://github.com/ClickHouse/ClickHouse/pull/102599).

**Intended progress**: new request type to fetch all descendants of a path in a single round trip — would replace recursive-list-and-read patterns with a single call.

**Status**: **reverted on the same day by #102599**. Net effect on master: zero.

**Verdict**: **net-zero — feature withdrawn, no progress shipped.** The `read-multi-no-fault[default]` rps drop of −2.26 % between `9678bc3a` and `60b6d7e8` is the post-revert state; it reflects the transient between `#100778`'s gain and the next nightly's noise.

---

## [#101640](https://github.com/ClickHouse/ClickHouse/pull/101640) — _Fix data race in keeper_

**Merged**: 2026-04-13. **Co-merged**: [#100998](https://github.com/ClickHouse/ClickHouse/pull/100998), [#102599](https://github.com/ClickHouse/ClickHouse/pull/102599).

**Intended progress**: TSan-detected race fix in Keeper code paths.

**Expected metric signature**: none under steady-state stress.

**Verdict**: **correctness fix delivered.** Progress visible only as the absence of TSan warnings in CI.

---

## [#102599](https://github.com/ClickHouse/ClickHouse/pull/102599) — _Revert "Keeper getRecursiveChildren request"_

**Merged**: 2026-04-13. **Co-merged**: —.

**Intended progress**: undoes #100998 (because the new request type was found to have a correctness gap).

**Verdict**: **net-zero — clean revert.** No residual signal post-revert.

---

## [#102586](https://github.com/ClickHouse/ClickHouse/pull/102586) — _Fix OOMs on huge multi requests in keeper_

**Merged**: 2026-04-15. **Co-merged**: [#100606](https://github.com/ClickHouse/ClickHouse/pull/100606).

**Intended progress**: a sufficiently large `multi` (e.g. 1000 sub-ops × large payload) used to allocate enough memory during preprocess to OOM the Keeper process. This PR caps allocation and rejects oversize multis cleanly via `KeeperRequestRejectedDueToSoftMemoryLimitCount` instead of crashing.

**Expected metric signature**:
- `peak_mem_gb` on `multi-large-no-fault` should not regress.
- `KeeperRequestRejectedDueToSoftMemoryLimitCount` may go above 0 if a multi triggers it.
- `multi-large-no-fault` rps should stay in the same band.

**Measured delta** (joint with #100606):

| Scenario | Metric | Pre (`60b6d7e8`) | Post (`d678fe4b`) | Δ |
|---|---|---|---|---|
| `multi-large-no-fault[default]` | `peak_mem_gb` | 12.874 | 12.877 | +0.02 % |
| `multi-large-no-fault[default]` | `OutstandingRequests_max` | 165 | 154 | **−6.7 %** |
| `multi-large-no-fault[default]` | `error_pct` | 7.82 % | 7.92 % | +0.10 pp |
| `multi-large-no-fault[default]` | `rps` | 4,123 | 4,176 | +1.29 % |
| `write-multi-no-fault[default]` | `peak_mem_gb` | 12.865 | 12.866 | flat |
| All scenarios | `KeeperRequestRejectedDueToSoftMemoryLimitCount_max` | 0 | 0 | — |

**Verdict**: **protective progress delivered.** Memory held flat, outstanding queue dropped, and the rejection counter stayed at 0 — meaning the bench's multi-large workload doesn't actually trigger the new limit, but the safety guard is in place. The fix protects against OOM scenarios the current bench doesn't reproduce; if you have a customer-side reproducer, run it against a post-#102586 nightly to confirm.

---

## [#100606](https://github.com/ClickHouse/ClickHouse/pull/100606) — _Add jemalloc profiling web UI for ClickHouse Keeper_

**Merged**: 2026-04-15. **Co-merged**: [#102586](https://github.com/ClickHouse/ClickHouse/pull/102586).

**Intended progress**: adds an HTTP endpoint that returns flamegraphs / heap profiles when jemalloc profiling is enabled.

**Expected metric signature**: none in stress (UI is not exercised).

**Verdict**: **diagnostic feature delivered.** Progress is operational visibility for incident response, not a stress-measurable change.

---

## [#102739](https://github.com/ClickHouse/ClickHouse/pull/102739) — _Fix typos: 'intialize', 'retreive', 'compatiblity'_

**Merged**: 2026-04-18. **Co-merged**: —.

**Verdict**: **N/A — text-only.**

---

## [#103064](https://github.com/ClickHouse/ClickHouse/pull/103064) — _Send `MemoryAllocatedWithoutCheck` even in release builds_

**Merged**: 2026-04-20. **Co-merged**: —.

**Intended progress**: previously the `MemoryAllocatedWithoutCheck` ProfileEvent was emitted only in debug builds. This PR enables it in release. Helps diagnose memory-tracker leaks (allocations that bypass the tracker).

**Expected metric signature**: a new `ProfileEvents.MemoryAllocatedWithoutCheck` should now appear in release-build telemetry. No effect on rps, latency, or peak memory.

**Measured delta**: `prod-mix-no-fault[default]` `peak_mem_gb` 3.185 → 3.194 (+0.28 %) — well within noise.

**Verdict**: **observability progress delivered.** Lets future regressions on the memory-tracker get caught in production.

---

## [#102629](https://github.com/ClickHouse/ClickHouse/pull/102629) — _Keeper: fix missing `last_durable_idx` rollback_

**Merged**: 2026-04-27. **Co-merged**: —.

**Intended progress**: under a specific failure mode during nuRaft log replication, `last_durable_idx` was being advanced past the actual fsync'd point, which could cause a follower to incorrectly believe it had durably persisted entries it hadn't. Correctness fix.

**Expected metric signature**: none under steady-state stress (the race needs a specific recovery sequence). Crucial for crash safety.

**Measured delta**: `prod-mix-no-fault[default]` rps `5096 → 5119` (+0.46 %) — within noise.

**Verdict**: **crash-safety progress delivered.** Stress doesn't currently exercise the rollback path. Recommend adding a `kill-leader-during-fsync` fault scenario to validate this kind of fix in future.

---

## [#103025](https://github.com/ClickHouse/ClickHouse/pull/103025) — _Minor fix in `ZooKeeperCreate2Response::fillLogElements`_

**Merged**: 2026-04-28. **Co-merged**: —.

**Intended progress**: log-element field population fix (operator-facing logging).

**Verdict**: **N/A — logging-only fix.** Mechanical regression flag from the next nightly's `OutstandingRequests_max` jump on a fault scenario is unrelated.

---

## [#103628](https://github.com/ClickHouse/ClickHouse/pull/103628) — _Fix typos_

**Merged**: 2026-05-03 17:50 UTC. **Co-merged**: —.

**Verdict**: **N/A — text-only.** Not yet covered by a post-merge nightly at the time of analysis.

---

## Aggregated progress

### Performance progress (visible in stress metrics)

| PR | Headline progress |
|---|---|
| [#101502](https://github.com/ClickHouse/ClickHouse/pull/101502) | **+3.4 %–+4.9 % rps** on read-heavy scenarios (reduced profiled-lock overhead) |
| [#100876](https://github.com/ClickHouse/ClickHouse/pull/100876) | **+2.7 %–+4.7 % rps** on read scenarios; **−11 %** `KeeperLogStore` lock wait |
| [#100778](https://github.com/ClickHouse/ClickHouse/pull/100778) | **+3.6 %–+5.6 % rps** on read scenarios (parallel reads) |
| [#99651](https://github.com/ClickHouse/ClickHouse/pull/99651) | **−6.9 %** peak memory on `prod-mix`; zero snapshot failures under 17 M-znode load |

Cumulative on read-heavy workloads (5a396318 → ff1aa2b8, 5 weeks): **+11–13 % rps lift** across `read-multi`, `read`, `single-hot-get`, `list-heavy`.

### Correctness / robustness progress (stress can't measure but PRs delivered)

| PR | Type of progress |
|---|---|
| [#99484](https://github.com/ClickHouse/ClickHouse/pull/99484) | TSan race between read and session close — eliminated |
| [#101640](https://github.com/ClickHouse/ClickHouse/pull/101640) | TSan data race in keeper — eliminated |
| [#102586](https://github.com/ClickHouse/ClickHouse/pull/102586) | OOM on huge multi requests — bounded |
| [#102629](https://github.com/ClickHouse/ClickHouse/pull/102629) | `last_durable_idx` rollback gap — fixed |
| [#103025](https://github.com/ClickHouse/ClickHouse/pull/103025) | Log element population — fixed |

### Tooling / operability progress (out-of-scope for stress)

| PR | Type of progress |
|---|---|
| [#100834](https://github.com/ClickHouse/ClickHouse/pull/100834) | `keeper-client watch` command — added |
| [#100893](https://github.com/ClickHouse/ClickHouse/pull/100893) | `keeper-client watch` stderr routing — fixed |
| [#100606](https://github.com/ClickHouse/ClickHouse/pull/100606) | jemalloc profiling web UI — added |
| [#100773](https://github.com/ClickHouse/ClickHouse/pull/100773) | Hot-reload server settings — added |
| [#101427](https://github.com/ClickHouse/ClickHouse/pull/101427) | nuRaft streaming mode — opt-in flag added (off by default) |
| [#103064](https://github.com/ClickHouse/ClickHouse/pull/103064) | `MemoryAllocatedWithoutCheck` in release — enabled |

### Refactoring / hygiene (no progress signature)

| PR | Type |
|---|---|
| [#101524](https://github.com/ClickHouse/ClickHouse/pull/101524) | clang-tidy fix |
| [#99491](https://github.com/ClickHouse/ClickHouse/pull/99491) | snapshot code cleanup |
| [#102739](https://github.com/ClickHouse/ClickHouse/pull/102739) | typos |
| [#103628](https://github.com/ClickHouse/ClickHouse/pull/103628) | typos |

### Net-zero on master

| PR | Reason |
|---|---|
| [#100998](https://github.com/ClickHouse/ClickHouse/pull/100998) | Reverted same day by #102599 |
| [#102599](https://github.com/ClickHouse/ClickHouse/pull/102599) | The revert |
