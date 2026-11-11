# Memory ready-scan benchmark

This report compares the unchanged baseline commit `924fdf2` with the indexed implementation using the same benchmark harness. Raw command output, sample arrays, and exit codes are preserved in [memory-ready-scan-benchmark-raw.txt](memory-ready-scan-benchmark-raw.txt). These local measurements are descriptive and are not CI thresholds.

## Method

- Commands: `cargo bench --locked --bench memory_ready_scan` and `cargo bench --locked --bench memory_accept`.
- Toolchain: Cargo 1.94.0, rustc 1.94.0; optimized bench profile.
- Machine: Ubuntu Linux 6.8.0-147-generic, x86_64, Intel Xeon E5-2680 v4 @ 2.40 GHz, 56 logical CPUs.
- The ready-scan matrix independently varies terminal history and queued tasks over 0, 256, 4,096, and 32,768 records (all 16 combinations). Each task has a 64-byte payload; the store budget is 64 MiB. Setup and lifecycle transitions are outside the timed scans.
- Each case uses page size 128, 3 warmups, and 9 timed scans. A concurrent probe performs `get_encoded_task` after the first scan page while the full scan future is still active. The scan task yields after that page so the probe can run without a sleep-based timing assumption.
- Entry visits are exact algorithm counts for this due-only dataset: the baseline examines every retained record once per page; the indexed implementation visits queued keys after the exclusive cursor, including one lookahead key on each non-final page. The lifecycle regression separately counts actual index visits and confirms 14 visits on the baseline fixture versus 2 indexed visits for 12 terminal and 2 queued records.
- The acceptance comparison uses the existing 128-item batch harness, 3 warmups and 9 measured batches per history size. Two runs were captured for each revision because the host showed measurable run-to-run variation.

## Ready-scan results

| History | Queued | Baseline visits | Indexed visits | Baseline median | Indexed median | Baseline probe median / p95 | Indexed probe median / p95 |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | 32,768 | 8,388,608 | 33,023 | 131.15 ms | 20.71 ms | 3.15 / 5.86 µs | 3.64 / 4.57 µs |
| 32,768 | 0 | 32,768 | 0 | 226.89 µs | 197 ns | 3.29 / 5.13 µs | 2.35 / 8.04 µs |
| 32,768 | 256 | 66,048 | 257 | 570.28 µs | 113.01 µs | 1.34 / 4.03 µs | 2.20 / 8.30 µs |
| 32,768 | 4,096 | 1,179,648 | 4,127 | 11.42 ms | 1.72 ms | 3.09 / 3.65 µs | 2.82 / 8.04 µs |
| 32,768 | 32,768 | 16,777,216 | 33,023 | 267.33 ms | 42.32 ms | 3.65 / 4.11 µs | 9.20 µs / 21.14 ms* |

For the largest queued set, the indexed scan median was about 6.3 times lower in both the empty-history and maximum-history cases. The algorithmic visits fell from millions to about 33 thousand. The lookup probe generally remained in the low-microsecond range, but the indexed maximum-size case had one 21 ms outlier; no latency bound is claimed from these samples.

`*` The p95 is the maximum of nine samples (nearest-rank percentile); the outlier is retained in the raw results. Host scheduling varied during the measurements, so the table should be read as evidence of scan work reduction rather than a portable speed ratio.

## Acceptance cost

| History | Baseline run medians (ns/accept) | Indexed run medians (ns/accept) |
| ---: | ---: | ---: |
| 0 | 828, 1,078 | 1,215, 1,205 |
| 256 | 930, 928 | 1,096, 1,088 |
| 4,096 | 1,190, 922 | 1,084, 1,064 |
| 32,768 | 2,264, 1,129 | 1,304, 1,301 |

The queued index adds a small write-maintenance cost in low-history runs: the observed difference was roughly 0.13–0.38 µs per acceptance in those samples. The baseline also varied between runs, especially at 32,768 history. This cost is recorded alongside the much lower repeated-scan work; the benchmark does not establish an SLA or a fixed improvement percentage.

## Limits

The harness measures store page scans and a concurrent store lookup. It does not measure scheduler resource admission. The service-level page yield is covered by the deterministic current-thread scheduler test. Future retry entries can still be visited while filtering a scan; the implementation does not claim every workload is O(page size). The benchmark has no timing assertion in CI.

## Current worktree investigation

The measurements above are retained historical evidence from commit `924fdf2`.
For RS-TASK-002, the no-index implementation at `97aa79d` was measured with an
additional future-retry matrix because the earlier due-only workload does not
quantify retry backlog costs. The updated harness crosses 0, 256, 4,096,
and 32,768 future retries with 0, 1, and 128 immediately ready tasks. It
measures ready-page scans, deadline lookup, the complete scan-and-select
portion of a scheduler cycle (excluding the actual sleep), and concurrent
`get_encoded_task`/`accept_encoded` probes after the first page releases the
mutex. It uses fixed sequential IDs, alternating adjacent future deadlines, 5
warmups, and 31 timed samples per operation. Setup is outside timed regions.

The baseline's deterministic visit counts were checked by cfg(test)-only store
counters: both an empty-ready page and deadline lookup visited every queued
record in the future-only fixture. The indexed implementation's test-only
counter records index entries yielded by the range query (zero or one); it does
not count internal B-tree node comparisons. Neither counter is compiled into
release builds or exposed in the public API.

The raw file contains the baseline runs, indexed runs, interleaved no-index
comparisons, and toolchain/host metadata. The first baseline table below is
retained to show the original 97aa79d measurements.

### Results and decision

Two no-index runs completed with rustc/cargo 1.94.0, optimized bench profile,
Linux x86_64, 56 reported logical CPUs, and a four-worker Tokio runtime. Each
cell below is `ready-page / deadline-query / scan-and-select` median time; the
full 31-sample arrays are in the raw file.

| Future / ready tasks | Run 1 (µs) | Run 2 (µs) |
| ---: | ---: | ---: |
| 0 / 0 | 0.155 / 0.070 / 0.199 | 0.253 / 0.122 / 0.340 |
| 0 / 1 | 0.317 / 0.079 / 0.377 | 0.636 / 0.135 / 0.706 |
| 0 / 128 | 40.482 / 2.792 / 43.290 | 74.360 / 6.824 / 80.619 |
| 256 / 0 | 4.748 / 4.141 / 8.762 | 4.547 / 4.181 / 8.662 |
| 256 / 1 | 6.857 / 5.720 / 12.498 | 8.856 / 7.856 / 16.551 |
| 256 / 128 | 122.063 / 26.580 / 148.676 | 92.746 / 22.930 / 115.565 |
| 4,096 / 0 | 295.008 / 307.475 / 596.053 | 329.536 / 334.769 / 666.090 |
| 4,096 / 1 | 291.006 / 289.596 / 573.000 | 239.511 / 244.831 / 512.399 |
| 4,096 / 128 | 300.406 / 293.407 / 580.922 | 398.093 / 329.051 / 748.219 |
| 32,768 / 0 | 2,106.848 / 2,117.743 / 4,224.276 | 2,100.200 / 2,113.053 / 4,197.265 |
| 32,768 / 1 | 2,110.372 / 2,116.001 / 4,233.080 | 2,089.309 / 2,108.230 / 4,201.377 |
| 32,768 / 128 | 2,179.329 / 2,168.607 / 4,369.706 | 2,130.510 / 2,107.092 / 4,234.348 |

At 4,096 and 32,768 future tasks with zero or one ready task, baseline deadline
lookup accounts for about half of the scan-and-select cycle. Its visit count
was exactly the queued count. The concurrent get and accept probes were
generally in the low single-digit microseconds at the median, with noisy tails.

### Interleaved no-index vs indexed comparison

The final harness, including the same ready-page, deadline, scan-and-select,
and `Running → Queued` retry-transition timings, was run in this order:
indexed, no-index, indexed, no-index. The no-index worktree is commit
`97aa79d`; the indexed run uses the implementation described below. Both used
the same shared Cargo target and dependency worktrees. All four commands exited
0. The table lists the two run medians for scan-and-select (µs); all 12 matrix
cases and their complete samples remain in the raw file.

| Future / ready tasks | No-index runs (µs) | Indexed runs (µs) |
| ---: | ---: | ---: |
| 256 / 0 | 18.217, 16.491 | 4.889, 6.232 |
| 256 / 128 | 89.653, 103.248 | 51.893, 84.707 |
| 4,096 / 0 | 491.893, 561.457 | 266.036, 237.831 |
| 4,096 / 128 | 551.072, 544.532 | 293.960, 291.809 |
| 32,768 / 0 | 4,164.280, 4,136.112 | 2,140.843, 2,243.238 |
| 32,768 / 128 | 4,223.131, 4,337.922 | 2,151.560, 2,245.466 |

At 32,768 future tasks, deadline-query medians fell from 2,066–2,083 µs to
0.405–0.425 µs with no ready tasks, and from 2,087–2,141 µs to 0.360–0.443 µs
with 128 ready tasks. Ready-page times stayed around 2.08–2.31 ms in these
interleaved runs. This cuts the measured scan-and-select median by about 46–49%
at the largest backlog while retaining the existing ready-scan cost. At 4,096
futures, the cycle reduction was about 46–58%. Timing varied between runs, so
these are descriptive local samples, not an SLA.

The indexed harness also measured 31 `Running → Queued` retry transitions per
case, with accept and start setup outside the timed region. At 32,768 future
entries, the requeue medians were 0.665 and 1.180 µs with the index, compared
with 0.540 and 0.534 µs in the no-index worktree. This is an observed extra
0.125–0.646 µs per requeue in these samples; transition timing also varied with
host load. The separate `memory_accept` benchmark reports initial acceptance
(median 1.127–2.289 µs over the listed history sizes), but initial acceptance
has no retry deadline and does not exercise the new index.

Each `(deadline, TaskId)` key occupies 16 bytes (`size_of` lower bound): 32,768
entries therefore require at least 512 KiB of key storage. This excludes
B-tree node links, occupancy slack, and allocator overhead; resident memory
was not directly measured. The index holds only queued tasks that have a retry
deadline and is bounded by the configured unfinished-task retention limit.

**Decision: retain the minimal deadline index.** The interleaved runs show
about 2 ms saved from the measured cycle at 32,768 queued retries, against a
sub-microsecond median increase per retry transition and a 512 KiB key-size
lower bound at that size. The actual memory cost is higher than the key lower
bound and was not directly measured, so the result does not claim a precise
memory/performance ratio. The reduction is large enough to justify the added
bounded index for configured large backlogs. The ready page still performs
O(N) filtering across future retries; this change does not eliminate that
scan. No performance threshold or SLA is implied.

The plan's example of changing a retry deadline while a task stays `Queued`
cannot be invoked through the current state machine: `Queued → Queued` is not
an allowed transition and the store returns `Conflict` before mutation. This
task preserves that existing behavior and checks deadline removal on start,
then reinsertion with a new deadline on the next valid `Running → Queued`
retry transition instead.
