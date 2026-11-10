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
