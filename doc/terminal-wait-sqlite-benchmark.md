# SQLite terminal-wait polling benchmark

## Evidence search

The repository history contains no `doc/terminal-wait-sqlite-benchmark.md` or matching benchmark record (`git log --all -- doc/terminal-wait-sqlite-benchmark.md`; source and docs search). The earlier measurements referenced in review could not be recovered, so a controlled comparison was run.

## Reproduction

```sh
cargo bench --locked --features sqlite --bench terminal_wait_sqlite
```

The harness creates a fresh SQLite WAL database for each trial, uses bundled SQLite 3.54.1 and a two-worker Tokio runtime, and stores one checkpoint row. Each waiter makes an immediate read, then polls until the writer changes the row to terminal after 80 ms. The timeout is 400 ms. Five rounds alternate fixed 25 ms polling and the current 25/50/100 ms exponential backoff. The benchmark measures first-read startup p95, terminal-commit-to-observation p50/p95, checkpoint reads, timeouts, and read errors. Complete per-round output is in [terminal-wait-sqlite-benchmark-raw.txt](terminal-wait-sqlite-benchmark-raw.txt).

## Results

Medians across five rounds (times in microseconds; checkpoint reads are total reads in one trial):

| Waiters | Strategy | Checkpoint reads | Startup p95 | Observation p50 | Observation p95 | Observation p95 range |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 32 | Fixed 25 ms | 160 | 5,673 | 15,161 | 17,011 | 7,680–24,643 |
| 32 | Current backoff | 128 | 5,709 | 96,205 | 97,460 | 91,151–97,742 |
| 128 | Fixed 25 ms | 560 | 23,345 | 12,565 | 25,514 | 9,942–28,378 |
| 128 | Current backoff | 512 | 18,025 | 96,830 | 101,904 | 97,151–103,220 |
| 512 | Fixed 25 ms | 1,475 | 69,692 | 15,734 | 29,609 | 28,086–36,156 |
| 512 | Current backoff | 2,048 | 73,827 | 45,856 | 60,266 | 50,890–96,104 |

No timeouts or SQLite read errors occurred. Fixed 25 ms polling observed the terminal checkpoint sooner in these trials. The current strategy reduced read counts at 32 and 128 waiters, while results at 512 waiters were noisy and the median count was higher for backoff. Startup p95 rose substantially with waiter count for both strategies.

## Conclusion and limits

This synthetic 80 ms completion under a 400 ms budget demonstrates a latency-versus-query-load tradeoff at lower waiter counts, but results at high concurrency vary considerably. It does not establish that the current policy exceeds a deployment latency budget: no representative application budget or waiter distribution was available. It also measures a minimal SQLite checkpoint table, not `SqliteTaskStore`, task admission, handlers, or the complete service path. Therefore the current wait strategy is retained; validate with a deployment-specific task workload before changing behavior.

Existing virtual-time regressions cover immediate first reads and the 40/60 ms and 90/120 ms cases. For short budgets, the quarter-budget cap and 25 ms floor constrain the backoff; these tests remain the relevant contract checks.

## Corrected timing and sample accounting (2026-11-21)

The revised harness opens the writer and every requested reader before starting its common monotonic clock. A connection-open failure now reports the round, strategy and participant and exits before spawning waiters. The writer uses a 2-second SQLite busy timeout; readers use the same bound. The common start is published only after preparation, the writer targets 80 ms from that start, and each reader's 400 ms budget starts immediately before its first read. Reader tasks are collected before writer completion data is summarized.

The failure path is reproducible with `cargo bench --locked --features sqlite --bench terminal_wait_sqlite -- --self-test-open-failure`; this exercises reader and writer opens beneath a missing directory and checks their context. The compiled benchmark self-check completed under a 5-second process timeout.

Each output row reports successful observations, timeouts, read errors, observed samples and commit-interval overlaps. A result observed during the writer's commit call is included at the lower-bound latency of zero and counted separately. Empty percentile sets print `N/A`. Across all five rounds, each nonzero waiter group satisfies `success + timeout + errors = requested waiters × 5` for each strategy. The complete corrected per-round data follows the original historical samples in [terminal-wait-sqlite-benchmark-raw.txt](terminal-wait-sqlite-benchmark-raw.txt). The first revised run's service phase was interrupted and is labeled/excluded; the next run completed both phases.

For the completed synthetic run, medians across five rounds were:

| Waiters | Strategy | Median checkpoint reads | Median observation p95 | Successes / timeouts / errors |
| ---: | --- | ---: | ---: | ---: |
| 32 | Fixed 25 ms | 160 | 16.5 ms | 160 / 0 / 0 |
| 32 | Current backoff | 128 | 90.3 ms | 128 / 32 / 0 |
| 128 | Fixed 25 ms | 640 | 19.6 ms | 640 / 0 / 0 |
| 128 | Current backoff | 512 | 93.8 ms | 640 / 0 / 0 |
| 512 | Fixed 25 ms | 2,270 | 30.5 ms | 2,560 / 0 / 0 |
| 512 | Current backoff | 1,758 | 101.6 ms | 2,560 / 0 / 0 |

The 32-waiter backoff group had one 32-reader timeout round; it remains in the results. At 128 and 512 waiters, backoff reduced query counts while fixed polling observed completion sooner in successful samples. This confirms the synthetic latency/query tradeoff and does not alone set a production budget.

## Real `SqliteTaskStore` workload

The same executable now also runs the service path against a real `SqliteTaskStore`, one target task with an 80 ms handler, and one 2 ms probe per waiter. Each trial uses a fresh SQLite database and a two-worker Tokio runtime; fixed polling calls the public `get_state_checkpoint` API with an immediate first read and a 400 ms total deadline. The production group calls `wait_terminal` with the same total deadline. Five rounds interleave the two strategies at each waiter count. The delegating store counts checkpoint reads for the target and records the target terminal transition interval. Handler-start p95 is reported separately for the target and probes.

| Waiters | Strategy | Target startup p95 | Probe startup p95 | Target observation p95 | Target reads | Timeout / errors |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 0 | Fixed / backoff | N/A | N/A | N/A | 0 / 0 | 0 / 0 |
| 32 | Fixed 25 ms | 39.2 ms | 196.5 ms | 21.1 ms | 832 | 0 / 0 |
| 32 | Current backoff | 50.9 ms | 210.3 ms | 75.6 ms | 640 | 0 / 0 |
| 128 | Fixed 25 ms | 49.7 ms | 854.3 ms | 30.2 ms | 3,298 | 0 / 0 |
| 128 | Current backoff | 34.2 ms | 945.1 ms | 99.0 ms | 2,560 | 0 / 0 |
| 512 | Fixed 25 ms | 254.3 ms | 30.3 s | 95.8 ms | 12,433 | 512 / 0 |
| 512 | Current backoff | 93.5 ms | 3.49 s | 85.8 ms | 10,240 | 0 / 0 |

At 512 waiters the fixed-poll group had 512 timeouts among 2,560 waits, while the production backoff group had none and issued 17.6% fewer target checkpoint reads. Its p95 terminal observation was also lower in this local run. The probe startup p95 shows that serial probe admission dominates this deliberately heavy workload. These results support retaining the production strategy; they do not establish a deployment-specific latency budget or waiter distribution. Validate those against application traffic before changing policy.

Reproduce the mixed benchmark with:

```sh
cargo bench --locked --features sqlite --bench terminal_wait_sqlite
```

The corrected 10,000-row deep-cursor report is saved as [sqlite-history-review-23-20261121.json](sqlite-history-review-23-20261121.json) and was generated with:

```sh
cargo bench --locked --features sqlite --bench sqlite_history -- --sizes 10000 --output "$PWD/doc/sqlite-history-review-23-20261121.json"
```

At both 10% and 90% cursor depths, rows and order matched. The old predicate was interrupted under the actual 2,000-callback SQLite VM budget and fell back; the range predicate completed without interruption. Bare SQL, budgeted timing, interruption status and total VM steps are reported separately. The `old_sql_interrupted` JSON field now reflects `OperationInterrupted`, not a VM-step threshold estimate.
