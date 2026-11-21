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
