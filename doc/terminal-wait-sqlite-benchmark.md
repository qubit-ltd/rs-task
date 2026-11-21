# SQLite terminal-wait polling benchmark

## Reproduction and environment

Run `cargo bench --locked --features sqlite --bench terminal_wait_sqlite`. The harness uses bundled SQLite 3.54.1, a two-worker Tokio runtime, a fresh WAL database per trial, an 80 ms target handler, and a 400 ms wait budget. Five rounds alternate fixed 25 ms polling with the current 25/50/100 ms backoff. It runs both a synthetic checkpoint table and the real `SqliteTaskStore` service path. The complete output from the corrected run is in [terminal-wait-sqlite-benchmark-raw.txt](terminal-wait-sqlite-benchmark-raw.txt).

For the real-store trials, each requested waiter also schedules one probe at 1 ms intervals from a common start, with a shared 2 s observation window. Scheduling uses absolute due times, so slow submit calls do not shift later due times. The output distinguishes planned, launched, accepted in-window, accepted after the window, failed, unresolved, and handler-started probes. Handler startup p95 only includes handler starts observed within the common window. Probe admission and admission-to-handler p95 values have their own sample populations. Unlaunched probes are reported through the planned-versus-launched counts. The per-waiter terminal-wait timeout remains 400 ms.

## Synthetic checkpoint results

Medians over five rounds; latency in milliseconds. Each nonzero waiter group reconciles all 5 × waiter-count waits as success, timeout, or error. There were no timeouts or errors in this run.

| Waiters | Strategy | Median checkpoint reads | Median observation p95 | Success / timeout / error |
| ---: | --- | ---: | ---: | ---: |
| 32 | Fixed 25 ms | 160 | 20.3 | 160 / 0 / 0 |
| 32 | Current backoff | 128 | 88.5 | 160 / 0 / 0 |
| 128 | Fixed 25 ms | 640 | 24.7 | 640 / 0 / 0 |
| 128 | Current backoff | 512 | 100.8 | 640 / 0 / 0 |
| 512 | Fixed 25 ms | 2,163 | 31.3 | 2,560 / 0 / 0 |
| 512 | Current backoff | 1,720 | 102.2 | 2,560 / 0 / 0 |

In this synthetic workload, fixed polling observed completion sooner, while backoff used fewer checkpoint reads. This minimal table does not establish a deployment latency budget.

## Real `SqliteTaskStore` results

The real-store output aggregates five trials per strategy and waiter count. Observation p95 uses successful terminal observations; overlapping writer commits are counted separately and are not inserted as zero-latency samples. Probe count fields are totals across the five rounds. `handler_started_in_window` and `handler_not_started_in_window` account for in-window admissions; accepted-after-window probes are reported separately.

| Waiters | Strategy | Target reads | Successful observations / timeouts | Observation p95 | Planned / launched / accepted in window / after window | Handler started / not started in window | Launch-lag p95 | Admission-to-handler p95 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 0 | Fixed 25 ms | 0 | 0 / 0 | N/A | 0 / 0 / 0 / 0 | 0 / 0 | N/A | N/A |
| 0 | Current backoff | 0 | 0 / 0 | N/A | 0 / 0 / 0 / 0 | 0 / 0 | N/A | N/A |
| 32 | Fixed 25 ms | 714 | 128 / 32 | 27.8 ms | 160 / 160 / 160 / 0 | 160 / 0 | 834.0 ms | 286.3 ms |
| 32 | Current backoff | 640 | 160 / 0 | 82.8 ms | 160 / 160 / 160 / 0 | 160 / 0 | 214.3 ms | 705.1 ms |
| 128 | Fixed 25 ms | 2,940 | 639 / 0 | 38.5 ms | 640 / 634 / 633 / 1 | 398 / 235 | 1,639.2 ms | 991.8 ms |
| 128 | Current backoff | 2,560 | 640 / 0 | 141.0 ms | 640 / 584 / 582 / 2 | 369 / 213 | 1,653.9 ms | 1,128.9 ms |
| 512 | Fixed 25 ms | 14,702 | 2,048 / 512 | 48.0 ms | 2,560 / 771 / 766 / 5 | 234 / 532 | 1,690.1 ms | 1,159.0 ms |
| 512 | Current backoff | 8,756 | 1,484 / 1,076 | 100.1 ms | 2,560 / 571 / 566 / 5 | 167 / 399 | 1,725.1 ms | 1,239.4 ms |

This deliberately heavy local probe workload saturates submission and handler admission at 128 and 512 waiters: many planned probes were not launched before the shared window ended, and startup p95 is therefore not a measure of the unlaunched probes. At 512 waiters both strategies timed out substantially; fixed polling completed more waits in this run but generated more reads. At 32 waiters backoff completed all waits while fixed polling timed out 32. These results do not show a stable winner across loads, and no deployment latency budget or representative waiter distribution is available. Keep the production policy and re-evaluate against deployment traffic before changing it.

## Deep-cursor fallback evidence

The 10,000-row report is [sqlite-history-review-23-20261121.json](sqlite-history-review-23-20261121.json), produced by:

```sh
cargo bench --locked --features sqlite --bench sqlite_history -- --sizes 10000 --output "$PWD/doc/sqlite-history-review-23-20261121.json"
```

At both 10% and 90% cursor depth, the old and range query returned identical rows in the same order. The actual SQLite progress handler is invoked every 1,000 VM instructions and interrupts on its second callback (approximately 2,000 VM instructions). The old query was interrupted at both depths and completed through the same split-index fallback used by production; the range query completed without interruption. The report records bare-query VM steps, actual interruption, fallback steps, and budgeted timings separately. Warmup also follows the interrupted-query fallback path.
