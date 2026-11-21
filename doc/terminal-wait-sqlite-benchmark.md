# SQLite terminal-wait polling benchmark

## Reproduction and environment

Run `cargo bench --locked --features sqlite --bench terminal_wait_sqlite`. The harness uses bundled SQLite 3.54.1, a two-worker Tokio runtime, a fresh WAL database per trial, an 80 ms target handler, and a 400 ms wait budget. Five rounds alternate fixed 25 ms polling with the current 25/50/100 ms backoff. It runs both a synthetic checkpoint table and the real `SqliteTaskStore` service path. The raw file contains the complete run output; its latest result block predates the probe timestamp correction and remains labeled as historical below.

For the real-store trials, each requested waiter also schedules one probe at 1 ms intervals from a common start, with a shared 2 s observation window. Scheduling uses absolute due times, so slow submit calls do not shift later due times. Current output keeps `probes_launched` as the dispatch count and reports not-dispatched, actual submitted, and not-submitted probes separately. Store acceptance is classified by its start and return interval against the half-open observation window: confirmed in-window, confirmed outside, or overlapping the cutoff. Failed and unresolved submits remain separate. Only confirmed in-window accepts enter the handler-started/not-started denominator; handler startup latency includes starts observed within the window. Dispatch lag, actual-submit lag, submit-to-store-return, submit-to-public-reply, and submit-to-handler-start use separately named samples. The per-waiter terminal-wait timeout remains 400 ms.

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

## Concurrent probe and terminal-sample run (2026-11-21; historical timestamps)

This is the latest complete raw run, recorded before the probe timestamp correction. Its schedule uses absolute 1 ms due times and dispatches each submit task without waiting for the prior public `submit()` reply. The corrected benchmark retains `probes_launched` as dispatch count and separately records `probes_submitted` and `probes_not_submitted`; the counts reconcile as dispatched = submitted + not submitted. Its submitted probes are classified as `probes_accepted_in_window` when successful acceptance returns strictly before the cutoff, `accepted_outside_window` when acceptance starts at or after the cutoff, `admission_window_overlap` when acceptance starts before but returns at or after the cutoff, `submit_failed`, or `admission_unresolved`. Probes dispatched but not submitted by the cutoff are `probes_not_submitted`. A cutoff-time submit is not called. The handler started/not-started denominator contains only confirmed in-window acceptances. The observation window is half-open `[start, end)`: a successful return exactly at the cutoff is not in-window, and its recorded start determines whether it is confirmed outside or overlapping.

The corrected p95 fields are `dispatch_lag_p95_us` (dispatch minus planned time), `submit_lag_p95_us` (actual submit start minus planned time), `submit_to_store_accept_return_p95_us`, `submit_to_public_reply_p95_us`, and `submit_to_handler_start_p95_us`. `submit_to_store_accept_return_p95_us` starts at the actual `submit()` call and ends when storage acceptance returns. The historical `launch_to_store_accept_return_p95_us` starts immediately before awaiting storage acceptance and ends when it returns; `launch_to_submit_reply_p95_us` starts at task dispatch and ends when the public `submit()` reply returns. The historical raw output contains only aggregated p95 values, so it cannot be used to recompute the current actual-submit-to-storage-return p95. The table below preserves the historical fields and values as recorded in the raw file.

For the 32-waiter rows, the raw observation p95 values are 25,485 μs (fixed polling) and 52,912 μs (current backoff), or **25.5 ms** and **52.9 ms** to one decimal place. This conversion uses the same two rows as the other columns below. Checkpoint totals count API calls that began and calls that returned; a returned call may report an error. The difference between started and returned is still pending or cancelled at the caller and does not prove that a SQLite statement stopped. The 400 ms terminal-wait budget and shared 2 s probe observation window are separate.

| Waiters | Strategy | Checkpoint calls started / returned / errors | Successful observations / effective samples / overlap | Timeout / error | Planned / dispatched / accepted by return before window end / returned at or after end / unresolved | Handler started / not started in window | Observation p95 | Store accept start-to-return p95 | Dispatch-to-submit-reply p95 | Store-return-to-handler p95 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | Fixed 25 ms | 751 / 659 / 0 | 67 / 67 / 0 | 93 / 0 | 160 / 160 / 160 / 0 / 0 | 144 / 16 | **25.5 ms** | 581.8 ms | 581.9 ms | 1,853.8 ms |
| 32 | Current backoff | 549 / 549 / 0 | 96 / 96 / 0 | 64 / 0 | 160 / 160 / 160 / 0 / 0 | 160 / 0 | **52.9 ms** | 268.2 ms | 268.4 ms | 723.7 ms |
| 128 | Fixed 25 ms | 1,664 / 1,024 / 0 | 0 / 0 / 0 | 640 / 0 | 640 / 640 / 408 / 169 / 63 | 298 / 110 | N/A | 536.0 ms | 5,373.3 ms | 1,334.2 ms |
| 128 | Current backoff | 1,664 / 1,024 / 0 | 0 / 0 / 0 | 640 / 0 | 640 / 640 / 414 / 118 / 108 | 229 / 185 | N/A | 660.5 ms | 6,033.4 ms | 1,292.8 ms |
| 512 | Fixed 25 ms | 6,826 / 4,461 / 0 | 0 / 0 / 0 | 2,560 / 0 | 2,560 / 2,560 / 1,546 / 1,014 / 0 | 0 / 1,546 | N/A | 1,583.6 ms | 4,012.1 ms | N/A |
| 512 | Current backoff | 7,099 / 5,002 / 0 | 0 / 0 / 0 | 2,560 / 0 | 2,560 / 2,560 / 1,878 / 682 / 0 | 0 / 1,878 | N/A | 1,542.2 ms | 2,076.4 ms | N/A |

The raw counts reconcile: at each tier, effective samples plus overlaps equal successful observations; planned probes equal dispatched probes; and dispatched probes partition into returns before the window end, returns at or after it, and unresolved requests. The 128- and 512-waiter groups had zero successful terminal observations, so they provide no observation-latency distribution. At 128, both strategies timed out all 640 waits; at 512, both timed out all 2,560 waits. Their accept timing counts describe only this saturated setup and cannot establish a window-boundary classification under the corrected interval rules.

Keep the current production backoff. These synthetic results show high-pressure limits but do not provide a representative deployment waiter distribution or business latency deadline. Re-evaluate only when those deployment observations are available. The 0-waiter rows are setup controls, not a polling comparison.
