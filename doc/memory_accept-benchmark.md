# Memory Task Acceptance Benchmark

## Setup

The `memory_accept` benchmark uses Cargo's release bench profile on one Linux
x86_64 host (Ubuntu kernel 6.8.0-147-generic), with rustc 1.94.0 and Cargo
1.94.0. The host reported 56 available logical CPUs. A single-thread Tokio
runtime accepts 64-byte payloads under a 64 MiB budget, with terminal history
capacities of 0, 256, 4,096, and 32,768. Each measured sample times exactly
128 unique task IDs. Task transitions, retention checks, and history accounting
run outside the timed region. Each group uses three warmup batches and nine
measured batches.

The benchmark checks that each ID is newly accepted, each task reaches
`Succeeded`, and the retained terminal task count returns to the configured
history size after every batch. It checks retained records through the IDs
allocated during setup and measurement.

Times are nanoseconds per acceptance, calculated by dividing each 128-accept
batch duration by 128. The p95 is nearest rank; with nine samples it is the
largest observed sample. These are descriptive measurements, not CI thresholds.

## Pre-change baseline

This run completed before modifying `src/store/memory_task_store.rs`. The
initial harness checked retained tasks but did not print minimum and maximum;
the min/max fields are therefore unavailable for this run.

| History | Samples | Median | p95 (also max) |
| ---: | ---: | ---: | ---: |
| 0 | 9 | 859 ns | 865 ns |
| 256 | 9 | 5,201 ns | 8,258 ns |
| 4,096 | 9 | 235,821 ns | 286,314 ns |
| 32,768 | 9 | 2,025,945 ns | 3,010,893 ns |

## Alternating matched runs

The harness was then updated to verify the exact retained count by checking all
allocated IDs. With that same harness, the old and fast-path implementations
were run in alternating order. Values in the following tables are
`min / median / p95 / max`, in nanoseconds per acceptance. Each run has nine
samples, and p95 equals max for these nine-sample groups.

| History | Baseline 1 | Fast path 1 | Median change |
| ---: | ---: | ---: | ---: |
| 0 | 866 / 877 / 1,099 / 1,099 | 1,277 / 1,443 / 1,674 / 1,674 | +64.5% |
| 256 | 4,815 / 4,924 / 5,399 / 5,399 | 1,483 / 1,759 / 1,913 / 1,913 | -64.3% |
| 4,096 | 216,830 / 245,118 / 296,641 / 296,641 | 2,319 / 3,411 / 4,544 / 4,544 | -98.6% |
| 32,768 | 2,472,338 / 3,740,710 / 7,774,146 / 7,774,146 | 2,934 / 4,174 / 5,279 / 5,279 | -99.9% |

| History | Baseline 2 | Fast path 2 | Median change |
| ---: | ---: | ---: | ---: |
| 0 | 1,405 / 1,452 / 69,277 / 69,277 | 967 / 1,385 / 1,625 / 1,625 | -4.6% |
| 256 | 8,259 / 9,134 / 55,699 / 55,699 | 947 / 1,017 / 1,328 / 1,328 | -88.9% |
| 4,096 | 228,584 / 286,631 / 384,604 / 384,604 | 1,108 / 1,432 / 1,785 / 1,785 | -99.5% |
| 32,768 | 1,971,366 / 2,529,049 / 5,139,633 / 5,139,633 | 1,288 / 1,299 / 1,655 / 1,655 | -99.95% |

The empty-history result moved from 877 ns to 1,443 ns in the first pair, then
from 1,452 ns to 1,385 ns in the second. The second baseline run also had a
69.3µs outlier. These runs show substantial system load drift, so the first
pair's empty-history increase is not repeatable and cannot be attributed to
the fast path with confidence. The second adjacent pair shows no empty-history
regression. At history sizes of 256 and above, both adjacent pairs show large
reductions that exceed their within-run sample spread by orders of magnitude.

## Decision

Keep the fast path. When the existing payload total plus the new payload fits,
acceptance skips the terminal-history scan. Overflow or a request that does
not fit continues through the existing reclaimability and eviction logic. The
matched measurements support a large-history improvement; the empty-history
results are noisy but do not show a repeatable regression. This single-host
measurement does not establish application latency or guarantee similar gains
on other hardware and workloads.

## Commands and artifacts

Before the production change:

```text
cargo bench --bench memory_accept
exit: 0
```

After the production change:

```text
cargo test --test store_tests memory
exit: 0 (13 passed, 1 filtered out)

cargo bench --bench memory_accept
exit: 0
```

The tables above preserve the measured results and comparison. Temporary
benchmark logs were used during validation and are removed after the task.

Cargo refreshed `Cargo.lock` during the first benchmark invocation even though
the benchmark adds no dependency. Re-running `cargo bench --locked
--bench memory_accept` with the original lock exited 101 because Cargo said the
lockfile needed updating. The generated refresh is retained for locked
resolution. It only disambiguates the registry `qubit-value` package from a
same-named local package in the lock file; it adds no dependency.
