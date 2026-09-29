#[path = "support/history_dataset.rs"]
mod history_dataset;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use qubit_task::TaskExecutionServiceBuilder;
use qubit_task::model::TaskCursor;
use qubit_task::model::TaskQuery;
use qubit_task::store::SqliteTaskStore;
use qubit_task::store::TaskStore;
use rusqlite::Connection;
use rusqlite::params;
use serde::Serialize;
use tokio::runtime::Builder;

const QUERY_SAMPLES: usize = 25;
const QUERY_WARMUPS: usize = 3;
const SHUTDOWN_SAMPLES: usize = 5;
const PAGE_SIZE: usize = 32;

#[derive(Serialize)]
struct Report {
    crate_name: &'static str,
    crate_version: &'static str,
    sqlite_version: &'static str,
    features: &'static [&'static str],
    flush_checkpoint_strategy: &'static str,
    datasets: Vec<DatasetReport>,
}

#[derive(Serialize)]
struct DatasetReport {
    rows: usize,
    seed: &'static str,
    indexless_database_bytes: u64,
    legacy_index_build_ms: f64,
    legacy_indexed_database_bytes: u64,
    first_store_open_and_compound_index_build_ms: f64,
    indexed_database_bytes: u64,
    indexed_wal_bytes_after_checkpoint: u64,
    query_samples: usize,
    query_warmups: usize,
    no_filter_page: Timing,
    deep_no_filter_page: Timing,
    deep_single_correlation_page: Timing,
    unfinished_recovery_page: Timing,
    legacy_or_no_index_baseline: Timing,
    legacy_or_baseline: Timing,
    count_states: Timing,
    shutdown_samples: usize,
    shutdown_timing_strategy: &'static str,
    shutdown: Timing,
    query_plans: QueryPlans,
}

#[derive(Serialize)]
struct Timing {
    p50_ms: f64,
    p95_ms: f64,
}

#[derive(Serialize)]
struct QueryPlans {
    deep_no_filter: Vec<String>,
    deep_single_correlation: Vec<String>,
    unfinished_recovery: Vec<String>,
    legacy_or_no_index_baseline: Vec<String>,
    legacy_or_baseline: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_args()?;
    let output = match options.output {
        Some(path) => path,
        None => history_dataset::temporary_output_path()?,
    };
    if !output.is_absolute() {
        return Err("--output must be an absolute path".into());
    }
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()?;
    let mut datasets = Vec::new();
    for size in options.sizes {
        datasets.push(run_dataset(&runtime, size)?);
    }
    let report = Report {
        crate_name: env!("CARGO_PKG_NAME"),
        crate_version: env!("CARGO_PKG_VERSION"),
        sqlite_version: rusqlite::version(),
        features: &["sqlite"],
        flush_checkpoint_strategy: "after seed and after measurements: PRAGMA wal_checkpoint(TRUNCATE); latency excludes seed, checkpoint, and index construction",
        datasets,
    };
    std::fs::write(&output, serde_json::to_vec_pretty(&report)?)?;
    println!("wrote {}", output.display());
    Ok(())
}

struct Options {
    output: Option<PathBuf>,
    sizes: Vec<usize>,
}

fn parse_args() -> Result<Options, Box<dyn std::error::Error>> {
    let mut output = None;
    let mut sizes = vec![10_000, 100_000];
    let mut args = std::env::args().skip(1);
    while let Some(argument) = args.next() {
        match argument.as_str() {
            "--output" => output = Some(PathBuf::from(args.next().ok_or("--output needs a path")?)),
            "--sizes" => {
                let raw = args.next().ok_or("--sizes needs comma-separated values")?;
                sizes = raw
                    .split(',')
                    .map(|value| {
                        value
                            .parse::<usize>()
                            .map_err(|_| format!("invalid dataset size: {value}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if sizes.is_empty()
                    || sizes
                        .iter()
                        .any(|size| ![10_000, 20_000, 100_000].contains(size))
                {
                    return Err("supported sizes are 10000, 20000, and 100000".into());
                }
            }
            "--bench" => {}
            "--help" | "-h" => {
                println!("sqlite_history [--output <absolute-json>] [--sizes 10000,20000,100000]");
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok(Options { output, sizes })
}

fn run_dataset(
    runtime: &tokio::runtime::Runtime,
    size: usize,
) -> Result<DatasetReport, Box<dyn std::error::Error>> {
    let directory = history_dataset::temporary_directory()?;
    let dataset = history_dataset::create(size, &directory.path)?;
    let indexless_database_bytes = file_size(&dataset.database_path)?;

    let mut no_index_connection = Connection::open(&dataset.database_path)?;
    let legacy_or_no_index_baseline =
        legacy_history_measurement(&mut no_index_connection, dataset.cursor)?;
    let legacy_or_no_index_plan = explain(
        &no_index_connection,
        "SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,lifecycle_json FROM tasks
         WHERE (?1 IS NULL OR accepted_at > ?1 OR (accepted_at = ?1 AND id > ?2))
           AND (?3 IS NULL OR correlation_key = ?3)
         ORDER BY accepted_at,id LIMIT ?4",
        params![dataset.cursor.accepted_at_ms as i64, dataset.cursor.id.to_string(), Option::<String>::None, 33i64],
    )?;
    drop(no_index_connection);

    let legacy_index_start = Instant::now();
    {
        let connection = Connection::open(&dataset.database_path)?;
        connection.execute_batch(
            "CREATE INDEX tasks_state_accepted ON tasks(state_kind, accepted_at);
             CREATE INDEX tasks_accepted_id ON tasks(accepted_at, id);",
        )?;
    }
    let legacy_index_build_ms = elapsed_ms(legacy_index_start.elapsed());
    let legacy_checkpoint = Connection::open(&dataset.database_path)?;
    legacy_checkpoint.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(legacy_checkpoint);
    let legacy_indexed_database_bytes = file_size(&dataset.database_path)?;

    let mut legacy_connection = Connection::open(&dataset.database_path)?;
    let legacy_or_baseline = legacy_history_measurement(&mut legacy_connection, dataset.cursor)?;
    let legacy_or_baseline_plan = explain(
        &legacy_connection,
        "SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,lifecycle_json FROM tasks
         WHERE (?1 IS NULL OR accepted_at > ?1 OR (accepted_at = ?1 AND id > ?2))
           AND (?3 IS NULL OR correlation_key = ?3)
         ORDER BY accepted_at,id LIMIT ?4",
        params![dataset.cursor.accepted_at_ms as i64, dataset.cursor.id.to_string(), Option::<String>::None, 33i64],
    )?;
    drop(legacy_connection);

    let open_start = Instant::now();
    let store = SqliteTaskStore::open(&dataset.database_path)?;
    let first_store_open_and_compound_index_build_ms = elapsed_ms(open_start.elapsed());
    let sqlite = Connection::open(&dataset.database_path)?;
    sqlite.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    let indexed_database_bytes = file_size(&dataset.database_path)?;
    let deep_no_filter_plan = production_plan(
        &sqlite,
        TaskQuery {
            after: Some(dataset.cursor),
            limit: PAGE_SIZE,
            ..TaskQuery::default()
        },
    )?;
    let deep_single_correlation_plan = production_plan(
        &sqlite,
        TaskQuery {
            after: Some(dataset.cursor),
            correlation_key: Some(dataset.correlation_key.clone()),
            limit: PAGE_SIZE,
            ..TaskQuery::default()
        },
    )?;
    let unfinished_recovery_plan = explain(
        &sqlite,
        "SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,lifecycle_json FROM tasks
         WHERE +state_kind IN ('Queued','Running') AND (accepted_at,id) > (?1,?2)
         ORDER BY accepted_at,id LIMIT 257",
        params![dataset.cursor.accepted_at_ms as i64, dataset.cursor.id.to_string()],
    )?;

    let runtime_store = &store;
    let first_id = dataset.cursor.id;
    let verified = runtime.block_on(runtime_store.get_summary(first_id))?;
    if verified.is_none() {
        return Err("store read failed to decode deterministic benchmark row".into());
    }
    let no_filter_page = measure(runtime, || async {
        let page = runtime_store
            .list(TaskQuery {
                limit: PAGE_SIZE,
                ..TaskQuery::default()
            })
            .await?;
        Ok(page.records.len())
    })?;
    let deep_no_filter_page = measure(runtime, || async {
        let page = runtime_store
            .list(TaskQuery {
                after: Some(dataset.cursor),
                limit: PAGE_SIZE,
                ..TaskQuery::default()
            })
            .await?;
        Ok(page.records.len())
    })?;
    let deep_single_correlation_page = measure(runtime, || async {
        let page = runtime_store
            .list(TaskQuery {
                after: Some(dataset.cursor),
                correlation_key: Some(dataset.correlation_key.clone()),
                limit: PAGE_SIZE,
                ..TaskQuery::default()
            })
            .await?;
        Ok(page.records.len())
    })?;
    let unfinished_recovery_page = measure(runtime, || async {
        let page = runtime_store.scan_unfinished(Some(dataset.cursor)).await?;
        Ok(page.tasks.len())
    })?;
    let count_states = measure(runtime, || async {
        Ok(runtime_store.count_states().await?.queued)
    })?;
    drop(sqlite);
    drop(store);
    let checkpoint = Connection::open(&dataset.database_path)?;
    checkpoint.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    let indexed_wal_bytes_after_checkpoint = sidecar_size(&dataset.database_path, "-wal");
    drop(checkpoint);

    let shutdown = measure_shutdown(runtime, &dataset.database_path)?;
    let query_plans = QueryPlans {
        deep_no_filter: deep_no_filter_plan,
        deep_single_correlation: deep_single_correlation_plan,
        unfinished_recovery: unfinished_recovery_plan,
        legacy_or_no_index_baseline: legacy_or_no_index_plan,
        legacy_or_baseline: legacy_or_baseline_plan,
    };
    Ok(DatasetReport {
        rows: size,
        seed: "xorshift64 seed 0x6a09e667f3bcc909; deterministic UUID-shaped IDs; identical prefix across sizes",
        indexless_database_bytes,
        legacy_index_build_ms,
        legacy_indexed_database_bytes,
        first_store_open_and_compound_index_build_ms,
        indexed_database_bytes,
        indexed_wal_bytes_after_checkpoint,
        query_samples: QUERY_SAMPLES,
        query_warmups: QUERY_WARMUPS,
        no_filter_page,
        deep_no_filter_page,
        deep_single_correlation_page,
        unfinished_recovery_page,
        legacy_or_no_index_baseline,
        legacy_or_baseline,
        count_states,
        shutdown_samples: SHUTDOWN_SAMPLES,
        shutdown_timing_strategy: "each sample builds a fresh service from a same-seed checkpointed DB copy before timing; build/recovery is excluded and only service.shutdown() is timed",
        shutdown,
        query_plans,
    })
}

fn measure<F, Fut, T>(
    runtime: &tokio::runtime::Runtime,
    mut operation: F,
) -> Result<Timing, Box<dyn std::error::Error>>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, qubit_task::store::StoreError>>,
{
    for _ in 0..QUERY_WARMUPS {
        let _ = runtime.block_on(operation())?;
    }
    let mut samples = Vec::with_capacity(QUERY_SAMPLES);
    for _ in 0..QUERY_SAMPLES {
        let start = Instant::now();
        let _ = runtime.block_on(operation())?;
        samples.push(start.elapsed());
    }
    Ok(summarize(samples))
}

fn measure_shutdown(
    runtime: &tokio::runtime::Runtime,
    database_path: &Path,
) -> Result<Timing, Box<dyn std::error::Error>> {
    let mut samples = Vec::with_capacity(SHUTDOWN_SAMPLES);
    let directory = history_dataset::temporary_directory()?;
    for sample in 0..SHUTDOWN_SAMPLES {
        let sample_path = directory.path.join(format!("shutdown-{sample}.sqlite"));
        std::fs::copy(database_path, &sample_path)?;
        let service = runtime.block_on(async {
            TaskExecutionServiceBuilder::recoverable_sqlite(&sample_path)?
                .build()
                .await
        })?;
        let start = Instant::now();
        runtime.block_on(service.shutdown())?;
        samples.push(start.elapsed());
    }
    Ok(summarize(samples))
}

fn legacy_history_measurement(
    connection: &mut Connection,
    cursor: TaskCursor,
) -> Result<Timing, Box<dyn std::error::Error>> {
    let sql = "SELECT id FROM tasks
        WHERE (?1 IS NULL OR accepted_at > ?1 OR (accepted_at = ?1 AND id > ?2))
          AND (?3 IS NULL OR correlation_key = ?3)
        ORDER BY accepted_at,id LIMIT ?4";
    let mut statement = connection.prepare(sql)?;
    let mut run = || -> Result<usize, rusqlite::Error> {
        let rows = statement.query_map(
            params![
                cursor.accepted_at_ms as i64,
                cursor.id.to_string(),
                Option::<String>::None,
                33i64
            ],
            |row| row.get::<_, String>(0),
        )?;
        Ok(rows.count())
    };
    for _ in 0..QUERY_WARMUPS {
        let _ = run()?;
    }
    let mut samples = Vec::with_capacity(QUERY_SAMPLES);
    for _ in 0..QUERY_SAMPLES {
        let start = Instant::now();
        let _ = run()?;
        samples.push(start.elapsed());
    }
    Ok(summarize(samples))
}

fn production_plan(
    connection: &Connection,
    query: TaskQuery,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // The plan is read through the public store query in production; this EXPLAIN
    // uses its stable SQL shape for reproducible plan reporting.
    let (where_sql, values): (&str, Vec<rusqlite::types::Value>) = if let Some(key) =
        query.correlation_key
    {
        (
            "WHERE (accepted_at,id) > (?1,?2) AND correlation_key = ?3 ORDER BY accepted_at,id LIMIT ?4",
            vec![
                rusqlite::types::Value::Integer(
                    query.after.expect("cursor was set").accepted_at_ms as i64,
                ),
                rusqlite::types::Value::Text(query.after.expect("cursor was set").id.to_string()),
                rusqlite::types::Value::Text(key),
                rusqlite::types::Value::Integer((query.limit + 1) as i64),
            ],
        )
    } else {
        (
            "WHERE (accepted_at,id) > (?1,?2) ORDER BY accepted_at,id LIMIT ?3",
            vec![
                rusqlite::types::Value::Integer(
                    query.after.expect("cursor was set").accepted_at_ms as i64,
                ),
                rusqlite::types::Value::Text(query.after.expect("cursor was set").id.to_string()),
                rusqlite::types::Value::Integer((query.limit + 1) as i64),
            ],
        )
    };
    explain(
        connection,
        &format!(
            "SELECT id,state_kind,accepted_at,correlation_key,idempotency_key,record_format_version,request_info_json,lifecycle_json FROM tasks {where_sql}"
        ),
        rusqlite::params_from_iter(values),
    )
}

fn explain<P: rusqlite::Params>(
    connection: &Connection,
    sql: &str,
    params: P,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut statement = connection.prepare(&format!("EXPLAIN QUERY PLAN {sql}"))?;
    let details = statement.query_map(params, |row| row.get::<_, String>(3))?;
    Ok(details.collect::<Result<Vec<_>, _>>()?)
}

fn summarize(mut samples: Vec<Duration>) -> Timing {
    samples.sort_unstable();
    let percentile =
        |percent: usize| samples[(samples.len() - 1) * percent / 100].as_secs_f64() * 1000.0;
    Timing {
        p50_ms: percentile(50),
        p95_ms: percentile(95),
    }
}

fn elapsed_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
fn file_size(path: &Path) -> Result<u64, std::io::Error> {
    Ok(std::fs::metadata(path)?.len())
}
fn sidecar_size(path: &Path, suffix: &str) -> u64 {
    let sidecar = PathBuf::from(format!("{}{suffix}", path.display()));
    std::fs::metadata(sidecar)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}
