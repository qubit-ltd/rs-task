use std::path::PathBuf;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use qubit_task::model::TaskCursor;
use qubit_task::model::TaskId;

/// Deterministic database paths and a real history position near its tail.
pub struct HistoryDataset {
    pub database_path: PathBuf,
    pub cursor: TaskCursor,
    pub correlation_key: String,
}

/// Creates a schema-version-3 database with deterministic legal task rows.
/// Secondary indexes are deliberately left to SqliteTaskStore::open so the
/// first-open/index-build cost can be measured independently of seeding.
pub fn create(size: usize, directory: &std::path::Path) -> Result<HistoryDataset, Box<dyn std::error::Error>> {
    use qubit_task::model::TaskRequest;
    use qubit_task::model::TaskRequestInfo;
    use rusqlite::Connection;
    use rusqlite::params;
    use serde_json::json;

    let database_path = directory.join(format!("history-{size}.sqlite"));
    let mut connection = Connection::open(&database_path)?;
    connection.execute_batch(
        "CREATE TABLE tasks (
            id TEXT PRIMARY KEY NOT NULL,
            state_kind TEXT NOT NULL,
            accepted_at INTEGER NOT NULL,
            correlation_key TEXT,
            idempotency_key TEXT UNIQUE,
            request_info_json TEXT NOT NULL,
            payload BLOB NOT NULL,
            record_format_version INTEGER NOT NULL DEFAULT 3,
            lifecycle_json TEXT NOT NULL
        );
        CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);
        PRAGMA user_version=3;",
    )?;

    let transaction = connection.transaction()?;
    let mut insert = transaction.prepare(
        "INSERT INTO tasks (id,state_kind,accepted_at,correlation_key,idempotency_key,request_info_json,payload,record_format_version,lifecycle_json)
         VALUES (?1,?2,?3,?4,NULL,?5,X'',3,?6)",
    )?;
    let mut rng = FixedRng(0x6a09_e667_f3bc_c909);
    let mut cursor = TaskCursor::new(0, TaskId::generate());
    let mut correlation_key = String::from("correlation-0");
    for index in 0..size {
        let random = rng.next();
        let id = format!("00000000-0000-4000-8000-{index:012x}");
        let task_id: TaskId = serde_json::from_str(&format!("\"{id}\""))?;
        let accepted_at = (index as i64) * 1000 + (random % 1000) as i64;
        let bucket = index % 1000;
        let (state_kind, state_json) = match bucket {
            0..=7 => ("Queued", json!("Queued")),
            8 => ("Running", json!("Running")),
            9..=48 => ("Blocked", json!({"Blocked":{"reason":"benchmark"}})),
            49..=98 => ("Failed", json!({"Failed":{"category":"benchmark","message":"fixture"}})),
            99..=148 => ("Cancelled", json!("Cancelled")),
            _ => ("Succeeded", json!("Succeeded")),
        };
        let key = (random >> 12) % 32;
        let correlation = if random & 7 == 0 {
            None
        } else {
            Some(format!("correlation-{key}"))
        };
        let request = TaskRequest::new("benchmark", "1", Vec::new());
        let mut request_info = TaskRequestInfo::from(&request);
        request_info.correlation_key = correlation.clone();
        let request_info_json = serde_json::to_string(&request_info)?;
        let lifecycle_json = json!({
            "id": task_id,
            "state": state_json,
            "state_version": 1,
            "attempt": 0,
            "retry_not_before_ms": null,
            "accepted_at_ms": accepted_at,
            "started_at_ms": null,
            "finished_at_ms": null,
            "assigned_resources": [],
            "output": null,
            "cancel_requested": false
        })
        .to_string();
        insert.execute(params![
            id,
            state_kind,
            accepted_at,
            correlation,
            request_info_json,
            lifecycle_json
        ])?;
        if index == size * 70 / 100 {
            cursor = TaskCursor::new(accepted_at as u64, task_id);
            correlation_key = format!("correlation-{key}");
        }
    }
    drop(insert);
    transaction.commit()?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    Ok(HistoryDataset {
        database_path,
        cursor,
        correlation_key,
    })
}

/// Small fixed xorshift generator; no platform RNG or dependency is involved.
struct FixedRng(u64);

impl FixedRng {
    fn next(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}

/// Creates a unique directory under the system temporary workspace.
pub struct TemporaryDirectory {
    pub path: PathBuf,
}

impl Drop for TemporaryDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

pub fn temporary_directory() -> Result<TemporaryDirectory, Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = std::env::temp_dir().join(format!("qubit-task-sqlite-history-{nonce}"));
    std::fs::create_dir(&path)?;
    Ok(TemporaryDirectory { path })
}

/// Chooses a persistent JSON filename inside the system temporary workspace.
pub fn temporary_output_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    Ok(std::env::temp_dir().join(format!("qubit-task-sqlite-history-{nonce}.json")))
}
