use super::{FileState, IngestState, OpencodeDatabaseState};
use crate::lease::IngestLease;
use anyhow::{Context, Result, bail, ensure};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};

mod codec;
mod lifecycle;
#[cfg(test)]
mod tests;

const DATABASE: &str = "checkpoints.sqlite";
const LOCK: &str = ".checkpoints.lock";
const FORMAT_VERSION: i64 = 1;
const MARKER_PREFIX: &str = "memex-checkpoints:";

#[derive(Debug)]
pub(crate) struct CheckpointHeader {
    pub next_doc_id: u64,
    pub opencode_databases: HashMap<String, OpencodeDatabaseState>,
}

#[derive(Default)]
pub(crate) struct CheckpointDelta {
    pub upserts: HashMap<String, FileState>,
    pub deletes: HashSet<String>,
    pub clear_files: bool,
    pub next_doc_id: Option<u64>,
    pub opencode_databases: Option<HashMap<String, OpencodeDatabaseState>>,
}

pub(crate) struct CheckpointReader {
    backend: Backend,
}

enum Backend {
    Legacy(Value),
    Sqlite {
        connection: Connection,
        _lease: File,
    },
}

pub(crate) struct CheckpointWriter {
    reader: CheckpointReader,
}

impl CheckpointReader {
    pub(crate) fn open(state_path: &Path) -> Result<Self> {
        lifecycle::open_reader(state_path)
    }

    pub(crate) fn header(&self) -> Result<CheckpointHeader> {
        match &self.backend {
            Backend::Legacy(value) => {
                let state: IngestState = serde_json::from_value(value.clone())?;
                Ok(CheckpointHeader {
                    next_doc_id: state.next_doc_id,
                    opencode_databases: state.opencode_databases,
                })
            }
            Backend::Sqlite { connection, .. } => {
                let (next_id, databases): (String, String) = connection.query_row(
                    "SELECT next_doc_id, opencode_databases FROM metadata WHERE singleton=1",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )?;
                Ok(CheckpointHeader {
                    next_doc_id: parse_next_id(&next_id)?,
                    opencode_databases: serde_json::from_str(&databases)?,
                })
            }
        }
    }

    pub(crate) fn load_files(
        &self,
        paths: &[String],
    ) -> Result<HashMap<String, Option<FileState>>> {
        crate::profiling::span!("state.checkpoint.load_files");
        let mut result = HashMap::with_capacity(paths.len());
        match &self.backend {
            Backend::Legacy(value) => {
                for path in paths {
                    let file = value["files"]
                        .get(path)
                        .cloned()
                        .map(serde_json::from_value)
                        .transpose()?;
                    result.insert(path.clone(), file);
                }
            }
            Backend::Sqlite { connection, .. } => {
                let transaction = connection.unchecked_transaction()?;
                {
                    let mut statement =
                        transaction.prepare_cached("SELECT payload FROM files WHERE path=?1")?;
                    for path in paths {
                        let payload: Option<String> =
                            statement.query_row([path], |row| row.get(0)).optional()?;
                        crate::profiling::count!(
                            "state.checkpoint.rows_decoded",
                            usize::from(payload.is_some())
                        );
                        let file = payload
                            .map(|json| serde_json::from_str(&json))
                            .transpose()?;
                        result.insert(path.clone(), file);
                    }
                }
                transaction.commit()?;
            }
        }
        Ok(result)
    }

    pub(crate) fn contains_file(&self, path: &str) -> Result<bool> {
        match &self.backend {
            Backend::Legacy(value) => Ok(value["files"].get(path).is_some()),
            Backend::Sqlite { connection, .. } => Ok(connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM files WHERE path=?1)",
                [path],
                |row| row.get(0),
            )?),
        }
    }

    pub(crate) fn file_keys(&self) -> Result<Vec<String>> {
        crate::profiling::count!("state.checkpoint.key_scans", 1);
        match &self.backend {
            Backend::Legacy(value) => Ok(value["files"]
                .as_object()
                .context("invalid legacy files")?
                .keys()
                .cloned()
                .collect()),
            Backend::Sqlite { connection, .. } => {
                let mut statement = connection.prepare("SELECT path FROM files")?;
                Ok(statement
                    .query_map([], |row| row.get(0))?
                    .collect::<rusqlite::Result<_>>()?)
            }
        }
    }

    pub(crate) fn has_files_excluding(&self, excluded: &HashSet<String>) -> Result<bool> {
        crate::profiling::count!("state.checkpoint.key_scans", 1);
        match &self.backend {
            Backend::Legacy(value) => Ok(value["files"]
                .as_object()
                .context("invalid legacy files")?
                .keys()
                .any(|path| !excluded.contains(path))),
            Backend::Sqlite { connection, .. } => {
                let mut statement = connection.prepare("SELECT path FROM files")?;
                let mut rows = statement.query([])?;
                while let Some(row) = rows.next()? {
                    let path: String = row.get(0)?;
                    if !excluded.contains(&path) {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    pub(crate) fn hot_files_since(&self, since: i64) -> Result<HashMap<String, FileState>> {
        match &self.backend {
            Backend::Legacy(value) => {
                let state: IngestState = serde_json::from_value(value.clone())?;
                Ok(state
                    .files
                    .into_iter()
                    .filter(|(_, file)| file.mtime >= since)
                    .collect())
            }
            Backend::Sqlite { connection, .. } => {
                let mut statement =
                    connection.prepare("SELECT path, payload FROM files WHERE mtime >= ?1")?;
                decode_rows(&mut statement, [since])
            }
        }
    }

    /// Paths whose source is a SQLite store. Their main file's mtime can stay cold for a
    /// whole session because commits live in the write-ahead log, so they are watched
    /// through the log rather than stat-compared with ordinary transcripts.
    pub(crate) fn sqlite_backed_paths(&self) -> Result<Vec<String>> {
        match &self.backend {
            Backend::Legacy(value) => {
                let state: IngestState = serde_json::from_value(value.clone())?;
                Ok(state
                    .files
                    .into_iter()
                    .filter(|(_, file)| file.identity.sqlite_wal.is_some())
                    .map(|(path, _)| path)
                    .collect())
            }
            Backend::Sqlite { connection, .. } => {
                let mut statement = connection.prepare(
                    "SELECT path FROM files WHERE json_extract(payload, '$.identity.sqlite_wal') IS NOT NULL",
                )?;
                let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
                Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
            }
        }
    }

    pub(crate) fn snapshot(&self) -> Result<IngestState> {
        Ok(serde_json::from_value(self.export_json()?)?)
    }

    pub(crate) fn export_json(&self) -> Result<Value> {
        match &self.backend {
            Backend::Legacy(value) => Ok(value.clone()),
            Backend::Sqlite { connection, .. } => {
                let transaction = connection.unchecked_transaction()?;
                let (next_id, databases, extras): (String, String, String) = transaction.query_row(
                    "SELECT next_doc_id, opencode_databases, legacy_extras FROM metadata WHERE singleton=1", [],
                    |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                )?;
                let mut object: Map<String, Value> = serde_json::from_str(&extras)?;
                object.insert("next_doc_id".into(), Value::from(parse_next_id(&next_id)?));
                object.insert(
                    "opencode_databases".into(),
                    serde_json::from_str(&databases)?,
                );
                let mut files = Map::new();
                {
                    let mut statement = transaction.prepare("SELECT path, payload FROM files")?;
                    let mut rows = statement.query([])?;
                    while let Some(row) = rows.next()? {
                        let path: String = row.get(0)?;
                        let payload: String = row.get(1)?;
                        crate::profiling::count!("state.checkpoint.rows_decoded", 1);
                        files.insert(path, serde_json::from_str(&payload)?);
                    }
                }
                object.insert("files".into(), Value::Object(files));
                transaction.commit()?;
                Ok(Value::Object(object))
            }
        }
    }
}

impl CheckpointWriter {
    pub(crate) fn open(
        state_path: &Path,
        lease: &IngestLease,
        allow_initialize: bool,
    ) -> Result<Self> {
        lifecycle::open_writer(
            state_path,
            lease,
            allow_initialize,
            lifecycle::MigrationFailure::None,
        )
    }

    pub(crate) fn reader(&self) -> &CheckpointReader {
        &self.reader
    }

    pub(crate) fn commit_delta(&mut self, delta: &CheckpointDelta) -> Result<bool> {
        if delta.upserts.is_empty()
            && delta.deletes.is_empty()
            && !delta.clear_files
            && delta.next_doc_id.is_none()
            && delta.opencode_databases.is_none()
        {
            return Ok(false);
        }
        let Backend::Sqlite { connection, .. } = &mut self.reader.backend else {
            bail!("checkpoint writer is not SQLite");
        };
        let transaction = connection.transaction()?;
        if delta.clear_files {
            transaction.execute("DELETE FROM files", [])?;
        }
        {
            let mut delete = transaction.prepare_cached("DELETE FROM files WHERE path=?1")?;
            for path in &delta.deletes {
                delete.execute([path])?;
            }
            let mut previous =
                transaction.prepare_cached("SELECT payload FROM files WHERE path=?1")?;
            let mut upsert = transaction.prepare_cached("INSERT INTO files(path,payload) VALUES(?1,?2) ON CONFLICT(path) DO UPDATE SET payload=excluded.payload")?;
            for (path, file) in &delta.upserts {
                let old: Option<String> =
                    previous.query_row([path], |row| row.get(0)).optional()?;
                crate::profiling::count!(
                    "state.checkpoint.rows_decoded",
                    usize::from(old.is_some())
                );
                let payload = codec::file_payload(file, old.as_deref())?;
                upsert.execute(params![path, payload])?;
            }
        }
        if let Some(next_id) = delta.next_doc_id {
            transaction.execute(
                "UPDATE metadata SET next_doc_id=?1 WHERE singleton=1",
                [next_id.to_string()],
            )?;
        }
        if let Some(databases) = &delta.opencode_databases {
            let old: String = transaction.query_row(
                "SELECT opencode_databases FROM metadata WHERE singleton=1",
                [],
                |row| row.get(0),
            )?;
            transaction.execute(
                "UPDATE metadata SET opencode_databases=?1 WHERE singleton=1",
                [codec::database_payload(databases, &old)?],
            )?;
        }
        transaction.commit()?;
        crate::profiling::count!("state.checkpoint.transactions", 1);
        crate::profiling::count!("state.checkpoint.rows_upserted", delta.upserts.len());
        crate::profiling::count!("state.checkpoint.rows_deleted", delta.deletes.len());
        crate::profiling::count!("state.checkpoint.clear_files", u64::from(delta.clear_files));
        Ok(true)
    }

    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        let Backend::Sqlite { connection, .. } = &self.reader.backend else {
            bail!("checkpoint writer is not SQLite");
        };
        lifecycle::checkpoint(connection)
    }

    pub(crate) fn replace_snapshot(&mut self, state: &IngestState) -> Result<()> {
        let keys = self.reader.file_keys()?;
        self.commit_delta(&CheckpointDelta {
            upserts: state.files.clone(),
            deletes: keys
                .into_iter()
                .filter(|path| !state.files.contains_key(path))
                .collect(),
            next_doc_id: Some(state.next_doc_id),
            opencode_databases: Some(state.opencode_databases.clone()),
            ..Default::default()
        })?;
        Ok(())
    }
}

pub(super) fn save_legacy(state: &IngestState, state_path: &Path) -> Result<()> {
    lifecycle::save_legacy(state, state_path)
}

pub(crate) fn reset(state_path: &Path, lease: &IngestLease) -> Result<()> {
    lifecycle::reset(state_path, lease)
}

pub(crate) fn has_authority(state_path: &Path) -> Result<bool> {
    lifecycle::has_authority(state_path)
}

pub(crate) fn is_checkpoint_artifact_name(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    matches!(
        name,
        DATABASE
            | "checkpoints.sqlite-wal"
            | "checkpoints.sqlite-shm"
            | "checkpoints.sqlite-journal"
            | LOCK
            | "ingest.json"
    ) || name
        .strip_prefix("ingest.legacy-")
        .and_then(|suffix| suffix.strip_suffix(".json"))
        .is_some_and(|digest| {
            digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        })
}

fn parse_next_id(value: &str) -> Result<u64> {
    let id: u64 = value.parse().context("invalid checkpoint next_doc_id")?;
    ensure!(
        id.to_string() == value,
        "noncanonical checkpoint next_doc_id"
    );
    Ok(id)
}

fn decode_rows(
    statement: &mut rusqlite::Statement<'_>,
    parameters: impl rusqlite::Params,
) -> Result<HashMap<String, FileState>> {
    let mut result = HashMap::new();
    let mut rows = statement.query(parameters)?;
    while let Some(row) = rows.next()? {
        let payload: String = row.get(1)?;
        crate::profiling::count!("state.checkpoint.rows_decoded", 1);
        result.insert(row.get(0)?, serde_json::from_str(&payload)?);
    }
    Ok(result)
}
