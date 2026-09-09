use super::*;
use rusqlite::config::DbConfig;
use sha2::{Digest, Sha256};
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{Duration, Instant};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS metadata (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    format_version INTEGER NOT NULL,
    store_id TEXT NOT NULL,
    origin TEXT NOT NULL,
    next_doc_id TEXT NOT NULL CHECK(typeof(next_doc_id)='text'),
    opencode_databases TEXT NOT NULL CHECK(json_valid(opencode_databases) AND json_type(opencode_databases)='object'),
    legacy_extras TEXT NOT NULL CHECK(json_valid(legacy_extras) AND json_type(legacy_extras)='object')
);
CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY NOT NULL,
    payload TEXT NOT NULL CHECK(json_valid(payload) AND json_type(payload)='object'),
    mtime INTEGER GENERATED ALWAYS AS (json_extract(payload,'$.mtime')) STORED NOT NULL
        CHECK(json_type(payload,'$.mtime')='integer' AND typeof(mtime)='integer')
);
CREATE INDEX IF NOT EXISTS files_mtime ON files(mtime);
";

pub(super) enum MigrationFailure {
    None,
    #[cfg(test)]
    At(&'static str),
}

impl MigrationFailure {
    fn check(&self, _point: &str) -> Result<()> {
        #[cfg(test)]
        if matches!(self, Self::At(point) if *point == _point) {
            bail!("injected migration failure at {_point}");
        }
        Ok(())
    }
}

enum Authority {
    Missing,
    Legacy { raw: Vec<u8>, value: Value },
    Marker(String),
}

fn authority(path: &Path) -> Result<Authority> {
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Authority::Missing);
        }
        Err(error) => return Err(error.into()),
    };
    let value: Value =
        serde_json::from_slice(&raw).context("invalid ingest checkpoint authority")?;
    if let Some(marker) = value.as_str() {
        let marker = marker
            .strip_prefix(MARKER_PREFIX)
            .context("unknown ingest checkpoint marker")?;
        let (version, store_id) = marker
            .split_once(':')
            .context("malformed ingest checkpoint marker")?;
        ensure!(
            version == FORMAT_VERSION.to_string(),
            "unsupported checkpoint marker version {version}"
        );
        ensure!(
            valid_identity(store_id),
            "invalid checkpoint store identity"
        );
        return Ok(Authority::Marker(store_id.to_owned()));
    }
    serde_json::from_slice::<IngestState>(&raw).context("invalid legacy ingest checkpoint")?;
    Ok(Authority::Legacy { raw, value })
}

fn valid_identity(identity: &str) -> bool {
    identity.len() == 64
        && identity
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn location(state_path: &Path, name: &str) -> Result<PathBuf> {
    Ok(super::super::parent_directory(state_path)?.join(name))
}

fn lifecycle_lock(state_path: &Path, exclusive: bool, create: bool) -> Result<File> {
    let path = location(state_path, LOCK)?;
    if create {
        fs::create_dir_all(super::super::parent_directory(state_path)?)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(create)
        .create(create)
        .truncate(false)
        .open(&path)
        .with_context(|| {
            format!(
                "missing or inaccessible checkpoint lifecycle lock {}",
                path.display()
            )
        })?;
    let started = Instant::now();
    loop {
        let result = if exclusive {
            file.try_lock()
        } else {
            file.try_lock_shared()
        };
        match result {
            Ok(()) => return Ok(file),
            Err(TryLockError::WouldBlock) if started.elapsed() < Duration::from_millis(500) => {
                std::thread::sleep(Duration::from_millis(10))
            }
            Err(TryLockError::WouldBlock) => {
                bail!("checkpoint lifecycle lock is busy: {}", path.display())
            }
            Err(TryLockError::Error(error)) => {
                return Err(error).context("failed to lock checkpoint lifecycle");
            }
        }
    }
}

fn open_connection(state_path: &Path, writable: bool, create: bool) -> Result<Connection> {
    let mut flags = if writable {
        OpenFlags::SQLITE_OPEN_READ_WRITE
    } else {
        OpenFlags::SQLITE_OPEN_READ_ONLY
    };
    flags |= OpenFlags::SQLITE_OPEN_NO_MUTEX;
    if create {
        flags |= OpenFlags::SQLITE_OPEN_CREATE;
    }
    let connection = Connection::open_with_flags(location(state_path, DATABASE)?, flags)
        .context("cannot open authoritative checkpoint database")?;
    connection.busy_timeout(Duration::from_millis(500))?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
    Ok(connection)
}

fn configure_writer(connection: &Connection) -> Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "FULL")?;
    connection.pragma_update(None, "fullfsync", true)?;
    connection.pragma_update(None, "checkpoint_fullfsync", true)?;
    connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
    connection.pragma_update(None, "wal_autocheckpoint", 1000)?;
    connection.pragma_update(None, "journal_size_limit", 16 * 1024 * 1024)?;
    let mode: String = connection.pragma_query_value(None, "journal_mode", |row| row.get(0))?;
    ensure!(
        mode.eq_ignore_ascii_case("wal"),
        "checkpoint journal_mode is not WAL"
    );
    for (name, expected) in [
        ("synchronous", 2),
        ("fullfsync", 1),
        ("checkpoint_fullfsync", 1),
        ("wal_autocheckpoint", 1000),
        ("journal_size_limit", 16 * 1024 * 1024),
    ] {
        let value: i64 = connection.pragma_query_value(None, name, |row| row.get(0))?;
        ensure!(
            value == expected,
            "checkpoint {name} is {value}, expected {expected}"
        );
    }
    ensure!(
        connection.db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE)?,
        "checkpoint close maintenance could not be disabled"
    );
    Ok(())
}

fn validate(connection: &Connection, expected_identity: &str) -> Result<()> {
    let (version, identity, next_id, databases, extras): (i64, String, String, String, String) = connection.query_row(
        "SELECT format_version,store_id,next_doc_id,opencode_databases,legacy_extras FROM metadata WHERE singleton=1", [],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
    ).context("invalid checkpoint metadata")?;
    ensure!(
        version == FORMAT_VERSION,
        "unsupported checkpoint database format {version}"
    );
    ensure!(
        identity == expected_identity,
        "checkpoint store identity mismatch"
    );
    parse_next_id(&next_id)?;
    serde_json::from_str::<HashMap<String, OpencodeDatabaseState>>(&databases)?;
    serde_json::from_str::<Map<String, Value>>(&extras)?;
    connection
        .prepare("SELECT path,payload,mtime FROM files INDEXED BY files_mtime WHERE mtime>=?1")?;
    Ok(())
}

pub(super) fn open_reader(state_path: &Path) -> Result<CheckpointReader> {
    match authority(state_path)? {
        Authority::Legacy { value, .. } => Ok(CheckpointReader {
            backend: Backend::Legacy(value),
        }),
        Authority::Missing => {
            validate_missing(state_path)?;
            Ok(CheckpointReader {
                backend: Backend::Legacy(serde_json::to_value(IngestState::default())?),
            })
        }
        Authority::Marker(identity) => open_active(state_path, &identity, false),
    }
}

fn open_active(state_path: &Path, identity: &str, writable: bool) -> Result<CheckpointReader> {
    let lease = lifecycle_lock(state_path, false, false)?;
    ensure!(
        matches!(authority(state_path)?, Authority::Marker(current) if current == identity),
        "checkpoint authority changed while opening"
    );
    let connection = open_connection(state_path, writable, false)?;
    validate(&connection, identity)?;
    if writable {
        configure_writer(&connection)?;
    }
    Ok(CheckpointReader {
        backend: Backend::Sqlite {
            connection,
            _lease: lease,
        },
    })
}

pub(super) fn has_authority(state_path: &Path) -> Result<bool> {
    match authority(state_path)? {
        Authority::Missing => {
            validate_missing(state_path)?;
            Ok(false)
        }
        Authority::Legacy { .. } => Ok(true),
        Authority::Marker(identity) => {
            open_active(state_path, &identity, false)?;
            Ok(true)
        }
    }
}

fn validate_missing(state_path: &Path) -> Result<()> {
    if !location(state_path, DATABASE)?.try_exists()? {
        return Ok(());
    }
    let mut lease = lifecycle_lock(state_path, false, false)?;
    ensure!(
        matches!(authority(state_path)?, Authority::Missing),
        "checkpoint authority changed while opening"
    );
    validate_bootstrap(state_path, &mut lease)
}

fn bootstrap_identity(lease: &mut File) -> Result<String> {
    lease.seek(SeekFrom::Start(0))?;
    let mut receipt = String::new();
    lease.read_to_string(&mut receipt)?;
    let identity = receipt
        .strip_prefix("bootstrap:")
        .context("checkpoint database has no authority or bootstrap receipt")?;
    ensure!(
        valid_identity(identity),
        "invalid checkpoint bootstrap identity"
    );
    Ok(identity.to_owned())
}

fn validate_bootstrap(state_path: &Path, lease: &mut File) -> Result<()> {
    let identity = bootstrap_identity(lease)?;
    let connection = open_connection(state_path, false, false)?;
    let tables: i64 = connection.query_row(
        "SELECT count(*) FROM sqlite_master WHERE type='table'",
        [],
        |row| row.get(0),
    )?;
    if tables == 0 {
        return Ok(());
    }
    validate(&connection, &identity)?;
    let empty: bool = connection.query_row(
        "SELECT origin=?1 AND next_doc_id='1' AND opencode_databases='{}' AND legacy_extras='{}' AND NOT EXISTS(SELECT 1 FROM files) FROM metadata WHERE singleton=1",
        [format!("bootstrap:{identity}")], |row| row.get(0),
    )?;
    ensure!(
        empty,
        "populated checkpoint database has no authority; restore a consistent snapshot or rebuild"
    );
    Ok(())
}

pub(super) fn open_writer(
    state_path: &Path,
    _ingest_lease: &IngestLease,
    allow_initialize: bool,
    failure: MigrationFailure,
) -> Result<CheckpointWriter> {
    match authority(state_path)? {
        Authority::Marker(identity) => {
            return Ok(CheckpointWriter {
                reader: open_active(state_path, &identity, true)?,
            });
        }
        Authority::Missing if !allow_initialize => {
            bail!(
                "missing checkpoint authority; initialization requires empty-root or pending recovery validation"
            );
        }
        _ => {}
    }
    crate::profiling::span!("state.checkpoint.migrate");
    let mut lease = lifecycle_lock(state_path, true, true)?;
    let authority = authority(state_path)?;
    if let Authority::Marker(identity) = authority {
        drop(lease);
        return Ok(CheckpointWriter {
            reader: open_active(state_path, &identity, true)?,
        });
    }
    let (value, identity, origin) = match authority {
        Authority::Legacy { raw, value } => {
            failure.check("before_backup")?;
            let digest = format!("{:x}", Sha256::digest(&raw));
            backup(state_path, &raw, &digest)?;
            failure.check("after_backup")?;
            (value, new_identity()?, format!("legacy:{digest}"))
        }
        Authority::Missing => {
            ensure!(
                allow_initialize,
                "missing checkpoint authority; initialization requires empty-root or pending recovery validation"
            );
            let identity = if location(state_path, DATABASE)?.try_exists()? {
                validate_bootstrap(state_path, &mut lease)?;
                bootstrap_identity(&mut lease)?
            } else {
                let identity = new_identity()?;
                lease.set_len(0)?;
                lease.seek(SeekFrom::Start(0))?;
                write!(lease, "bootstrap:{identity}")?;
                lease.sync_all()?;
                super::super::sync_directory(super::super::parent_directory(state_path)?)?;
                identity
            };
            (
                serde_json::to_value(IngestState::default())?,
                identity.clone(),
                format!("bootstrap:{identity}"),
            )
        }
        Authority::Marker(_) => unreachable!(),
    };
    let mut connection = open_connection(state_path, true, true)?;
    configure_writer(&connection)?;
    let state: IngestState = serde_json::from_value(value.clone())?;
    failure.check("before_import")?;
    {
        let transaction = connection.transaction()?;
        transaction.execute_batch(SCHEMA)?;
        transaction.execute("DELETE FROM files", [])?;
        transaction.execute("DELETE FROM metadata", [])?;
        transaction.execute(
            "INSERT INTO metadata(singleton,format_version,store_id,origin,next_doc_id,opencode_databases,legacy_extras) VALUES(1,?1,?2,?3,?4,?5,?6)",
            params![FORMAT_VERSION, identity, origin, state.next_doc_id.to_string(), serde_json::to_string(value.get("opencode_databases").unwrap_or(&serde_json::json!({})))?, codec::legacy_extras(&value)?],
        )?;
        {
            let mut insert =
                transaction.prepare("INSERT INTO files(path,payload) VALUES(?1,?2)")?;
            for (path, payload) in value["files"].as_object().context("invalid legacy files")? {
                insert.execute(params![path, serde_json::to_string(payload)?])?;
            }
        }
        failure.check("before_import_commit")?;
        transaction.commit()?;
    }
    failure.check("after_import_commit")?;
    let mut writer = CheckpointWriter {
        reader: CheckpointReader {
            backend: Backend::Sqlite {
                connection,
                _lease: lease,
            },
        },
    };
    writer.checkpoint()?;
    failure.check("before_database_sync")?;
    File::open(location(state_path, DATABASE)?)?.sync_all()?;
    super::super::sync_directory(super::super::parent_directory(state_path)?)?;
    failure.check("after_database_sync")?;
    let marker = serde_json::to_vec(&format!("{MARKER_PREFIX}{FORMAT_VERSION}:{identity}"))?;
    failure.check("before_marker")?;
    super::super::atomic_write(state_path, &marker)?;
    failure.check("after_marker")?;
    drop(writer);
    Ok(CheckpointWriter {
        reader: open_active(state_path, &identity, true)?,
    })
}

fn new_identity() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("checkpoint store identity: {error}"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn backup(state_path: &Path, raw: &[u8], digest: &str) -> Result<()> {
    let path = location(state_path, &format!("ingest.legacy-{digest}.json"))?;
    if path.try_exists()? {
        ensure!(
            fs::read(&path)? == raw,
            "legacy checkpoint backup content mismatch"
        );
        File::open(&path)?.sync_all()?;
        return super::super::sync_directory(super::super::parent_directory(state_path)?);
    }
    let mut temporary =
        tempfile::NamedTempFile::new_in(super::super::parent_directory(state_path)?)?;
    temporary.write_all(raw)?;
    temporary.as_file().sync_all()?;
    if let Err(error) = temporary.persist_noclobber(&path) {
        if error.error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(error.error.into());
        }
        ensure!(
            fs::read(&path)? == raw,
            "legacy checkpoint backup content mismatch"
        );
        File::open(&path)?.sync_all()?;
    }
    super::super::sync_directory(super::super::parent_directory(state_path)?)
}

pub(super) fn checkpoint(connection: &Connection) -> Result<()> {
    crate::profiling::span!("state.checkpoint.maintenance");
    let (busy, log, checkpointed): (i64, i64, i64) =
        connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })?;
    ensure!(
        busy == 0 && log == 0 && checkpointed == 0,
        "checkpoint WAL truncate did not complete (busy={busy}, log={log}, checkpointed={checkpointed})"
    );
    Ok(())
}

pub(super) fn save_legacy(state: &IngestState, state_path: &Path) -> Result<()> {
    let _lease = lifecycle_lock(state_path, false, true)?;
    if let Ok(raw) = fs::read(state_path) {
        if let Ok(Value::String(_)) = serde_json::from_slice::<Value>(&raw) {
            bail!("cannot overwrite checkpoint marker with legacy JSON; use save_with_lease");
        }
        if location(state_path, DATABASE)?.try_exists()? {
            serde_json::from_slice::<IngestState>(&raw)
                .context("cannot overwrite invalid checkpoint authority with legacy JSON")?;
        }
    } else {
        ensure!(
            !location(state_path, DATABASE)?.try_exists()?,
            "cannot replace missing checkpoint authority with legacy JSON"
        );
    }
    super::super::atomic_write(state_path, serde_json::to_string_pretty(state)?.as_bytes())
}

pub(super) fn reset(state_path: &Path, _ingest_lease: &IngestLease) -> Result<()> {
    let _lease = lifecycle_lock(state_path, true, true)?;
    for path in [
        state_path.to_owned(),
        location(state_path, DATABASE)?,
        location(state_path, "checkpoints.sqlite-wal")?,
        location(state_path, "checkpoints.sqlite-shm")?,
        location(state_path, "checkpoints.sqlite-journal")?,
    ] {
        match fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    super::super::sync_directory(super::super::parent_directory(state_path)?)
}
