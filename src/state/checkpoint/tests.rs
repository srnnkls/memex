use super::*;
use crate::config::Paths;
use crate::lease::LeaseAttempt;
use crate::state::{FileIdentity, PendingToolCall};
use std::time::Duration;

fn fixture() -> (tempfile::TempDir, PathBuf, IngestLease) {
    let temp = tempfile::tempdir().unwrap();
    let paths = Paths::new(Some(temp.path().join("root"))).unwrap();
    let lease = match IngestLease::try_acquire(&paths, "checkpoint test").unwrap() {
        LeaseAttempt::Acquired(lease) => lease,
        LeaseAttempt::Busy(_) => panic!("unexpected lease contention"),
    };
    (temp, paths.state.join("ingest.json"), lease)
}

fn file(mtime: i64) -> FileState {
    FileState {
        size: u64::MAX,
        mtime,
        offset: u64::MAX,
        turn_id: u32::MAX,
        legacy_turn_id: Some(u32::MAX),
        claude_background: None,
        parser_version: u32::MAX,
        pending_tool_calls: HashMap::from([(
            "call".into(),
            PendingToolCall {
                tool_use_doc_id: Some(u64::MAX),
                timestamp: u64::MAX,
                argument_bytes: Some(u64::MAX),
                ..Default::default()
            },
        )]),
        identity: FileIdentity {
            device: Some(u64::MAX),
            inode: Some(u64::MAX),
            prefix_bytes: u64::MAX,
            modified_ns: Some(i64::MIN),
            changed_ns: Some(i64::MAX),
            ..Default::default()
        },
        codex_metadata_offsets: Some(vec![0, u64::MAX]),
    }
}

fn database() -> OpencodeDatabaseState {
    OpencodeDatabaseState {
        parser_version: u32::MAX,
        event_rowid: i64::MIN,
        event_id: Some("event".into()),
        owned_session_ids: HashSet::from(["session".into()]),
    }
}

fn extended_legacy(path: &Path) -> Value {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut value = serde_json::to_value(IngestState {
        next_doc_id: u64::MAX,
        files: HashMap::from([("./odd/../a\n\0é.jsonl".into(), file(i64::MIN))]),
        opencode_databases: HashMap::from([("database".into(), database())]),
    })
    .unwrap();
    value["extension"] = serde_json::json!({"unsigned": u64::MAX});
    let row = &mut value["files"]["./odd/../a\n\0é.jsonl"];
    row["extension"] = serde_json::json!(["opaque", u64::MAX]);
    row["identity"]["extension"] = serde_json::json!({"id": u64::MAX});
    row["pending_tool_calls"]["call"]["extension"] = serde_json::json!({"future": true});
    value["opencode_databases"]["database"]["extension"] = serde_json::json!("owner");
    fs::write(path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    value
}

fn connection(writer: &CheckpointWriter) -> &Connection {
    match &writer.reader.backend {
        Backend::Sqlite { connection, .. } => connection,
        Backend::Legacy(_) => panic!("not SQLite"),
    }
}

#[test]
fn migration_preserves_exact_unsigned_values_extensions_paths_and_raw_backup() {
    let (_temp, path, lease) = fixture();
    let original = extended_legacy(&path);
    let raw = fs::read(&path).unwrap();
    let mut writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    assert_eq!(writer.reader().export_json().unwrap(), original);
    assert_eq!(writer.reader().header().unwrap().next_doc_id, u64::MAX);
    assert_eq!(
        writer
            .reader()
            .snapshot()
            .unwrap()
            .files
            .values()
            .next()
            .unwrap()
            .offset,
        u64::MAX
    );
    assert!(serde_json::from_slice::<IngestState>(&fs::read(&path).unwrap()).is_err());
    let backups: Vec<_> = fs::read_dir(path.parent().unwrap())
        .unwrap()
        .flatten()
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with("ingest.legacy-")
        })
        .collect();
    assert_eq!(backups.len(), 1);
    assert_eq!(fs::read(backups[0].path()).unwrap(), raw);
    writer.checkpoint().unwrap();
}

#[test]
fn delta_updates_preserve_private_nested_extensions_and_remove_deleted_known_fields() {
    let (_temp, path, lease) = fixture();
    let original = extended_legacy(&path);
    let mut writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    let mut state = writer.reader().snapshot().unwrap();
    let row = state.files.values_mut().next().unwrap();
    row.offset = 7;
    row.identity.inode = None;
    row.pending_tool_calls
        .get_mut("call")
        .unwrap()
        .tool_use_doc_id = None;
    row.codex_metadata_offsets = None;
    state
        .opencode_databases
        .get_mut("database")
        .unwrap()
        .event_rowid = 19;
    writer.replace_snapshot(&state).unwrap();
    let exported = writer.reader().export_json().unwrap();
    assert_eq!(exported["extension"], original["extension"]);
    let key = "./odd/../a\n\0é.jsonl";
    for nested in [
        "/extension",
        "/identity/extension",
        "/pending_tool_calls/call/extension",
    ] {
        assert_eq!(
            exported["files"][key].pointer(nested),
            original["files"][key].pointer(nested)
        );
    }
    assert!(exported["files"][key]["identity"].get("inode").is_none());
    assert!(
        exported["files"][key]["pending_tool_calls"]["call"]
            .get("tool_use_doc_id")
            .is_none()
    );
    assert!(
        exported["files"][key]
            .get("codex_metadata_offsets")
            .is_none()
    );
    assert_eq!(
        exported["opencode_databases"]["database"]["extension"],
        "owner"
    );
}

#[test]
fn legacy_defaults_remain_readable_after_migration() {
    let (_temp, path, lease) = fixture();
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        r#"{"next_doc_id":9,"files":{"legacy":{"size":4,"mtime":-1,"offset":3,"turn_id":2}}}"#,
    )
    .unwrap();
    let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    let state = writer.reader().snapshot().unwrap();
    assert_eq!(state.next_doc_id, 9);
    assert_eq!(state.files["legacy"].identity, FileIdentity::default());
    assert!(state.files["legacy"].pending_tool_calls.is_empty());
    assert!(state.opencode_databases.is_empty());
}

#[test]
fn every_writer_verifies_durable_sqlite_pragmas() {
    let (_temp, path, lease) = fixture();
    for _ in 0..2 {
        let writer = CheckpointWriter::open(&path, &lease, true).unwrap();
        let connection = connection(&writer);
        assert_eq!(
            connection
                .pragma_query_value(None, "journal_mode", |row| row.get::<_, String>(0))
                .unwrap(),
            "wal"
        );
        for (name, expected) in [
            ("synchronous", 2),
            ("fullfsync", 1),
            ("checkpoint_fullfsync", 1),
            ("wal_autocheckpoint", 1000),
            ("journal_size_limit", 16 * 1024 * 1024),
        ] {
            assert_eq!(
                connection
                    .pragma_query_value(None, name, |row| row.get::<_, i64>(0))
                    .unwrap(),
                expected
            );
        }
        assert!(
            connection
                .db_config(rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE)
                .unwrap()
        );
    }
}

#[test]
fn schema_validates_generated_mtime_and_uses_its_index() {
    let (_temp, path, lease) = fixture();
    let writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    let connection = connection(&writer);
    for payload in [
        r#"{"mtime":"5"}"#,
        r#"{"mtime":1.5}"#,
        r#"{"mtime":18446744073709551615}"#,
        r#"{}"#,
    ] {
        assert!(
            connection
                .execute(
                    "INSERT INTO files(path,payload) VALUES('bad',?1)",
                    [payload]
                )
                .is_err()
        );
    }
    let plan: String = connection
        .query_row(
            "EXPLAIN QUERY PLAN SELECT path,payload FROM files WHERE mtime>=?1",
            [1],
            |row| row.get(3),
        )
        .unwrap();
    assert!(plan.contains("files_mtime"), "{plan}");
    let path_type: String = connection
        .query_row(
            "SELECT type FROM pragma_table_info('files') WHERE name='path'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(path_type, "TEXT");
}

#[test]
fn sparse_reads_and_delta_leave_unrelated_payloads_identical() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            upserts: (0..2000)
                .map(|id| (format!("file-{id}"), file(id)))
                .collect(),
            next_doc_id: Some(u64::MAX),
            ..Default::default()
        })
        .unwrap();
    let before: String = connection(&writer)
        .query_row(
            "SELECT payload FROM files WHERE path='file-1000'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let loaded = writer
        .reader()
        .load_files(&["file-1".into(), "absent".into()])
        .unwrap();
    assert_eq!(loaded.len(), 2);
    assert!(loaded["file-1"].is_some());
    assert_eq!(loaded["absent"], None);
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("file-1".into(), file(-8))]),
            deletes: HashSet::from(["file-2".into()]),
            ..Default::default()
        })
        .unwrap();
    let after: String = connection(&writer)
        .query_row(
            "SELECT payload FROM files WHERE path='file-1000'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(before, after);
    assert!(!writer.reader().contains_file("file-2").unwrap());
    assert_eq!(writer.reader().file_keys().unwrap().len(), 1999);
    assert_eq!(writer.reader().header().unwrap().next_doc_id, u64::MAX);
}

#[test]
fn empty_delta_starts_no_transaction_and_changes_no_pages() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    connection(&writer).execute_batch("BEGIN").unwrap();
    assert!(!writer.commit_delta(&CheckpointDelta::default()).unwrap());
    assert!(!connection(&writer).is_autocommit());
    connection(&writer).execute_batch("ROLLBACK").unwrap();
    let reader = CheckpointReader::open(&path).unwrap();
    let Backend::Sqlite {
        connection: observer,
        ..
    } = &reader.backend
    else {
        panic!()
    };
    let before: i64 = observer
        .pragma_query_value(None, "data_version", |row| row.get(0))
        .unwrap();
    assert!(!writer.commit_delta(&CheckpointDelta::default()).unwrap());
    assert_eq!(
        before,
        observer
            .pragma_query_value(None, "data_version", |row| row.get::<_, i64>(0))
            .unwrap()
    );
}

#[test]
fn hot_and_key_queries_do_not_decode_cold_payloads() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("hot".into(), file(50))]),
            ..Default::default()
        })
        .unwrap();
    connection(&writer)
        .execute(
            "INSERT INTO files(path,payload) VALUES('cold',?1)",
            [r#"{"mtime":-50,"size":"invalid FileState"}"#],
        )
        .unwrap();
    assert_eq!(writer.reader().hot_files_since(0).unwrap().len(), 1);
    assert_eq!(writer.reader().file_keys().unwrap().len(), 2);
    assert!(writer.reader().contains_file("cold").unwrap());
    assert!(
        writer
            .reader()
            .has_files_excluding(&HashSet::from(["hot".into()]))
            .unwrap()
    );
    assert!(
        !writer
            .reader()
            .has_files_excluding(&HashSet::from(["hot".into(), "cold".into()]))
            .unwrap()
    );
    assert!(writer.reader().snapshot().is_err());
}

#[test]
fn live_reader_observes_wal_commits_without_database_mtime_changes() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    let reader = CheckpointReader::open(&path).unwrap();
    let database_path = path.with_file_name(DATABASE);
    let modified = fs::metadata(&database_path).unwrap().modified().unwrap();
    assert!(reader.hot_files_since(0).unwrap().is_empty());
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("new".into(), file(1))]),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        fs::metadata(&database_path).unwrap().modified().unwrap(),
        modified
    );
    assert_eq!(reader.hot_files_since(0).unwrap()["new"].mtime, 1);
    assert!(reader.contains_file("new").unwrap());
}

#[test]
fn truncate_reports_pinned_readers_and_completes_after_release() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("first".into(), file(1))]),
            ..Default::default()
        })
        .unwrap();
    let reader = CheckpointReader::open(&path).unwrap();
    let Backend::Sqlite {
        connection: pinned, ..
    } = &reader.backend
    else {
        panic!()
    };
    pinned.execute_batch("BEGIN; SELECT * FROM files;").unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            next_doc_id: Some(3),
            ..Default::default()
        })
        .unwrap();
    assert!(writer.checkpoint().is_err());
    pinned.execute_batch("ROLLBACK").unwrap();
    writer.checkpoint().unwrap();
    assert_eq!(
        fs::metadata(path.with_file_name("checkpoints.sqlite-wal"))
            .unwrap()
            .len(),
        0
    );
}

#[test]
fn migration_failure_boundaries_preserve_legacy_or_activated_authority() {
    for point in [
        "before_backup",
        "after_backup",
        "before_import",
        "before_import_commit",
        "after_import_commit",
        "before_database_sync",
        "after_database_sync",
        "before_marker",
        "after_marker",
    ] {
        let (_temp, path, lease) = fixture();
        let original = extended_legacy(&path);
        let raw = fs::read(&path).unwrap();
        assert!(
            lifecycle::open_writer(&path, &lease, false, lifecycle::MigrationFailure::At(point))
                .is_err(),
            "{point}"
        );
        if point != "after_marker" {
            assert_eq!(fs::read(&path).unwrap(), raw, "{point}");
        }
        assert_eq!(
            CheckpointReader::open(&path)
                .unwrap()
                .export_json()
                .unwrap(),
            original,
            "{point}"
        );
        let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
        assert_eq!(writer.reader().export_json().unwrap(), original, "{point}");
    }
}

#[test]
fn legacy_changes_after_partial_import_are_reimported_not_replaced_by_backup() {
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    assert!(
        lifecycle::open_writer(
            &path,
            &lease,
            false,
            lifecycle::MigrationFailure::At("after_import_commit")
        )
        .is_err()
    );
    let replacement =
        serde_json::json!({"next_doc_id":72,"files":{},"opencode_databases":{},"future":"new"});
    fs::write(&path, serde_json::to_vec(&replacement).unwrap()).unwrap();
    let writer = CheckpointWriter::open(&path, &lease, false).unwrap();
    assert_eq!(writer.reader().export_json().unwrap(), replacement);
    assert_eq!(
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .filter(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("ingest.legacy-"))
            .count(),
        2
    );
}

#[test]
fn backup_collision_is_an_error_and_never_overwrites_bytes() {
    use sha2::{Digest, Sha256};
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    let raw = fs::read(&path).unwrap();
    let backup = path.with_file_name(format!("ingest.legacy-{:x}.json", Sha256::digest(&raw)));
    fs::write(&backup, "wrong bytes").unwrap();
    assert!(CheckpointWriter::open(&path, &lease, false).is_err());
    assert_eq!(fs::read(&path).unwrap(), raw);
    assert_eq!(fs::read(&backup).unwrap(), b"wrong bytes");
}

#[test]
fn read_only_missing_state_creates_nothing_and_initialization_requires_permission() {
    let (_temp, path, lease) = fixture();
    assert!(!path.parent().unwrap().exists());
    assert!(!has_authority(&path).unwrap());
    assert_eq!(
        CheckpointReader::open(&path)
            .unwrap()
            .snapshot()
            .unwrap()
            .next_doc_id,
        1
    );
    assert!(!path.parent().unwrap().exists());
    assert!(CheckpointWriter::open(&path, &lease, false).is_err());
    assert!(!path.with_file_name(DATABASE).exists());
    assert!(CheckpointWriter::open(&path, &lease, true).is_ok());
}

#[test]
fn empty_bootstrap_retry_requires_matching_receipt_and_initialization_permission() {
    let (_temp, path, lease) = fixture();
    assert!(
        lifecycle::open_writer(
            &path,
            &lease,
            true,
            lifecycle::MigrationFailure::At("after_import_commit")
        )
        .is_err()
    );
    assert!(!has_authority(&path).unwrap());
    assert!(CheckpointWriter::open(&path, &lease, false).is_err());
    let receipt = fs::read(path.with_file_name(LOCK)).unwrap();
    fs::write(
        path.with_file_name(LOCK),
        format!("bootstrap:{}", "0".repeat(64)),
    )
    .unwrap();
    assert!(CheckpointReader::open(&path).is_err());
    fs::write(path.with_file_name(LOCK), receipt).unwrap();
    assert!(CheckpointWriter::open(&path, &lease, true).is_ok());
}

#[test]
fn missing_authority_rejects_populated_database_even_with_archived_backup() {
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    drop(CheckpointWriter::open(&path, &lease, false).unwrap());
    fs::remove_file(&path).unwrap();
    assert!(has_authority(&path).is_err());
    assert!(IngestState::load(&path).is_err());
    assert!(CheckpointWriter::open(&path, &lease, true).is_err());
}

#[test]
fn malformed_marker_missing_database_missing_lock_and_unknown_versions_fail_closed() {
    for broken in [
        "marker",
        "database",
        "lock",
        "version",
        "identity",
        "schema_version",
        "next_id",
    ] {
        let (_temp, path, lease) = fixture();
        drop(CheckpointWriter::open(&path, &lease, true).unwrap());
        match broken {
            "marker" => fs::write(&path, "\"memex-checkpoints:broken\"").unwrap(),
            "database" => fs::remove_file(path.with_file_name(DATABASE)).unwrap(),
            "lock" => fs::remove_file(path.with_file_name(LOCK)).unwrap(),
            "version" => {
                let raw = fs::read_to_string(&path)
                    .unwrap()
                    .replace("checkpoints:1:", "checkpoints:99:");
                fs::write(&path, raw).unwrap();
            }
            "identity" => fs::write(
                &path,
                serde_json::to_vec(&format!("{MARKER_PREFIX}1:{}", "0".repeat(64))).unwrap(),
            )
            .unwrap(),
            "schema_version" | "next_id" => {
                let connection = Connection::open(path.with_file_name(DATABASE)).unwrap();
                connection
                    .execute_batch(if broken == "schema_version" {
                        "UPDATE metadata SET format_version=99"
                    } else {
                        "UPDATE metadata SET next_doc_id='01'"
                    })
                    .unwrap();
            }
            _ => unreachable!(),
        }
        assert!(CheckpointReader::open(&path).is_err(), "{broken}");
        assert!(has_authority(&path).is_err(), "{broken}");
        assert!(
            CheckpointWriter::open(&path, &lease, true).is_err(),
            "{broken}"
        );
    }
}

#[test]
fn legacy_save_cannot_overwrite_marker_and_explicit_snapshot_keeps_extensions() {
    let (_temp, path, lease) = fixture();
    let original = extended_legacy(&path);
    drop(CheckpointWriter::open(&path, &lease, false).unwrap());
    let marker = fs::read(&path).unwrap();
    let mut state = IngestState::load(&path).unwrap();
    state.next_doc_id = 17;
    assert!(state.save(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), marker);
    state.save_with_lease(&path, &lease).unwrap();
    let exported = CheckpointReader::open(&path)
        .unwrap()
        .export_json()
        .unwrap();
    assert_eq!(exported["next_doc_id"], 17);
    assert_eq!(exported["extension"], original["extension"]);
}

#[test]
fn reset_waits_for_readers_preserves_lock_inode_and_archives() {
    #[cfg(unix)]
    use std::os::unix::fs::MetadataExt;
    let (_temp, path, lease) = fixture();
    extended_legacy(&path);
    drop(CheckpointWriter::open(&path, &lease, false).unwrap());
    let reader = CheckpointReader::open(&path).unwrap();
    #[cfg(unix)]
    let inode = fs::metadata(path.with_file_name(LOCK)).unwrap().ino();
    assert!(reset(&path, &lease).is_err());
    assert!(path.exists());
    drop(reader);
    reset(&path, &lease).unwrap();
    assert!(!path.exists());
    assert!(!path.with_file_name(DATABASE).exists());
    assert!(path.with_file_name(LOCK).exists());
    #[cfg(unix)]
    assert_eq!(
        fs::metadata(path.with_file_name(LOCK)).unwrap().ino(),
        inode
    );
    assert!(
        fs::read_dir(path.parent().unwrap())
            .unwrap()
            .flatten()
            .any(|entry| entry
                .file_name()
                .to_string_lossy()
                .starts_with("ingest.legacy-"))
    );
    assert!(!has_authority(&path).unwrap());
    drop(CheckpointWriter::open(&path, &lease, true).unwrap());
}

#[test]
fn reset_serializes_with_concurrent_reader_lifetime() {
    let (_temp, path, lease) = fixture();
    drop(CheckpointWriter::open(&path, &lease, true).unwrap());
    let reader = CheckpointReader::open(&path).unwrap();
    let held = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(70));
        assert_eq!(reader.header().unwrap().next_doc_id, 1);
    });
    reset(&path, &lease).unwrap();
    held.join().unwrap();
    assert!(!path.exists());
}

#[test]
fn clear_all_preserves_allocator_and_small_database_map_unless_explicitly_changed() {
    let (_temp, path, lease) = fixture();
    let mut writer = CheckpointWriter::open(&path, &lease, true).unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            upserts: HashMap::from([("old".into(), file(0))]),
            next_doc_id: Some(900),
            opencode_databases: Some(HashMap::from([("database".into(), database())])),
            ..Default::default()
        })
        .unwrap();
    writer
        .commit_delta(&CheckpointDelta {
            clear_files: true,
            upserts: HashMap::from([("new".into(), file(1))]),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(writer.reader().file_keys().unwrap(), ["new"]);
    let header = writer.reader().header().unwrap();
    assert_eq!(header.next_doc_id, 900);
    assert_eq!(header.opencode_databases["database"], database());
}

#[test]
fn checkpoint_artifact_names_are_canonical_and_bounded() {
    for name in [
        "checkpoints.sqlite",
        "checkpoints.sqlite-wal",
        "checkpoints.sqlite-shm",
        "checkpoints.sqlite-journal",
        ".checkpoints.lock",
        "ingest.json",
    ] {
        assert!(is_checkpoint_artifact_name(OsStr::new(name)));
    }
    assert!(is_checkpoint_artifact_name(OsStr::new(&format!(
        "ingest.legacy-{}.json",
        "a".repeat(64)
    ))));
    for name in [
        "checkpoints.sqlite-session.jsonl",
        "ingest.legacy-x.json",
        "my-checkpoints.sqlite",
        "session.jsonl",
    ] {
        assert!(!is_checkpoint_artifact_name(OsStr::new(name)));
    }
}
