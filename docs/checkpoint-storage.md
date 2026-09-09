# Checkpoint storage

Ingest checkpoints are authoritative. They bind source offsets, parser state, OpenCode ownership, and the document-ID allocator to published records. Analytics remains independently rebuildable; `ingest.pending.json` and `scan_cache.json` retain their existing formats and publication order.

## Schema and access

`state/checkpoints.sqlite` has two tables:

| Table | Columns |
| --- | --- |
| `metadata` | Singleton key `singleton=1`; `format_version=1`; opaque `store_id`; migration/bootstrap `origin`; canonical unsigned decimal TEXT `next_doc_id`; complete small JSON object TEXT `opencode_databases`; JSON object TEXT `legacy_extras`. |
| `files` | Exact, unnormalized TEXT primary key `path`; complete JSON TEXT `payload`; validated, generated, stored signed INTEGER `mtime`, indexed by `files_mtime`. |

Unsigned values inside payloads remain JSON integers, including `u64::MAX`. Only `mtime` is extracted into SQLite INTEGER. Private flattened serde codecs preserve unknown top-level, file, identity, pending-tool-call, and OpenCode database fields. Updates replace current known fields without restoring removed optional fields; extensions remain attached to surviving entities. Deleting an entity deletes its extensions.

`CheckpointReader` opens read-only and never creates the database, authority marker, or lifecycle lock. It exposes owned headers, requested file rows with explicit absent results, key-only queries, indexed recent rows, and administrative full snapshots/JSON exports. Statements and read transactions end before callers perform filesystem probes. A sparse mutation writes only explicit upserts/deletes and changed singleton fields. An empty delta opens no checkpoint transaction.

Every writer configures and reads back WAL mode, `synchronous=FULL`, `fullfsync=ON`, `checkpoint_fullfsync=ON`, `wal_autocheckpoint=1000`, and a 16 MiB `journal_size_limit`. The safe SQLite `NO_CKPT_ON_CLOSE` setting disables implicit close-time checkpoints. These thresholds do not impose a hard WAL-size bound while readers pin frames. Explicit maintenance runs `wal_checkpoint(TRUNCATE)` and requires zero busy, log, and remaining checkpoint counts; an incomplete truncate is an error.

## Authority and migration

`state/ingest.json` is either a legacy state object or the JSON string:

```json
"memex-checkpoints:1:<64-lowercase-hex-store-id>"
```

The marker's identity must equal `metadata.store_id`. Its string shape causes old struct deserializers to reject it rather than treating a migrated root as empty.

Migration under the caller's `IngestLease` proceeds as follows:

1. Acquire the lifecycle lock exclusively. Legacy JSON remains authoritative even when an incomplete database already exists.
2. Validate the legacy state and preserve its exact bytes in `ingest.legacy-<sha256>.json`. Publish the backup atomically without clobbering; an existing backup must contain identical bytes.
3. Transactionally import or reimport the complete state into the fixed database path. Never replace SQLite files or sidecars to activate a migration.
4. Complete a checked WAL truncate, synchronize the database and containing directory, then atomically write and synchronize the marker.
5. Close the migration connection and release the exclusive lifecycle lease before opening the normal shared-leased connection.

Before marker publication, retries import the current legacy JSON, not an archived backup. After publication, SQLite is the only authority. Invalid marker JSON, unsupported versions, mismatched identities, corrupt metadata, and marker-with-missing-database/lock fail closed. Archived backups never independently authorize recovery or rollback.

Missing authority permits initialization at document ID 1 only when the caller has validated an empty index or pending-intent coverage of all indexed records and their document IDs. Existing vector-recovery requirements still apply. A database left by interrupted empty initialization is retryable only with the matching durable `bootstrap:<store-id>` receipt in the lifecycle lock and empty bootstrap metadata/files. A populated or unidentified database without authority requires a consistent restore or explicit rebuild.

## Lifecycle and compatibility

The persistent `state/.checkpoints.lock` protects database-handle lifetime. Connections hold shared leases. Migration, initialization, and reset take exclusive leases after the ingest lease; shared leases are never upgraded. Lock and SQLite contention produce bounded errors. Reset retains the lock inode and archived legacy backups while removing the active database, sidecars, and marker. Live readers must close before reset can finish.

`IngestState::load` is a read-only full-snapshot compatibility adapter. `save` writes legacy JSON only and refuses marker replacement. `save_with_lease` explicitly updates a full SQLite snapshot using the caller's existing ingest lease; it does not reacquire that lease. Ordinary ingestion uses sparse checkpoint deltas instead.

Checkpoint publication remains after lexical/vector publication and before pending-intent finalization. A failed checkpoint leaves pending recovery available. Rollback requires a consistent root snapshot or rebuild; replacing the marker with a stale legacy backup is not a safe rollback.

The canonical generated-artifact filter is `state::checkpoint::is_checkpoint_artifact_name`. Watch consumers must query current checkpoint rows rather than infer commits from database, WAL, or marker modification times.

## Diagnostics

Profiling builds record migration and maintenance spans as `state.checkpoint.migrate` and `state.checkpoint.maintenance`. Sparse reads use `state.checkpoint.load_files`. Fixed-name counters record `state.checkpoint.rows_decoded`, `state.checkpoint.key_scans`, `state.checkpoint.rows_upserted`, `state.checkpoint.rows_deleted`, `state.checkpoint.clear_files`, and `state.checkpoint.transactions`; instrumentation compiles out without the profiling feature.

Administrative correctness checks use `CheckpointReader::export_json()` for complete logical state, including preserved extensions, and `CheckpointWriter::checkpoint()` for charged terminal maintenance. A legacy backup preserves source bytes, while an export represents the current logical checkpoint.
