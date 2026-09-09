# Shared immutable index files

New publishing generations store metadata and flat references to immutable segment files. Unchanged segment payloads are not linked or copied into each new generation.

```text
index/
  CURRENT
  generations/<generation>/
    meta.json
    .managed.json
    .storage-format
    .segments.json
    .lease
  segments/
    .lock
    <creating-generation>/<segment-file>
```

`.segments.json` is versioned and maps logical Tantivy filenames to their creating generation. Owner IDs namespace files so independent staging branches cannot collide when they generate the same logical deletion filename. References never form chains.

## Publication

A writable directory reads inherited files from the shared store and writes new files locally. Deleting an inherited logical file does not delete its shared physical file. Published views reject writes.

After the index writer finishes, new committed files enter the shared store once. Their local links can remain in the creating generation until it is pruned. The file data and reference/format metadata are made durable before the generation pointer changes. Metadata already durably written by Tantivy is not synchronized a second time. The store guard serializes reference publication and reclamation, not parsing or index construction.

A failed publication leaves the previous `CURRENT` intact until the new reference set is ready. A crash after the pointer changes uses the existing ingestion recovery protocol.

## Readers and collection

The current generation, leased readers, and live staging generations retain their shared references. Normal pruning removes unleased generations before collecting unreachable shared files. Offline GC also sweeps orphaned files and reports `shared_files_removed`.

Malformed references, unsupported versions, traversal paths, missing referenced files, and symlinked shared data fail closed. Shared files are reclaimed only when no retained generation or staging manifest references them.

## Compatibility

Old flat and full-directory indexes remain readable. Their committed files are adopted once when migration is published; the first migration can be slower than a steady-state update. Explicit indexing may publish this format upgrade even when no records changed.

Older binaries cannot read the shared-reference layout. Upgrade every process using an index before migrating it. Do not roll back to an older binary against a migrated index; restore a pre-migration snapshot or rebuild using the older version.

See [shared-segment measurements](shared-segments-benchmark.md) for migration-excluded latency and paired trace/flamegraph results.
