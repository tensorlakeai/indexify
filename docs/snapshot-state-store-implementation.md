# Snapshot State Store Persistence Implementation

## Overview

This document describes the implementation of snapshot persistence to RocksDB, which ensures snapshots survive server restarts.

## Implementation Summary

### What Was Implemented

1. **Persistence Handlers** (`crates/server/src/state_store/mod.rs`)
   - Added handlers in `write_in_persistent_store()` method to persist snapshots to RocksDB
   - Handles both `CreateSnapshot` and `DeleteSnapshot` request payloads
   - Reuses existing `TerminateSandbox` handler for sandbox status updates

2. **Request Structure Updates** (`crates/server/src/state_store/requests.rs`)
   - Changed `CreateSnapshotRequest` to hold full `SandboxSnapshot` object (matching pattern used by `CreateSandboxRequest`)
   - Changed `TerminateSandboxRequest` to hold full `Sandbox` object
   - This ensures consistent clock values between in-memory and persistent stores

3. **Route Handler Updates**
   - Updated `crates/server/src/routes/sandbox_snapshots.rs` to pass full snapshot object
   - Updated `crates/server/src/routes/sandboxes.rs` to pass full sandbox object
   - Updated `crates/server/src/state_store/state_changes.rs` to access fields via object references

4. **Integration Test Updates** (`crates/server/src/integration_test_sandboxes.rs`)
   - Fixed three test cases to use new request structure
   - Tests now fetch sandbox objects before creating termination requests

## Code Changes

### Persistence Layer

**File: `crates/server/src/state_store/mod.rs` (lines ~576-590)**

```rust
RequestPayload::CreateSnapshot(request) => {
    state_machine::upsert_snapshot(&txn, &request.snapshot, current_clock).await?;
}
RequestPayload::DeleteSnapshot(request) => {
    state_machine::delete_snapshot(&txn, &request.namespace, request.snapshot_id.get()).await?;
}
RequestPayload::TerminateSandbox(request) => {
    state_machine::upsert_sandbox(&txn, &request.sandbox, current_clock).await?;
}
```

### Request Structures

**File: `crates/server/src/state_store/requests.rs`**

```rust
// Before:
pub struct CreateSnapshotRequest {
    pub snapshot_id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub namespace: String,
    pub ttl_secs: u64,
    pub snapshot_tag: String,
}

// After:
pub struct CreateSnapshotRequest {
    pub snapshot: SandboxSnapshot,
}

// Similarly for TerminateSandboxRequest:
pub struct TerminateSandboxRequest {
    pub sandbox: Sandbox,
}
```

### Route Handler Example

**File: `crates/server/src/routes/sandbox_snapshots.rs` (lines ~197-209)**

```rust
state
    .indexify_state
    .write(StateMachineUpdateRequest {
        payload: RequestPayload::CreateSnapshot(
            crate::state_store::requests::CreateSnapshotRequest {
                snapshot: snapshot.clone(),
            },
        ),
    })
    .await
    .map_err(|e| {
        IndexifyAPIError::internal_error(anyhow::anyhow!("failed to create snapshot: {}", e))
    })?;
```

## Verification

### Database Schema

Snapshots are stored in the RocksDB column family defined in `IndexifyObjectsColumns::Snapshots`.

**Key Format:** `{namespace}:{snapshot_id}`
**Value:** Serialized `SandboxSnapshot` object

### Testing Snapshot Persistence

To verify snapshots persist across server restarts:

```bash
# 1. Start server
cargo run --bin indexify-server

# 2. Create a sandbox
curl -X POST http://localhost:8900/v1/namespaces/default/sandboxes \
  -H "Content-Type: application/json" \
  -d '{"image": "python:3.11-slim"}'
# Note the sandbox_id from response

# 3. Wait for sandbox to reach Running status, then create snapshot
curl -X POST http://localhost:8900/v1/namespaces/default/sandboxes/{sandbox_id}/snapshots \
  -H "Content-Type: application/json" \
  -d '{"ttl_secs": 3600}'
# Note the snapshot_id from response

# 4. Verify snapshot exists
curl http://localhost:8900/v1/namespaces/default/snapshots/{snapshot_id}

# 5. Stop server (Ctrl+C)

# 6. Restart server
cargo run --bin indexify-server

# 7. Verify snapshot still exists after restart
curl http://localhost:8900/v1/namespaces/default/snapshots/{snapshot_id}

# Should return the same snapshot with status "active"
```

### RocksDB Inspection

You can inspect the database directly:

```rust
// Read all snapshots from DB
let db = &self.db;
let cf = db.cf_handle(Snapshots::COLUMN_FAMILY_NAME)?;
let iter = db.iterator_cf(cf, IteratorMode::Start);

for item in iter {
    let (key, value) = item?;
    let key_str = std::str::from_utf8(&key)?;
    let snapshot: SandboxSnapshot = serde_json::from_slice(&value)?;
    println!("Key: {}, Snapshot: {:?}", key_str, snapshot);
}
```

## Already Existing Components

These components were already implemented and did not require changes:

1. **Column Family Definition** - `IndexifyObjectsColumns::Snapshots` enum variant
2. **Write Functions** - `state_machine::upsert_snapshot()` and `delete_snapshot()`
3. **Read Functions** - `scanner::get_snapshot()` and `list_snapshots()`
4. **Serialization** - `SandboxSnapshot` derives Serialize/Deserialize

## State Flow

### Create Snapshot Flow

1. API receives POST request → `create_snapshot()` handler
2. Handler builds `SandboxSnapshot` object with `Creating` status
3. Handler creates `StateMachineUpdateRequest` with `CreateSnapshot` payload
4. State store writes to RocksDB via `upsert_snapshot()`
5. State store updates in-memory state
6. Processor detects `Creating` snapshot, sends request to executor
7. Executor creates snapshot via Docker commit, reports success
8. Processor updates snapshot status to `Active`
9. Updated status persisted to RocksDB

### Delete Snapshot Flow

1. API receives DELETE request → `delete_snapshot()` handler
2. Handler creates `StateMachineUpdateRequest` with `DeleteSnapshot` payload
3. State store deletes from RocksDB via `delete_snapshot()`
4. State store removes from in-memory state
5. Processor sends delete request to executor
6. Executor removes Docker image

### Restore from Snapshot Flow

1. API receives POST request → `restore_snapshot()` handler
2. Handler reads snapshot from state store (may come from RocksDB if not in memory)
3. Handler checks TTL expiration
4. If not expired: creates new sandbox with snapshot image
5. If expired: creates fresh sandbox with original config
6. New sandbox creation follows normal sandbox lifecycle

## Clock Consistency

The implementation ensures clock consistency between persistent and in-memory state:

1. `StateMachineUpdateRequest::prepare_for_persistence()` is called before write
2. All clocks set to same value in both stores
3. Request structures hold full objects (not individual fields) to maintain clock integrity

## Error Handling

- **Snapshot not found**: Returns 404 error
- **Database write failure**: Returns 500 error with descriptive message
- **Concurrent operations**: Serialized via state machine transaction
- **Invalid snapshot state**: Returns 409 Conflict error

## Monitoring

Relevant metrics to track:

- `snapshot_create_operations_total{result="success|failure"}`
- `snapshot_delete_operations_total{result="success|failure"}`
- `snapshots_total{namespace, status}` - gauge of active snapshots
- `snapshot_db_write_duration_seconds` - persistence latency

## Future Enhancements

1. **Snapshot Cleanup on Startup** - Check for orphaned Creating snapshots and retry/cleanup
2. **TTL Background Job** - Automatically delete expired snapshots from database
3. **Database Compaction** - Periodic compaction to reclaim space from deleted snapshots
4. **Backup/Restore** - Export/import snapshots for disaster recovery
5. **Metrics Dashboard** - Grafana dashboard for snapshot operations

## Related Files

- `crates/server/src/state_store/mod.rs` - Main state store with persistence handlers
- `crates/server/src/state_store/state_machine.rs` - Database write functions
- `crates/server/src/state_store/scanner.rs` - Database read functions
- `crates/server/src/state_store/requests.rs` - Request payload types
- `crates/server/src/routes/sandbox_snapshots.rs` - HTTP API handlers
- `crates/server/src/data_model/mod.rs` - Snapshot data structures
- `crates/server/src/processor/snapshot_processor.rs` - Snapshot lifecycle management
