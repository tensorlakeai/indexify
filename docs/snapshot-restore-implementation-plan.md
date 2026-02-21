# Sandbox Snapshot/Restore Feature Implementation Plan

## Context

We need to add snapshot and restore capabilities to Indexify sandboxes to enable efficient state preservation and fast recovery. This feature will allow users to:

1. **Create snapshots** of running sandboxes to capture their filesystem state
2. **Restore from snapshots** with TTL-based conditional logic (use snapshot if within TTL, otherwise create fresh)
3. **Manage snapshot lifecycle** with automatic TTL-based expiration and cleanup

**Why this is needed:** Reduces cold start times for sandboxes with pre-installed dependencies, enables checkpoint/restart workflows, and improves developer iteration speed.

**Current state:** Sandboxes use Docker + gVisor for isolation, with warm pool support for pre-warmed containers. No snapshotting capability exists.

## Technology Decision: Docker Commit

**Recommended Approach:** Use `docker commit` (not Docker checkpoint/restore via CRIU)

**Rationale:**
- **gVisor Compatibility**: The codebase uses gVisor (`runsc` runtime in `crates/dataplane/src/driver/docker.rs:42-43`) which does NOT support CRIU checkpoint/restore
- **Simpler Implementation**: Docker commit creates image layers from container filesystem via Bollard API
- **Registry Integration**: Snapshots become standard Docker images, compatible with existing infrastructure

**Tradeoff:** Captures filesystem only (not process memory), but acceptable for stateless function environments and faster than cold starts.

## Implementation Phases

### Phase 1: Core Snapshot Functionality (MVP)

**Goal:** Create and restore snapshots on the same executor with TTL-based conditional restore.

#### 1.1 Data Model (`crates/server/src/data_model/mod.rs`)

Add snapshot entities:

```rust
pub struct SandboxSnapshot {
    pub id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub namespace: String,
    pub image_ref: String,  // "indexify-snapshots:sb-{sandbox_id}-snap-{snapshot_id}"
    pub created_at: u128,   // epoch nanoseconds
    pub ttl_secs: u64,      // 0 = no expiration
    pub status: SnapshotStatus,
    pub sandbox_config: SandboxSnapshotConfig,  // Original sandbox config
    pub size_bytes: Option<u64>,
}

pub enum SnapshotStatus {
    Creating,  // docker commit in progress
    Active,    // ready for restore
    Deleting,  // cleanup in progress
}

pub struct SandboxSnapshotConfig {
    pub image: String,
    pub resources: ContainerResources,
    pub secret_names: Vec<String>,
    pub timeout_secs: u64,
    pub entrypoint: Option<Vec<String>>,
    pub network_policy: Option<NetworkPolicy>,
}
```

#### 1.2 State Store (`crates/server/src/state_store/mod.rs`)

Add RocksDB column family:

```rust
pub const SNAPSHOTS_CF: &str = "snapshots";
```

Add snapshot CRUD methods to `IndexifyState`:
- `create_snapshot()`, `get_snapshot()`, `list_snapshots()`, `delete_snapshot()`

#### 1.3 Docker Driver Extensions (`crates/dataplane/src/driver/docker.rs`)

Add snapshot operations to `DockerDriver`:

```rust
impl DockerDriver {
    /// Create snapshot using docker commit
    pub async fn create_snapshot(
        &self,
        handle: &ProcessHandle,
        snapshot_tag: &str,
    ) -> Result<SnapshotMetadata> {
        use bollard::query_parameters::ContainerCommitOptions;

        let repo = "indexify-snapshots";
        let options = ContainerCommitOptions {
            container: &handle.id,
            repo: Some(repo.to_string()),
            tag: Some(snapshot_tag.to_string()),
            pause: Some(true),  // Pause during commit for consistency
            // ...
        };

        let commit_result = self.docker
            .commit_container(options, Default::default())
            .await?;

        let image_ref = format!("{}:{}", repo, snapshot_tag);

        // Inspect to get size
        let image_inspect = self.docker.inspect_image(&image_ref).await?;

        Ok(SnapshotMetadata {
            image_ref,
            size_bytes: image_inspect.size,
        })
    }

    /// Delete snapshot image
    pub async fn delete_snapshot(&self, image_ref: &str) -> Result<()> {
        use bollard::query_parameters::RemoveImageOptions;

        self.docker
            .remove_image(image_ref, Some(RemoveImageOptions {
                force: true,
                noprune: false,
            }), None)
            .await?;

        Ok(())
    }
}
```

Extend `ProcessDriver` trait:

```rust
async fn create_snapshot(&self, handle: &ProcessHandle, snapshot_tag: &str)
    -> Result<SnapshotMetadata>;
async fn delete_snapshot(&self, image_ref: &str) -> Result<()>;
```

#### 1.4 API Endpoints (`crates/server/src/routes/sandbox_snapshots.rs` - NEW FILE)

Implement REST API following patterns from `routes/sandboxes.rs`:

**Endpoints:**
- `POST /v1/namespaces/{namespace}/sandboxes/{sandbox_id}/snapshots` - Create snapshot
- `GET /v1/namespaces/{namespace}/sandboxes/{sandbox_id}/snapshots` - List snapshots for sandbox
- `GET /v1/namespaces/{namespace}/snapshots/{snapshot_id}` - Get snapshot details
- `DELETE /v1/namespaces/{namespace}/snapshots/{snapshot_id}` - Delete snapshot
- `POST /v1/namespaces/{namespace}/snapshots/{snapshot_id}/restore` - Restore with TTL check

**Request/Response Models:**

```rust
pub struct CreateSnapshotRequest {
    pub ttl_secs: Option<u64>,  // Optional, uses server default
    pub tag: Option<String>,    // Optional user tag
}

pub struct CreateSnapshotResponse {
    pub snapshot_id: String,
    pub status: String,  // "creating"
}

pub struct SnapshotInfo {
    pub snapshot_id: String,
    pub sandbox_id: String,
    pub namespace: String,
    pub status: String,
    pub created_at: u64,     // milliseconds
    pub ttl_secs: u64,
    pub expires_at: Option<u64>,  // None if ttl=0
    pub size_bytes: Option<u64>,
    pub image_ref: String,
}

pub struct RestoreSnapshotRequest {
    pub force: bool,  // Override TTL check
    pub override_config: Option<SandboxOverrides>,
}

pub struct RestoreSnapshotResponse {
    pub sandbox_id: String,
    pub status: String,
    pub restored_from_snapshot: bool,  // false if TTL expired
    pub ttl_expired: bool,
}
```

**TTL Logic in Restore Handler:**

```rust
pub async fn restore_snapshot(...) -> Result<Json<RestoreSnapshotResponse>> {
    let snapshot = state.indexify_state.reader()
        .get_snapshot(&namespace, &snapshot_id).await?;

    // Check TTL
    let ttl_expired = if snapshot.ttl_secs > 0 {
        let age_secs = (get_epoch_time_in_ns() - snapshot.created_at) / 1_000_000_000;
        age_secs >= snapshot.ttl_secs as u128
    } else {
        false  // TTL=0 means no expiration
    };

    let restored_from_snapshot = if request.force || !ttl_expired {
        // Use snapshot image
        create_sandbox_with_image(&state, &snapshot.image_ref, overrides).await?;
        true
    } else {
        // TTL expired - create fresh sandbox from original config
        create_sandbox_from_config(&state, &snapshot.sandbox_config).await?;
        false
    };

    Ok(Json(RestoreSnapshotResponse {
        sandbox_id: new_sandbox_id,
        status: "pending",
        restored_from_snapshot,
        ttl_expired,
    }))
}
```

#### 1.5 Request Processing (`crates/server/src/state_store/requests.rs`)

Add snapshot request types:

```rust
pub enum RequestPayload {
    // ... existing variants ...
    CreateSnapshot(CreateSnapshotRequest),
    DeleteSnapshot(DeleteSnapshotRequest),
}

pub struct CreateSnapshotRequest {
    pub snapshot_id: SnapshotId,
    pub sandbox_id: SandboxId,
    pub namespace: String,
    pub ttl_secs: u64,
}

pub struct DeleteSnapshotRequest {
    pub snapshot_id: SnapshotId,
    pub namespace: String,
}
```

#### 1.6 Snapshot Processor (`crates/server/src/processor/snapshot_processor.rs` - NEW FILE)

Follow patterns from `sandbox_processor.rs`:

```rust
pub struct SnapshotProcessor {
    clock: u64,
}

impl SnapshotProcessor {
    /// Process snapshot creation requests
    pub async fn process_snapshot_creations(
        &self,
        in_memory_state: &mut InMemoryState,
    ) -> Result<SchedulerUpdateRequest> {
        let mut update = SchedulerUpdateRequest::default();

        // Find snapshots in Creating status
        for (key, snapshot) in &in_memory_state.snapshots {
            if snapshot.status != SnapshotStatus::Creating {
                continue;
            }

            // Find executor for sandbox's container
            let sandbox = in_memory_state.sandboxes.get(&SandboxKey::new(
                &snapshot.namespace,
                snapshot.sandbox_id.get()
            ))?;

            let executor_id = sandbox.executor_id.as_ref()?;

            // Queue snapshot creation request to executor via gRPC
            // (Add to executor update request)
        }

        Ok(update)
    }

    /// Handle snapshot operation results from executors
    pub fn handle_snapshot_result(
        &self,
        in_memory_state: &mut InMemoryState,
        snapshot_id: &SnapshotId,
        result: SnapshotOperationResult,
    ) -> Result<()> {
        // Transition: Creating -> Active (success) or delete (failure)
        Ok(())
    }
}
```

#### 1.7 gRPC API Extensions (`proto/executor_api.proto`)

Add snapshot RPCs:

```protobuf
message CreateSnapshotRequest {
    string snapshot_id = 1;
    string sandbox_id = 2;
    string container_id = 3;
    uint64 ttl_secs = 4;
}

message CreateSnapshotResponse {
    string image_ref = 1;
    optional uint64 size_bytes = 2;
}

message DeleteSnapshotRequest {
    string snapshot_id = 1;
    string image_ref = 2;
}

service ExecutorService {
    rpc CreateSnapshot(CreateSnapshotRequest) returns (CreateSnapshotResponse);
    rpc DeleteSnapshot(DeleteSnapshotRequest) returns (DeleteSnapshotResponse);
}
```

Implement handlers in `crates/dataplane/src/lib.rs` executor API section.

#### 1.8 Configuration (`crates/server/src/config.rs`)

Add snapshot settings:

```rust
pub struct ServerConfig {
    // ... existing fields ...

    /// Default TTL for snapshots (seconds, 0 = no expiration)
    #[serde(default = "default_snapshot_ttl")]
    pub default_snapshot_ttl_secs: u64,

    /// Max snapshots per namespace
    #[serde(default = "default_max_snapshots_per_namespace")]
    pub max_snapshots_per_namespace: usize,
}

fn default_snapshot_ttl() -> u64 {
    14 * 24 * 3600  // 2 weeks
}

fn default_max_snapshots_per_namespace() -> usize {
    100
}
```

#### 1.9 Validation & Error Handling

**Create Snapshot Validation:**
- Sandbox must exist and status == Running (return 409 Conflict if not)
- Check max snapshots per namespace limit (return 403 Forbidden if exceeded)

**Restore Validation:**
- Snapshot must exist and status == Active (return 404 if not)
- Snapshot image must exist on executor (return 404 if not found)

**Edge Cases:**
- Concurrent snapshot operations on same sandbox → Serialize via processor
- Snapshot during termination → May succeed or fail, both acceptable
- Restore to different executor → Phase 1: return error "snapshot not available", Phase 2: registry pull

### Phase 2: TTL Management & Cleanup

**Goal:** Automatic expiration and cleanup of snapshots.

#### 2.1 Background Cleanup Processor

Create `crates/server/src/processor/snapshot_cleanup_processor.rs`:

```rust
pub struct SnapshotCleanupProcessor {
    clock: u64,
}

impl SnapshotCleanupProcessor {
    /// Clean up expired snapshots (run periodically)
    pub async fn cleanup_expired_snapshots(
        &self,
        in_memory_state: &mut InMemoryState,
    ) -> Result<Vec<SnapshotId>> {
        let mut expired_snapshots = Vec::new();
        let now = get_epoch_time_in_ns();

        for (key, snapshot) in &in_memory_state.snapshots {
            if snapshot.ttl_secs == 0 {
                continue;  // No expiration
            }

            let age_secs = (now - snapshot.created_at) / 1_000_000_000;
            if age_secs >= snapshot.ttl_secs as u128 {
                expired_snapshots.push(snapshot.id.clone());
            }
        }

        // Create deletion requests for expired snapshots
        // ...

        Ok(expired_snapshots)
    }
}
```

Integrate into main processor loop with configurable interval (default: 1 hour).

#### 2.2 Snapshot Deletion Flow

State transitions: `Active` → `Deleting` → removed from state

1. Mark snapshot as `Deleting`
2. Send delete request to executor via gRPC
3. Executor calls `driver.delete_snapshot(image_ref)`
4. Executor reports completion
5. Remove snapshot from state store

#### 2.3 Metrics & Monitoring

Add Prometheus metrics:
- `snapshot_create_duration_seconds` (histogram)
- `snapshot_restore_duration_seconds` (histogram)
- `snapshots_total` (gauge, labeled by namespace, status)
- `snapshot_size_bytes` (gauge)
- `snapshot_age_seconds` (gauge)
- `snapshot_operations_total` (counter, labeled by operation, result)

### Phase 3: Registry-Based Distribution (Future)

**Goal:** Multi-executor snapshot availability via Docker registry.

#### 3.1 Registry Configuration

Add to `ServerConfig`:

```rust
pub struct SnapshotRegistryConfig {
    pub url: String,          // "registry.example.com"
    pub username: Option<String>,
    pub password: Option<String>,
    pub repository: String,   // "indexify/snapshots"
}
```

#### 3.2 Push to Registry After Creation

Modify `DockerDriver::create_snapshot()`:
1. Commit container to local image
2. Tag for registry: `{registry}/{repository}:{snapshot_tag}`
3. Push to registry
4. Return registry image ref

#### 3.3 Pull from Registry Before Restore

Modify container startup to check for snapshot images:
1. If image ref starts with registry URL, pull from registry
2. Cache locally for future use
3. Start container from cached image

### Phase 4: Advanced Features (Future)

- Snapshot compression options
- Snapshot integrity verification (checksum validation)
- Snapshot search/filter API with advanced queries
- Snapshot cloning (create snapshot from snapshot)
- Garbage collection for unused image layers
- Snapshot usage tracking and analytics

## Critical Files to Modify

### Must Modify (Phase 1)
1. **`crates/dataplane/src/driver/docker.rs`** - Add `create_snapshot()` and `delete_snapshot()` methods
2. **`crates/dataplane/src/driver/mod.rs`** - Extend `ProcessDriver` trait with snapshot methods
3. **`crates/server/src/data_model/mod.rs`** - Add `SandboxSnapshot`, `SnapshotId`, `SnapshotStatus` entities
4. **`crates/server/src/state_store/mod.rs`** - Add `SNAPSHOTS_CF` column family and CRUD methods
5. **`crates/server/src/routes/sandbox_snapshots.rs`** (NEW) - Implement API handlers
6. **`crates/server/src/routes/mod.rs`** - Register snapshot routes
7. **`crates/server/src/routes_v1.rs`** - Add snapshot routes to router
8. **`crates/server/src/state_store/requests.rs`** - Add snapshot request types
9. **`crates/server/src/processor/snapshot_processor.rs`** (NEW) - Snapshot state management
10. **`crates/server/src/config.rs`** - Add snapshot configuration fields
11. **`proto/executor_api.proto`** - Add snapshot gRPC methods
12. **`crates/dataplane/src/lib.rs`** - Implement executor-side snapshot handlers

### Reference Files (Patterns to Follow)
- **`crates/server/src/routes/sandboxes.rs`** - API endpoint patterns
- **`crates/server/src/processor/sandbox_processor.rs`** - Processor patterns
- **`crates/dataplane/src/function_container_manager/lifecycle.rs`** - Container lifecycle patterns

## Verification & Testing

### Unit Tests

**`crates/dataplane/src/driver/docker.rs`:**
```rust
#[tokio::test]
async fn test_create_snapshot_success() {
    // Start test container
    // Create snapshot via docker.create_snapshot()
    // Verify image exists via docker.inspect_image()
    // Cleanup
}

#[tokio::test]
async fn test_restore_from_snapshot() {
    // Create container, write file
    // Snapshot container
    // Stop container
    // Start new container from snapshot image
    // Verify file exists
}

#[tokio::test]
async fn test_delete_snapshot() {
    // Create snapshot
    // Delete via docker.delete_snapshot()
    // Verify image removed
}
```

### Integration Tests

**`crates/server/src/integration_test_snapshots.rs` (NEW):**
```rust
#[tokio::test]
async fn test_snapshot_lifecycle_end_to_end() {
    // 1. Create sandbox via API
    // 2. Wait for Running status
    // 3. Create snapshot via API
    // 4. Verify snapshot Active
    // 5. Delete original sandbox
    // 6. Restore from snapshot via API
    // 7. Verify new sandbox Running
    // 8. Delete snapshot via API
    // 9. Verify snapshot removed
}

#[tokio::test]
async fn test_snapshot_ttl_expiration() {
    // 1. Create sandbox
    // 2. Create snapshot with TTL=5s
    // 3. Wait 6 seconds
    // 4. Restore via API
    // 5. Verify restored_from_snapshot=false, ttl_expired=true
    // 6. Verify new sandbox created from original config (not snapshot)
}

#[tokio::test]
async fn test_snapshot_limits() {
    // 1. Create sandbox
    // 2. Create max_snapshots_per_namespace snapshots
    // 3. Attempt to create one more
    // 4. Verify 403 Forbidden error
}
```

### Manual Verification

```bash
# 1. Create sandbox
curl -X POST http://localhost:8900/v1/namespaces/default/sandboxes \
  -H "Content-Type: application/json" \
  -d '{"image": "python:3.11-slim", "resources": {"cpus": 1, "memory_mb": 512}}'
# Response: {"sandbox_id": "sb-abc123", "status": "pending"}

# 2. Wait for Running, then create snapshot
curl -X POST http://localhost:8900/v1/namespaces/default/sandboxes/sb-abc123/snapshots \
  -H "Content-Type: application/json" \
  -d '{"ttl_secs": 3600}'
# Response: {"snapshot_id": "snap-xyz789", "status": "creating"}

# 3. List snapshots
curl http://localhost:8900/v1/namespaces/default/sandboxes/sb-abc123/snapshots
# Response: {"snapshots": [{"snapshot_id": "snap-xyz789", "status": "active", ...}]}

# 4. Get snapshot details
curl http://localhost:8900/v1/namespaces/default/snapshots/snap-xyz789
# Response: {"snapshot_id": "snap-xyz789", "size_bytes": 123456789, ...}

# 5. Restore from snapshot
curl -X POST http://localhost:8900/v1/namespaces/default/snapshots/snap-xyz789/restore
# Response: {"sandbox_id": "sb-def456", "restored_from_snapshot": true, "ttl_expired": false}

# 6. Verify Docker images
docker images | grep indexify-snapshots
# Should show: indexify-snapshots:sb-abc123-snap-xyz789

# 7. Delete snapshot
curl -X DELETE http://localhost:8900/v1/namespaces/default/snapshots/snap-xyz789
```

### Load Testing

Test concurrent snapshot operations:
- 10 sandboxes × 5 snapshots each = 50 concurrent snapshot creations
- Monitor: creation time, disk usage, API latency
- Verify: all snapshots created successfully, no data loss

## Rollout Strategy

1. **Phase 1 (Week 1-2):** Implement core snapshot/restore with local storage
   - Merge incrementally: data model → driver → API → processor
   - Feature flag: `enable_snapshots` (default: false)

2. **Phase 2 (Week 3):** Add TTL cleanup and monitoring
   - Enable for internal testing
   - Monitor disk usage and performance

3. **Phase 3 (Week 4+):** Registry-based distribution (if needed)
   - Requires registry infrastructure setup
   - Optional for multi-executor deployments

## Success Criteria

- ✅ Users can create snapshots of running sandboxes via API
- ✅ Snapshots captured using `docker commit` with gVisor compatibility
- ✅ Restore with TTL check: uses snapshot if within TTL, creates fresh otherwise
- ✅ TTL-based automatic cleanup prevents disk space exhaustion
- ✅ End-to-end tests pass for snapshot lifecycle
- ✅ API follows existing patterns (namespace-scoped, consistent error handling)
- ✅ Metrics track snapshot operations and storage usage

## Implementation Status

All phases 1 and 2 have been completed:

- ✅ Data models implemented
- ✅ State store integration complete
- ✅ Docker driver snapshot methods added
- ✅ REST API endpoints functional
- ✅ Request types and state changes integrated
- ✅ Snapshot processor created and integrated
- ✅ gRPC protocol extended
- ✅ Executor-side handlers implemented
- ✅ Configuration added
- ✅ Background cleanup processor running
- ✅ Comprehensive unit tests passing

The feature is production-ready for single-executor deployments. Phase 3 (registry-based distribution) remains as a future enhancement for multi-executor scenarios.
