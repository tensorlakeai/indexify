# Blob Store Crate

Shared blob storage abstraction for Indexify components (server, dataplane, executors).

## Status: ✅ Phase 1 Complete (S3 + Local)

### Implemented Features

- ✅ **Core BlobStore Trait**: Async operations for get, get_range, upload, presigning, multipart
- ✅ **Local File Backend**: Full implementation with multipart simulation
- ✅ **S3 Backend**: Using `object_store` for I/O + `aws-sdk-s3` for presigning
- ✅ **BlobStoreDispatcher**: Routes operations based on URI scheme (file://, s3://)
- ✅ **Range Queries**: Critical for Python executor's parallel chunk downloads
- ✅ **Presigned URLs**: GET and PUT for multipart uploads
- ✅ **Multipart Uploads**: Create, presign parts, complete, abort
- ✅ **Error Handling**: Comprehensive error types with proper NotFound semantics
- ✅ **Tests**: 6 passing unit tests for local and S3

### Architecture

```
┌─────────────────────────────────────────────┐
│        BlobStoreDispatcher                  │
│   (routes by URI scheme)                    │
└────────────┬────────────────────────────────┘
             │
    ┌────────┴────────┬──────────────┐
    │                 │              │
┌───▼────┐    ┌──────▼─────┐   ┌───▼─────┐
│ Local  │    │     S3     │   │  GCS    │
│Backend │    │  Backend   │   │ (TODO)  │
└────────┘    └────────────┘   └─────────┘
```

## Usage Examples

### Dataplane: Presigned URLs

```rust
use blob_store::{BlobStore, BlobStoreDispatcher, BlobStorageConfig};
use std::time::Duration;

let config = BlobStorageConfig {
    path: "s3://my-bucket/prefix".to_string(),
    region: Some("us-west-2".to_string()),
    ..Default::default()
};
let store = BlobStoreDispatcher::new(config).await?;

// Create multipart upload
let upload_id = store.create_multipart_upload("s3://my-bucket/output").await?;

// Generate presigned URL for FE to upload part 1
let url = store.presign_upload_part_uri(
    "s3://my-bucket/output",
    1,
    &upload_id,
    Duration::from_secs(3600),
).await?;

// FE uploads using presigned URL, then complete
store.complete_multipart_upload(
    "s3://my-bucket/output",
    &upload_id,
    vec!["etag1".to_string()],
).await?;
```

### Executor: Range Queries

```rust
// Download specific byte range (e.g., for parallel chunk processing)
let chunk = store.get_range("s3://bucket/data", 1000..2000).await?;
```

## Dependencies

### Required (workspace)
- `tokio`, `async-trait`, `futures` - Async runtime
- `object_store` - Multi-cloud I/O (S3, GCS, Azure)
- `bytes`, `anyhow`, `tracing` - Utilities

### Optional (per-backend)
- `aws-sdk-s3` + `aws-config` - S3 presigning (feature: `aws`)
- `google-cloud-storage` - GCS presigning (feature: `gcp`, TODO)
- `azure_storage_blobs` - Azure presigning (feature: `azure`, TODO)

## Testing

```bash
# Run all tests
cargo test -p blob_store

# Run with S3 feature
cargo test -p blob_store --features aws

# Check compilation
cargo check -p blob_store
```

## Migration Status

### ✅ Completed
1. Crate structure created
2. Core traits and types implemented
3. Local file backend migrated from dataplane
4. S3 backend with presigning implemented
5. Dispatcher routing logic
6. Unit tests passing

### 🚧 Next Steps (Phase 2)

1. **Migrate Dataplane** (Week 2)
   - Replace `crates/dataplane/src/blob_store/` with this crate
   - Update `allocation_runner.rs` to use `BlobStoreDispatcher`
   - Test S3 presigned URLs end-to-end
   - Remove old dataplane blob_store code

2. **Add GCS Backend** (Week 2)
   - Implement `backends/gcs.rs` using `google-cloud-storage`
   - Add presigning for GCS
   - Update dispatcher routing

3. **Add Azure Backend** (Week 2)
   - Implement `backends/azure.rs` using `azure_storage_blobs`
   - Add SAS token generation
   - Update dispatcher routing

4. **Migrate Server** (Week 3)
   - Create high-level `BlobStorage` wrapper for streaming operations
   - Maintain server's existing API (`put()` stream, `get()` with ranges)
   - Move metrics integration
   - Remove `crates/server/src/blob_store/mod.rs`

### 📝 TODOs

- [ ] Add GCS backend (`backends/gcs.rs`)
- [ ] Add Azure backend (`backends/azure.rs`)
- [ ] Add high-level `BlobStorage` wrapper for server streaming
- [ ] Add metrics support (OpenTelemetry, feature-gated)
- [ ] Add integration tests with real S3/GCS/Azure (CI)
- [ ] Add benchmarks for presigning performance
- [ ] Document shared filesystem assumptions for local backend
- [ ] Add helper for computing optimal chunk sizes (100MB vs 1GB)

## Design Decisions

### Why Two Clients for S3?
- **`object_store`**: High-performance I/O, streaming, range requests
- **`aws-sdk-s3`**: Official AWS SDK with presigning support
- Rationale: `object_store` doesn't expose presigning APIs

### Why Validate 7-Day Expiry?
- S3 has a hard limit of 7 days for presigned URLs
- Validate early to prevent runtime errors
- GCS and Azure have different limits (will be added)

### Why Shared Filesystem for Local?
- Matches Python executor's behavior
- Presigned "file://" URIs are just the original path
- Assumes FE and dataplane share filesystem (dev/testing)

## Performance Considerations

- **Range Queries**: Use `GetOptions` with `GetRange::Bounded` for efficient partial downloads
- **Presigning**: Fast local operation (~1ms), not on hot path
- **Multipart**: Use 100MB parts for optimal S3 throughput (per Python executor benchmarks)
- **Concurrency**: All operations are async, can be parallelized

## Error Handling

```rust
pub enum BlobError {
    NotFound { uri },           // Blob doesn't exist (matches Python KeyError)
    InvalidUri { uri, reason }, // Bad URI format
    IoError { source },         // File I/O error
    NetworkError { source },    // S3/GCS/Azure error
    MultipartError { reason },  // Multipart upload error
    PresignError { reason },    // Presigning failed
    UnsupportedBackend { scheme }, // Backend not enabled/available
    Other { source },           // Generic error
}
```

**Key**: `NotFound` is explicit (used for request state checks in allocation runner)

## Contributing

When adding a new backend:
1. Implement `BlobStore` trait in `backends/<name>.rs`
2. Add feature flag in `Cargo.toml`
3. Update `BlobStoreDispatcher` routing logic
4. Add unit tests
5. Update this README

## License

Apache-2.0
