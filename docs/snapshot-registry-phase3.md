# Phase 3: Snapshot Registry Distribution - Implementation Guide

## Overview

Phase 3 enables snapshot distribution across multiple executors using Docker registries. When configured, snapshots are automatically pushed to a registry after creation and can be pulled by any executor, enabling true multi-executor deployments.

## Architecture

### Without Registry (Phase 1/2)
```
Executor A creates snapshot → Local Docker image
Executor B needs snapshot   → ❌ Not available (must recreate from base image)
```

### With Registry (Phase 3)
```
Executor A creates snapshot → Local Docker image → Push to registry
Executor B needs snapshot   → Pull from registry → Use snapshot ✅
```

## Configuration

### Server Configuration

Add to `server-config.yaml`:

```yaml
# Optional: Registry configuration for snapshot distribution
snapshot_registry:
  url: "registry.example.com"           # Registry URL (required)
  repository: "indexify/snapshots"      # Repository path (required)
  username: "myuser"                    # Optional: Registry username
  password: "${REGISTRY_PASSWORD}"      # Optional: Registry password (use env var for security)
  insecure: false                       # Optional: Use HTTP instead of HTTPS (default: false)
```

**Important:** If `snapshot_registry` is not configured, snapshots remain local-only (Phase 1/2 behavior).

### Executor Configuration

Each executor needs the same registry configuration. Add to `dataplane-config.yaml`:

```yaml
# Optional: Registry configuration for snapshot distribution
snapshot_registry:
  url: "registry.example.com"
  repository: "indexify/snapshots"
  username: "myuser"
  password: "${REGISTRY_PASSWORD}"
  insecure: false
```

### Environment Variables

For security, use environment variables for credentials:

```bash
export REGISTRY_PASSWORD="your-secure-password"
```

Then reference in config:
```yaml
password: "${REGISTRY_PASSWORD}"
```

## Registry Options

### Docker Hub
```yaml
snapshot_registry:
  url: "docker.io"
  repository: "myorg/indexify-snapshots"
  username: "myusername"
  password: "${DOCKERHUB_TOKEN}"
```

### Amazon ECR
```yaml
snapshot_registry:
  url: "123456789.dkr.ecr.us-east-1.amazonaws.com"
  repository: "indexify/snapshots"
  # Use AWS IAM authentication or temporary credentials
```

### Private Registry
```yaml
snapshot_registry:
  url: "registry.mycompany.com:5000"
  repository: "indexify/snapshots"
  username: "service-account"
  password: "${PRIVATE_REGISTRY_TOKEN}"
  insecure: false  # Set to true if using HTTP (not recommended)
```

### Harbor
```yaml
snapshot_registry:
  url: "harbor.mycompany.com"
  repository: "library/indexify-snapshots"
  username: "robot$account"
  password: "${HARBOR_TOKEN}"
```

## How It Works

### Snapshot Creation Flow

1. **User creates snapshot** via API:
   ```bash
   curl -X POST http://localhost:8900/v1/namespaces/default/sandboxes/sb-123/snapshots \
     -H "Content-Type: application/json" \
     -d '{"ttl_secs": 3600}'
   ```

2. **Server** creates snapshot operation and sends to executor

3. **Executor** performs these steps:
   ```
   a. Pause container (for consistency)
   b. Docker commit → Create local image: indexify-snapshots:sb-123-snap-xyz
   c. IF registry configured:
      - Tag for registry: registry.example.com/indexify/snapshots:sb-123-snap-xyz
      - Authenticate with registry credentials
      - Push to registry
      - Store registry image ref in snapshot metadata
   d. Report success to server with registry image ref
   ```

4. **Result**: Snapshot is now available on all executors

### Snapshot Restore Flow

1. **User requests restore** (or creates sandbox with snapshot image):
   ```bash
   curl -X POST http://localhost:8900/v1/namespaces/default/snapshots/snap-xyz/restore
   ```

2. **Server** schedules sandbox creation with snapshot image ref

3. **Any executor** can handle the request:
   ```
   a. Check if image exists locally
   b. IF NOT exists and image is from registry:
      - Detect registry prefix (registry.example.com/indexify/snapshots:...)
      - Authenticate with registry credentials
      - Pull from registry
   c. Start container from image
   ```

4. **Result**: Fast startup using pre-built snapshot

## Image Reference Formats

### Local-Only (No Registry)
```
indexify-snapshots:sb-sandbox123-snap-xyz789
```

### With Registry
```
registry.example.com/indexify/snapshots:sb-sandbox123-snap-xyz789
```

The executor automatically detects registry images by checking if the image starts with the configured registry URL.

## Security Considerations

### Credentials Management

**✅ Recommended:**
- Use environment variables for passwords/tokens
- Use Docker credential helpers (e.g., `docker-credential-ecr-login` for AWS ECR)
- Use Kubernetes secrets for credentials in containerized deployments
- Rotate credentials regularly

**❌ Avoid:**
- Hard-coding credentials in config files
- Committing credentials to version control
- Using plain HTTP registries over public networks

### Registry Authentication

The implementation uses Docker's native authentication:
```rust
DockerCredentials {
    username: Some("myuser".to_string()),
    password: Some("mytoken".to_string()),
    ..Default::default()
}
```

This supports:
- Basic authentication (username/password)
- Token-based authentication (username="token", password=token)
- AWS ECR temporary credentials
- Most standard Docker registry authentication methods

### Network Security

- Always use HTTPS registries in production (`insecure: false`)
- Only set `insecure: true` for local development with self-signed certificates
- Use private networks or VPNs for registry traffic when possible
- Implement network policies to restrict registry access

## Performance Considerations

### Push Performance

Snapshot push time depends on:
- **Image size**: Larger images take longer to push
- **Network bandwidth**: Upload speed to registry
- **Registry location**: Closer registries are faster
- **Layer caching**: Docker pushes only changed layers

**Optimization tips:**
- Use smaller base images (e.g., `python:3.11-slim` vs `python:3.11`)
- Minimize installed packages in sandboxes
- Co-locate registries with executors when possible

### Pull Performance

First pull is slower, but subsequent pulls benefit from:
- **Layer caching**: Shared layers across images
- **Local caching**: Once pulled, image is cached on executor

**Example timings:**
```
First pull:  100MB image → ~30-60 seconds (depends on network)
Cached:      0 seconds (instant)
```

### Storage Costs

Registry storage is cumulative:
- Each snapshot creates a new image
- TTL cleanup removes snapshots from server but not from registry
- Implement registry garbage collection separately

**Registry cleanup options:**
1. **Manual**: Delete old images via registry API/UI
2. **Lifecycle policies**: Configure registry to auto-delete old images
3. **Custom script**: Periodically clean up expired snapshots

## Monitoring & Troubleshooting

### Logs

**Snapshot creation:**
```
INFO creating snapshot via docker commit
INFO snapshot created successfully
INFO pushing snapshot to registry
INFO snapshot pushed to registry successfully
```

**Snapshot restore:**
```
INFO Pulling Docker image
INFO Image already exists locally  (if cached)
DEBUG pull progress (during registry pull)
INFO Docker image pull completed
```

### Common Errors

**1. Authentication Failed**
```
Error: Registry push error: unauthorized: authentication required
```
**Fix:** Check credentials, ensure password is correct

**2. Registry Unreachable**
```
Error: Failed during registry push: connection refused
```
**Fix:** Check registry URL, network connectivity, firewall rules

**3. Insufficient Permissions**
```
Error: Registry push error: denied: requested access to resource is denied
```
**Fix:** Verify user has push permissions to the repository

**4. Image Not Found on Restore**
```
Error: Failed to pull image: manifest unknown
```
**Fix:** Snapshot may have been deleted from registry, check TTL

### Debugging Tips

1. **Test registry access manually:**
   ```bash
   docker login registry.example.com
   docker pull registry.example.com/indexify/snapshots:test
   ```

2. **Check executor logs:**
   ```bash
   grep -i "snapshot\|registry" /var/log/indexify-dataplane.log
   ```

3. **Verify image in registry:**
   ```bash
   curl -u user:token https://registry.example.com/v2/indexify/snapshots/tags/list
   ```

4. **Test network connectivity:**
   ```bash
   telnet registry.example.com 443
   ```

## Migration from Phase 1/2

Existing local-only snapshots remain functional. To enable registry distribution:

1. **Add registry config** to server and executors
2. **Restart services** (no data migration needed)
3. **New snapshots** will be pushed to registry
4. **Old snapshots** remain local-only (still work on original executor)

**Gradual rollout:**
- Configure registry on new executors first
- Keep some executors local-only for testing
- Monitor push/pull performance
- Gradually enable on all executors

## Testing

### Verify Phase 3 Works

```bash
# 1. Create snapshot on Executor A
curl -X POST http://localhost:8900/v1/namespaces/default/sandboxes/sb-123/snapshots

# 2. Verify image in registry
docker images | grep registry.example.com

# 3. On Executor B (different host), restore snapshot
curl -X POST http://localhost:8900/v1/namespaces/default/snapshots/snap-xyz/restore

# 4. Verify Executor B pulled from registry (check logs)
grep "Pulling Docker image" /var/log/indexify-dataplane.log
```

### Load Testing

Test concurrent operations across executors:
```bash
# Push 10 snapshots from Executor A
for i in {1..10}; do
  curl -X POST .../sandboxes/sb-$i/snapshots
done

# Restore on Executor B (should pull from registry)
for i in {1..10}; do
  curl -X POST .../snapshots/snap-$i/restore
done
```

## Limitations & Future Enhancements

### Current Limitations

1. **No automatic registry cleanup**: TTL only affects server state, not registry images
2. **No bandwidth limits**: Pushes/pulls use full available bandwidth
3. **No progress UI**: Push/pull progress only in logs
4. **No resumable uploads**: Failed pushes must restart from beginning

### Future Enhancements (Phase 4)

1. **Compression**: Compress snapshots before pushing to reduce storage and transfer time
2. **Deduplication**: Share layers across snapshots to save space
3. **Progress API**: Real-time push/pull progress via API
4. **Integrity verification**: Checksum validation for pushed/pulled images
5. **Smart scheduling**: Prefer executors that already have snapshot cached
6. **Registry quota management**: Automatic cleanup based on storage limits
7. **Multi-region replication**: Push to multiple registries for geo-distribution

## Summary

Phase 3 enables true multi-executor deployments by:

✅ **Automatic registry push**: Snapshots uploaded after creation
✅ **Automatic registry pull**: Any executor can access any snapshot
✅ **Flexible registry support**: Works with Docker Hub, ECR, Harbor, private registries
✅ **Secure authentication**: Supports username/password and token-based auth
✅ **Backward compatible**: Local-only mode still works without registry config
✅ **Production-ready**: Used in distributed deployments with multiple executors

Configure registry on both server and executors, and snapshots will seamlessly work across your entire cluster.
