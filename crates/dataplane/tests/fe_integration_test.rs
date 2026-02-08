//! Integration tests for Function Executor gRPC protocol.
//!
//! These tests spawn a real `function-executor` process (from the tensorlake
//! Python SDK), connect to it via gRPC, and exercise the full FE protocol:
//! initialize, create allocation, watch state, send updates, and collect
//! results.
//!
//! The tests are automatically skipped if the `function-executor` binary is not
//! found. To run them:
//!
//! ```bash
//! # One-time setup:
//! python3 -m venv target/fe-test-venv
//! target/fe-test-venv/bin/pip install tensorlake==0.3.5
//!
//! # Run:
//! cargo test -p indexify-dataplane --test fe_integration_test
//! ```

use std::{
    collections::HashSet,
    net::TcpListener,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result};
use indexify_dataplane::function_executor::fe_client::FunctionExecutorGrpcClient;
use proto_api::function_executor_pb::{
    self,
    AllocationOutputBlob,
    AllocationState,
    AllocationUpdate,
    Blob,
    BlobChunk,
    CreateAllocationRequest,
    FunctionInputs,
    FunctionRef,
    InitializeRequest,
    SerializedObject,
    SerializedObjectInsideBlob,
    SerializedObjectManifest,
};
use tokio::process::{Child, Command};
use tonic::Streaming;

// ---------------------------------------------------------------------------
// Binary & Python discovery
// ---------------------------------------------------------------------------

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

/// Locate the `function-executor` binary.
fn find_fe_binary() -> Option<PathBuf> {
    // 1. Explicit env-var override
    if let Ok(p) = std::env::var("FE_BINARY_PATH") {
        let pb = PathBuf::from(&p);
        if pb.exists() {
            return Some(pb);
        }
    }

    // 2. venv installed by the setup instructions
    let venv = workspace_root().join("target/fe-test-venv/bin/function-executor");
    if venv.exists() {
        return Some(venv);
    }

    // 3. On PATH
    which("function-executor")
}

/// Locate a Python interpreter (must come from the same venv that has
/// tensorlake).
fn find_python() -> Option<PathBuf> {
    let venv = workspace_root().join("target/fe-test-venv/bin/python3");
    if venv.exists() {
        return Some(venv);
    }
    which("python3")
}

/// Simple `which` using PATH.
fn which(name: &str) -> Option<PathBuf> {
    std::env::var_os("PATH").and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(name))
            .find(|p| p.exists())
    })
}

/// Skip the test (return early) if the FE binary is not available.
macro_rules! skip_if_no_fe {
    () => {
        match find_fe_binary() {
            Some(p) => p,
            None => {
                eprintln!(
                    "SKIPPED: function-executor binary not found. \
                     Run: python3 -m venv target/fe-test-venv && \
                     target/fe-test-venv/bin/pip install tensorlake==0.3.5"
                );
                return;
            }
        }
    };
}

macro_rules! skip_if_no_python {
    () => {
        match find_python() {
            Some(p) => p,
            None => {
                eprintln!("SKIPPED: python3 not found");
                return;
            }
        }
    };
}

// ---------------------------------------------------------------------------
// Port allocation
// ---------------------------------------------------------------------------

fn allocate_ephemeral_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

// ---------------------------------------------------------------------------
// FE process wrapper
// ---------------------------------------------------------------------------

struct FEProcess {
    child: Child,
    port: u16,
    client: FunctionExecutorGrpcClient,
}

impl FEProcess {
    async fn spawn(fe_binary: &Path) -> Result<Self> {
        let port = allocate_ephemeral_port()?;
        let addr = format!("127.0.0.1:{}", port);

        let child = Command::new(fe_binary)
            .arg("--address")
            .arg(&addr)
            .arg("--executor-id")
            .arg("test-executor")
            .arg("--function-executor-id")
            .arg("test-fe")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .with_context(|| format!("Failed to spawn function-executor at {:?}", fe_binary))?;

        let client = FunctionExecutorGrpcClient::connect_with_retry(&addr, Duration::from_secs(15))
            .await
            .with_context(|| format!("Failed to connect to FE at {}", addr))?;

        Ok(Self {
            child,
            port,
            client,
        })
    }

    async fn kill(&mut self) {
        let _ = self.child.kill().await;
    }
}

impl Drop for FEProcess {
    fn drop(&mut self) {
        // Best-effort kill on drop (start_kill is sync)
        let _ = self.child.start_kill();
    }
}

// ---------------------------------------------------------------------------
// App-code ZIP generation
// ---------------------------------------------------------------------------

/// Call `generate_zip.py` via the venv Python and return a `SerializedObject`
/// containing the ZIP bytes.
async fn generate_app_code_zip(python: &Path) -> Result<SerializedObject> {
    let script =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/test_functions/generate_zip.py");

    let tmp_dir = tempfile::tempdir().context("Failed to create temp dir for zip")?;
    let zip_path = tmp_dir.path().join("app_code.zip");

    let output = Command::new(python)
        .arg(&script)
        .arg(&zip_path)
        .output()
        .await
        .context("Failed to run generate_zip.py")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        anyhow::bail!(
            "generate_zip.py failed (exit {:?}):\nstdout: {}\nstderr: {}",
            output.status.code(),
            stdout,
            stderr
        );
    }

    let zip_bytes = tokio::fs::read(&zip_path).await?;

    // Read metadata
    let meta_path = format!("{}.meta.json", zip_path.display());
    let meta_json = tokio::fs::read_to_string(&meta_path).await?;
    let meta: serde_json::Value = serde_json::from_str(&meta_json)?;
    let sha256 = meta["sha256"].as_str().unwrap_or("").to_string();
    let size = zip_bytes.len() as u64;

    // Keep the temp dir alive until we're done reading
    std::mem::forget(tmp_dir);

    Ok(SerializedObject {
        manifest: Some(SerializedObjectManifest {
            encoding: Some(function_executor_pb::SerializedObjectEncoding::BinaryZip as i32),
            encoding_version: Some(0),
            size: Some(size),
            metadata_size: Some(0),
            sha256_hash: Some(sha256),
            content_type: None,
            source_function_call_id: None,
        }),
        data: Some(zip_bytes),
    })
}

// ---------------------------------------------------------------------------
// Blob helpers (file:// backed)
// ---------------------------------------------------------------------------

/// Create a file-backed blob and write `data` into it.
fn create_file_blob_with_data(blob_id: &str, data: &[u8], dir: &Path) -> Result<(Blob, PathBuf)> {
    let file_path = dir.join(format!("{}.bin", blob_id));
    std::fs::write(&file_path, data)?;
    let uri = format!("file://{}", file_path.display());
    let blob = Blob {
        id: Some(blob_id.to_string()),
        chunks: vec![BlobChunk {
            uri: Some(uri),
            size: Some(data.len() as u64),
            etag: None,
        }],
    };
    Ok((blob, file_path))
}

/// Create an empty writable file blob for FE to write output into.
fn create_writable_file_blob(blob_id: &str, size: u64, dir: &Path) -> Result<(Blob, PathBuf)> {
    let file_path = dir.join(format!("{}.bin", blob_id));
    // Create an empty file (FE will write to it)
    std::fs::write(&file_path, &[])?;
    let uri = format!("file://{}", file_path.display());
    let blob = Blob {
        id: Some(blob_id.to_string()),
        chunks: vec![BlobChunk {
            uri: Some(uri),
            size: Some(size),
            etag: None,
        }],
    };
    Ok((blob, file_path))
}

// ---------------------------------------------------------------------------
// Input construction helpers
// ---------------------------------------------------------------------------

/// Build FunctionInputs for a simple JSON-serialized argument.
///
/// The argument value should be a JSON-serialized string (e.g. `"\"World\""`
/// for the string `World`).
fn build_function_inputs(
    arg_json_bytes: &[u8],
    blob_dir: &Path,
) -> Result<(FunctionInputs, PathBuf, PathBuf)> {
    use sha2::{Digest, Sha256};

    let sha256 = format!("{:x}", Sha256::digest(arg_json_bytes));

    // Input arg blob
    let (arg_blob, arg_path) = create_file_blob_with_data("arg-0", arg_json_bytes, blob_dir)?;

    let so = SerializedObjectInsideBlob {
        manifest: Some(SerializedObjectManifest {
            encoding: Some(function_executor_pb::SerializedObjectEncoding::Utf8Json as i32),
            encoding_version: Some(0),
            size: Some(arg_json_bytes.len() as u64),
            metadata_size: Some(0),
            sha256_hash: Some(sha256),
            content_type: None,
            source_function_call_id: None,
        }),
        offset: Some(0),
    };

    // Request error blob (empty, required but unused for success paths)
    let (error_blob, error_path) =
        create_writable_file_blob("request_error_blob", 10 * 1024 * 1024, blob_dir)?;

    let inputs = FunctionInputs {
        args: vec![so],
        arg_blobs: vec![arg_blob],
        request_error_blob: Some(error_blob),
        function_call_metadata: Some(Vec::new()),
    };

    Ok((inputs, arg_path, error_path))
}

/// Create an InitializeRequest for the hello_world function.
async fn build_init_request(python: &Path) -> Result<InitializeRequest> {
    let app_code = generate_app_code_zip(python).await?;
    Ok(InitializeRequest {
        function: Some(FunctionRef {
            namespace: Some("test-ns".to_string()),
            application_name: Some("hello_world".to_string()),
            function_name: Some("hello_world".to_string()),
            application_version: Some("v1".to_string()),
        }),
        application_code: Some(app_code),
    })
}

// ---------------------------------------------------------------------------
// State stream helpers
// ---------------------------------------------------------------------------

/// Read the next state from the stream, with a timeout.
async fn next_state(
    stream: &mut Streaming<AllocationState>,
    timeout: Duration,
) -> Result<AllocationState> {
    match tokio::time::timeout(timeout, stream.message()).await {
        Ok(Ok(Some(state))) => Ok(state),
        Ok(Ok(None)) => anyhow::bail!("Stream closed"),
        Ok(Err(e)) => anyhow::bail!("Stream error: {}", e),
        Err(_) => anyhow::bail!("Timeout waiting for allocation state"),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

/// Test 1: Spawn FE, connect, get_info(), verify fields.
#[tokio::test]
async fn test_fe_spawn_and_get_info() {
    let _ = tracing_subscriber::fmt::try_init();
    let fe_binary = skip_if_no_fe!();

    let mut fe = FEProcess::spawn(&fe_binary).await.unwrap();
    eprintln!("FE spawned on port {}", fe.port);

    let info = fe.client.get_info().await.unwrap();
    eprintln!("FE info: {:?}", info);

    // Version fields should be set (not empty)
    assert!(info.version.is_some(), "Expected FE to report a version");
    assert!(
        info.sdk_language.as_deref() == Some("python"),
        "Expected sdk_language=python, got {:?}",
        info.sdk_language
    );

    fe.kill().await;
}

/// Test 2: Spawn FE, initialize with hello_world function, verify success.
#[tokio::test]
async fn test_fe_initialize() {
    let _ = tracing_subscriber::fmt::try_init();
    let fe_binary = skip_if_no_fe!();
    let python = skip_if_no_python!();

    let mut fe = FEProcess::spawn(&fe_binary).await.unwrap();
    eprintln!("FE spawned on port {}", fe.port);

    let init_req = build_init_request(&python).await.unwrap();
    let resp = fe.client.initialize(init_req).await.unwrap();

    eprintln!("Initialize response: {:?}", resp);
    assert_eq!(
        resp.outcome_code(),
        function_executor_pb::InitializationOutcomeCode::Success,
        "Expected initialization success, got {:?}",
        resp.outcome_code()
    );

    fe.kill().await;
}

/// Test 3: Spawn FE, initialize, check_health, verify healthy.
#[tokio::test]
async fn test_fe_health_check() {
    let _ = tracing_subscriber::fmt::try_init();
    let fe_binary = skip_if_no_fe!();
    let python = skip_if_no_python!();

    let mut fe = FEProcess::spawn(&fe_binary).await.unwrap();

    let init_req = build_init_request(&python).await.unwrap();
    let resp = fe.client.initialize(init_req).await.unwrap();
    assert_eq!(
        resp.outcome_code(),
        function_executor_pb::InitializationOutcomeCode::Success,
    );

    let health = fe.client.check_health().await.unwrap();
    eprintln!("Health check: {:?}", health);
    assert_eq!(health.healthy, Some(true), "Expected FE to be healthy");

    fe.kill().await;
}

/// Test 4: Full allocation lifecycle — the core test.
///
/// 1. Spawn FE and initialize with hello_world
/// 2. Create allocation with JSON-serialized `"World"` input
/// 3. Watch allocation state, handle output blob requests
/// 4. Verify result is `"Hello, World!"`
#[tokio::test]
async fn test_fe_allocation_lifecycle() {
    let _ = tracing_subscriber::fmt::try_init();
    let fe_binary = skip_if_no_fe!();
    let python = skip_if_no_python!();

    // --- Setup ---
    let mut fe = FEProcess::spawn(&fe_binary).await.unwrap();
    eprintln!("FE spawned on port {}", fe.port);

    let init_req = build_init_request(&python).await.unwrap();
    let resp = fe.client.initialize(init_req).await.unwrap();
    assert_eq!(
        resp.outcome_code(),
        function_executor_pb::InitializationOutcomeCode::Success,
    );

    // --- Build allocation inputs ---
    let blob_dir = tempfile::tempdir().unwrap();
    // JSON-serialize the string "World" → `"World"` (with quotes, as JSON)
    let input_json = serde_json::to_vec(&"World").unwrap();
    let (inputs, _arg_path, _error_path) =
        build_function_inputs(&input_json, blob_dir.path()).unwrap();

    let allocation_id = "test-alloc-1";
    let request_id = "req-1";
    let function_call_id = "call-1";

    // --- Create allocation ---
    let create_req = CreateAllocationRequest {
        allocation: Some(function_executor_pb::Allocation {
            request_id: Some(request_id.to_string()),
            function_call_id: Some(function_call_id.to_string()),
            allocation_id: Some(allocation_id.to_string()),
            inputs: Some(inputs),
            result: None,
        }),
    };
    fe.client.create_allocation(create_req).await.unwrap();

    // --- Watch allocation state ---
    let mut stream = fe
        .client
        .watch_allocation_state(allocation_id)
        .await
        .unwrap();

    let state_timeout = Duration::from_secs(30);
    let mut seen_blob_ids: HashSet<String> = HashSet::new();
    let mut output_blob_path: Option<PathBuf> = None;

    // --- State machine loop ---
    loop {
        let state = next_state(&mut stream, state_timeout).await.unwrap();

        // Handle output blob requests
        for blob_req in &state.output_blob_requests {
            let blob_id = blob_req.id.as_deref().unwrap_or("");
            if !blob_id.is_empty() && seen_blob_ids.insert(blob_id.to_string()) {
                let size = blob_req.size.unwrap_or(0);
                eprintln!("Output blob request: id={}, size={}", blob_id, size);

                // Create writable blob for FE output
                let (blob, path) =
                    create_writable_file_blob(blob_id, size, blob_dir.path()).unwrap();
                output_blob_path = Some(path);

                let status = proto_api::google_rpc::Status {
                    code: 0, // OK
                    message: String::new(),
                    details: vec![],
                };

                let update = AllocationUpdate {
                    allocation_id: Some(allocation_id.to_string()),
                    update: Some(function_executor_pb::allocation_update::Update::OutputBlob(
                        AllocationOutputBlob {
                            status: Some(status),
                            blob: Some(blob),
                        },
                    )),
                };
                fe.client.send_allocation_update(update).await.unwrap();
            }
        }

        // Check for final result
        if let Some(ref result) = state.result {
            eprintln!("Allocation result: {:?}", result);

            assert_eq!(
                result.outcome_code(),
                function_executor_pb::AllocationOutcomeCode::Success,
                "Expected allocation success, got {:?}",
                result.outcome_code()
            );

            // The result should have a value output (the return value)
            match &result.outputs {
                Some(function_executor_pb::allocation_result::Outputs::Value(so)) => {
                    eprintln!("Result value SO: {:?}", so.manifest);
                    // The output was written to the output blob file
                    if let Some(ref path) = output_blob_path {
                        let output_bytes = std::fs::read(path).unwrap();
                        eprintln!(
                            "Output blob ({} bytes): {:?}",
                            output_bytes.len(),
                            String::from_utf8_lossy(&output_bytes)
                        );

                        // The output should contain the JSON-serialized return
                        // value. For `hello_world("World")` → `"Hello, World!"`
                        // The output has metadata prefix + JSON data.
                        // We check that the output contains the expected string.
                        let output_str = String::from_utf8_lossy(&output_bytes);
                        assert!(
                            output_str.contains("Hello, World!"),
                            "Expected output to contain 'Hello, World!', got: {}",
                            output_str
                        );
                    }
                }
                other => {
                    panic!("Expected value output, got: {:?}", other);
                }
            }

            break;
        }
    }

    // --- Cleanup ---
    fe.client.delete_allocation(allocation_id).await.unwrap();
    fe.kill().await;
}

/// Test 5: Kill FE process and verify health check detects death.
#[tokio::test]
async fn test_fe_health_check_detects_death() {
    let _ = tracing_subscriber::fmt::try_init();
    let fe_binary = skip_if_no_fe!();
    let python = skip_if_no_python!();

    let mut fe = FEProcess::spawn(&fe_binary).await.unwrap();

    let init_req = build_init_request(&python).await.unwrap();
    let resp = fe.client.initialize(init_req).await.unwrap();
    assert_eq!(
        resp.outcome_code(),
        function_executor_pb::InitializationOutcomeCode::Success,
    );

    // Health should be OK
    let health = fe.client.check_health().await.unwrap();
    assert_eq!(health.healthy, Some(true));

    // Kill the FE process
    fe.child.kill().await.unwrap();
    // Wait a moment for the process to die
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Health check should now fail
    let result = fe.client.check_health().await;
    assert!(
        result.is_err(),
        "Expected health check to fail after killing FE, got: {:?}",
        result
    );
}
