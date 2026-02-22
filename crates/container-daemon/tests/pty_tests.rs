//! Integration tests for the PTY WebSocket feature.
//!
//! These tests require real PTY allocation which only works on Linux
//! (pty-process 0.4.0 calls TIOCSWINSZ on the master fd, which returns
//! ENOTTY on macOS). Tests are gated with `#[cfg(target_os = "linux")]`.
//!
//! Non-PTY tests (like the 404 test) work on all platforms.

use std::time::Instant;

use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use indexify_container_daemon::{
    file_manager::FileManager,
    http_models::ErrorResponse,
    http_server::{AppState, build_router},
    process_manager::ProcessManager,
    pty_manager::PtyManager,
};
use tower::ServiceExt;

/// Create an AppState with real managers suitable for testing.
fn test_state() -> (AppState, tokio_util::sync::CancellationToken) {
    let cancel = tokio_util::sync::CancellationToken::new();
    let tmp = tempfile::TempDir::new().unwrap();
    // Leak the TempDir so it stays alive
    let tmp_path = tmp.path().to_path_buf();
    std::mem::forget(tmp);
    let state = AppState {
        process_manager: ProcessManager::new(tmp_path),
        file_manager: FileManager::new(),
        pty_manager: PtyManager::new(cancel.clone()),
        start_time: Instant::now(),
    };
    (state, cancel)
}

// ============================================================================
// Platform-independent tests
// ============================================================================

#[tokio::test]
async fn test_get_nonexistent_session_returns_404() {
    let (state, _cancel) = test_state();
    let app = build_router(state);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/pty/nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let err: ErrorResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(err.code.as_deref(), Some("NOT_FOUND"));
}

// ============================================================================
// Linux-only tests (require PTY allocation via pty-process)
// ============================================================================

#[cfg(target_os = "linux")]
mod linux_tests {
    use bytes::Bytes;
    use futures::SinkExt;
    use indexify_container_daemon::http_models::{
        CreatePtySessionRequest,
        CreatePtySessionResponse,
        ListPtySessionsResponse,
        PtySessionInfo,
    };
    use tokio::net::TcpListener;
    use tokio_tungstenite::{connect_async, tungstenite};

    use super::*;

    /// Helper: create a PTY session via the REST API using oneshot.
    async fn create_session_via_api(
        app: axum::Router,
        cmd: &str,
        args: &[&str],
    ) -> (axum::http::StatusCode, String) {
        let req = CreatePtySessionRequest {
            command: cmd.to_string(),
            args: Some(args.iter().map(|s| s.to_string()).collect()),
            env: None,
            working_dir: None,
            rows: None,
            cols: None,
        };
        let body = serde_json::to_string(&req).unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/pty")
                    .header("Content-Type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        (status, String::from_utf8_lossy(&body).to_string())
    }

    /// Spawn a real TCP server for WebSocket tests. Returns (port,
    /// cancel_token).
    async fn spawn_test_server() -> (u16, tokio_util::sync::CancellationToken) {
        let (state, cancel) = test_state();
        let app = build_router(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let cancel_clone = cancel.clone();
        tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    cancel_clone.cancelled().await;
                })
                .await
                .unwrap();
        });
        (port, cancel)
    }

    /// Helper: create a PTY session on a live server using hyper.
    async fn create_session_on_server(
        port: u16,
        cmd: &str,
        args: &[&str],
    ) -> CreatePtySessionResponse {
        let req = CreatePtySessionRequest {
            command: cmd.to_string(),
            args: Some(args.iter().map(|s| s.to_string()).collect()),
            env: None,
            working_dir: None,
            rows: None,
            cols: None,
        };
        let body = serde_json::to_string(&req).unwrap();

        let stream = tokio::net::TcpStream::connect(format!("127.0.0.1:{}", port))
            .await
            .unwrap();
        let (mut sender, conn) =
            hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream))
                .await
                .unwrap();
        tokio::spawn(conn);

        let request = hyper::Request::builder()
            .method("POST")
            .uri("/api/v1/pty")
            .header("Content-Type", "application/json")
            .header("Host", format!("127.0.0.1:{}", port))
            .body(http_body_util::Full::new(Bytes::from(body)))
            .unwrap();

        let response = sender.send_request(request).await.unwrap();
        assert_eq!(response.status(), 201);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&body).unwrap()
    }

    // REST API tests

    #[tokio::test]
    async fn test_create_pty_session_returns_201() {
        let (state, _cancel) = test_state();
        let app = build_router(state);
        let (status, body) = create_session_via_api(app, "echo", &["hello"]).await;
        assert_eq!(
            status,
            axum::http::StatusCode::CREATED,
            "Response body: {}",
            body
        );
        let resp: CreatePtySessionResponse = serde_json::from_str(&body).unwrap();
        assert!(!resp.session_id.is_empty());
        assert!(!resp.token.is_empty());
    }

    #[tokio::test]
    async fn test_list_sessions_includes_created() {
        let (state, _cancel) = test_state();
        let app = build_router(state);
        let (_, body) = create_session_via_api(app.clone(), "sleep", &["60"]).await;
        let created: CreatePtySessionResponse = serde_json::from_str(&body).unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/pty")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let list: ListPtySessionsResponse = serde_json::from_slice(&body).unwrap();
        assert!(
            list.sessions
                .iter()
                .any(|s| s.session_id == created.session_id)
        );
    }

    #[tokio::test]
    async fn test_get_session_returns_info_without_token() {
        let (state, _cancel) = test_state();
        let app = build_router(state);
        let (_, body) = create_session_via_api(app.clone(), "sleep", &["60"]).await;
        let created: CreatePtySessionResponse = serde_json::from_str(&body).unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/pty/{}", created.session_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let info: PtySessionInfo = serde_json::from_slice(&body).unwrap();
        assert_eq!(info.session_id, created.session_id);
        assert!(info.token.is_none());
        assert!(info.is_alive);
    }

    #[tokio::test]
    async fn test_kill_session_returns_204() {
        let (state, _cancel) = test_state();
        let app = build_router(state);
        let (_, body) = create_session_via_api(app.clone(), "sleep", &["60"]).await;
        let created: CreatePtySessionResponse = serde_json::from_str(&body).unwrap();
        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri(format!("/api/v1/pty/{}", created.session_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_session_limit_returns_429() {
        let (state, _cancel) = test_state();
        let app = build_router(state);
        for i in 0..64 {
            let (status, _) =
                create_session_via_api(app.clone(), "sleep", &[&format!("{}", 100 + i)]).await;
            assert_eq!(
                status,
                axum::http::StatusCode::CREATED,
                "Session {} should succeed",
                i
            );
        }
        let (status, body) = create_session_via_api(app.clone(), "sleep", &["999"]).await;
        assert_eq!(status, axum::http::StatusCode::TOO_MANY_REQUESTS);
        let err: ErrorResponse = serde_json::from_str(&body).unwrap();
        assert_eq!(err.code.as_deref(), Some("TOO_MANY_SESSIONS"));
    }

    // WebSocket tests

    #[tokio::test]
    async fn test_ws_invalid_token_rejected() {
        let (port, cancel) = spawn_test_server().await;
        let created = create_session_on_server(port, "sleep", &["60"]).await;
        let url = format!(
            "ws://127.0.0.1:{}/api/v1/pty/{}/ws?token=invalid_bad_token",
            port, created.session_id
        );
        let result = connect_async(&url).await;
        assert!(result.is_err(), "Expected connection to be rejected");
        cancel.cancel();
    }

    #[tokio::test]
    async fn test_ws_valid_token_connects_and_receives_output() {
        let (port, cancel) = spawn_test_server().await;
        let created = create_session_on_server(port, "echo", &["hello from pty"]).await;
        let url = format!(
            "ws://127.0.0.1:{}/api/v1/pty/{}/ws?token={}",
            port, created.session_id, created.token
        );
        let (mut ws, _) = connect_async(&url).await.expect("WebSocket connect failed");

        // Send READY signal (0x02)
        ws.send(tungstenite::Message::Binary(Bytes::from_static(&[0x02])))
            .await
            .unwrap();

        let mut got_data = false;
        let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(10);
        loop {
            let msg = tokio::time::timeout_at(deadline, futures::StreamExt::next(&mut ws)).await;
            match msg {
                Ok(Some(Ok(tungstenite::Message::Binary(data)))) => {
                    if !data.is_empty() && data[0] == 0x00 {
                        got_data = true;
                    }
                }
                Ok(Some(Ok(tungstenite::Message::Close(frame)))) => {
                    if let Some(f) = frame {
                        assert_eq!(
                            f.code,
                            tungstenite::protocol::frame::coding::CloseCode::Normal
                        );
                        assert!(
                            f.reason.starts_with("exit:"),
                            "Expected exit:N, got: {}",
                            f.reason
                        );
                    }
                    break;
                }
                Ok(None) => break,
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => {
                    if matches!(
                        e,
                        tungstenite::Error::Protocol(
                            tungstenite::error::ProtocolError::ResetWithoutClosingHandshake
                        )
                    ) {
                        break;
                    }
                    panic!("WebSocket error: {}", e);
                }
                Err(_) => {
                    panic!("Timed out waiting for WebSocket messages");
                }
            }
        }
        assert!(got_data, "Expected to receive PTY output data");
        cancel.cancel();
    }
}
