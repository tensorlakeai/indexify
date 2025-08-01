use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::Status;
use tonic::transport::Server;
use tracing::{info, warn, error};
use tokio::task::JoinHandle;
use prost_types::Any;

use crate::otel_protos::collector::logs::v1::{
    logs_service_server::LogsServiceServer,
};
use crate::gateway::Gate as GateProto;
use crate::Gate;

type SocketShutdownHandle = oneshot::Sender<()>;

/// Add services to the server based on the gate's service configuration
fn configure_services_for_gate(
    gate: Arc<RwLock<Gate>>,
    services: &[Any],
) -> Result<tonic::transport::server::Router, Status> {
    let mut server_router: Option<tonic::transport::server::Router> = None;
    
    for service in services {
        match service.type_url.as_str() {
            "type.googleapis.com/gateway.Collector" => {
                let logs_service = crate::collector::CollectorLogsServiceImpl {
                    gate: Arc::clone(&gate),
                };
                server_router = Some(match server_router {
                    None => Server::builder().add_service(LogsServiceServer::new(logs_service)),
                    Some(router) => router.add_service(LogsServiceServer::new(logs_service)),
                });
                info!("Added Collector service to gate");
            }
            unknown_type => {
                return Err(Status::invalid_argument(format!("Unsupported service type: {}", unknown_type)));
            }
        }
    }
    
    server_router.ok_or_else(|| Status::internal("No services configured for gate"))
}

#[derive(Debug)]
pub struct GateServer {
    pub gate: Arc<RwLock<Gate>>,
    shutdown_handle: Option<SocketShutdownHandle>,
    server_task: Option<JoinHandle<()>>,
}

impl GateServer {
    pub async fn new(gate_proto: GateProto) -> Result<Self, Status> {
        let gate = Arc::new(RwLock::new(Gate {
            proto: gate_proto,
        }));
        
        let (shutdown_handle, server_task) = start_gate_socket(Arc::clone(&gate)).await?;
        
        Ok(GateServer {
            gate,
            shutdown_handle: Some(shutdown_handle),
            server_task: Some(server_task),
        })
    }

    pub async fn shutdown(&mut self) -> Result<(), Status> {
        let gate_id = if let Ok(gate_guard) = self.gate.try_read() {
            gate_guard.proto.id.clone()
        } else {
            "unknown".to_string()
        };

        info!("Shutting down GateServer for gate '{}'", gate_id);

        // Send shutdown signal
        if let Some(shutdown_handle) = self.shutdown_handle.take() {
            if shutdown_handle.send(()).is_err() {
                warn!("Failed to send shutdown signal to gate '{}' socket server", gate_id);
            }
        }

        // Wait for the server task to complete
        if let Some(server_task) = self.server_task.take() {
            match server_task.await {
                Ok(()) => info!("Gate '{}' server task completed successfully", gate_id),
                Err(e) => error!("Gate '{}' server task failed: {}", gate_id, e),
            }
        }

        Ok(())
    }
}

impl Drop for GateServer {
    fn drop(&mut self) {
        // Only act as fallback if shutdown() wasn't called
        if self.shutdown_handle.is_some() || self.server_task.is_some() {
            let gate_id = if let Ok(gate_guard) = self.gate.try_read() {
                gate_guard.proto.id.clone()
            } else {
                "unknown".to_string()
            };
            
            warn!("GateServer for gate '{}' being dropped without explicit shutdown() call", gate_id);
            
            // Send shutdown signal if we still have it
            if let Some(shutdown_handle) = self.shutdown_handle.take() {
                if shutdown_handle.send(()).is_err() {
                    warn!("Failed to send shutdown signal to gate '{}' socket server in Drop", gate_id);
                }
            }
            
            // Let the server task terminate gracefully when it processes the shutdown signal
            if let Some(_server_task) = self.server_task.take() {
                info!("Server task for gate '{}' will terminate asynchronously", gate_id);
            }
        }
    }
}

async fn start_gate_socket(gate: Arc<RwLock<Gate>>) -> Result<(SocketShutdownHandle, JoinHandle<()>), Status> {
    let gate_guard = gate.read().await;
    let gate_id = gate_guard.proto.id.clone();
    let socket_path = gate_guard.proto.path.clone();
    drop(gate_guard); // Release the read lock early
    
    // Remove existing socket file if it exists
    if std::path::Path::new(&socket_path).exists() {
        std::fs::remove_file(&socket_path).map_err(|e| {
            Status::internal(format!("Failed to remove existing socket file '{socket_path}': {e}"))
        })?;
    }

    let listener = UnixListener::bind(&socket_path).map_err(|e| {
        Status::internal(format!("Failed to bind to socket '{socket_path}': {e}"))
    })?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    
    info!("Starting GRPC server for gate '{}' on socket: {}", gate_id, socket_path);
    
    // Get the gate's services and build the server
    let gate_guard = gate.read().await;
    let services = gate_guard.proto.services.clone();
    drop(gate_guard);
    
    // Build the server with all configured services
    let server_router = configure_services_for_gate(
        Arc::clone(&gate),
        &services,
    )?;
    
    // Spawn a task to run the GRPC server
    let gate_id_clone = gate_id.clone();
    let socket_path_clone = socket_path.clone();
    let server_task = tokio::spawn(async move {
        let uds_stream = UnixListenerStream::new(listener);
        
        // Serve all configured services on the socket
        let result = tokio::select! {
            result = server_router.serve_with_incoming(uds_stream) => result,
            _ = shutdown_rx => {
                info!("Received shutdown signal for gate '{}'", gate_id_clone);
                Ok(())
            }
        };
            
        match result {
            Ok(_) => info!("Gate '{}' socket server stopped normally", gate_id_clone),
            Err(e) => error!("Gate '{}' socket server error: {}", gate_id_clone, e),
        }
        
        // Clean up socket file
        if std::path::Path::new(&socket_path_clone).exists() {
            if let Err(e) = std::fs::remove_file(&socket_path_clone) {
                warn!("Failed to remove socket file '{}': {}", socket_path_clone, e);
            }
        }
    });

    Ok((shutdown_tx, server_task))
}