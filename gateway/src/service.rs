use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::info;
use prost_types::Any;

use crate::gateway::{
    gateway_service_server::GatewayService,
    CreateGateRequest, Gate as GateProto, GetGateRequest, DeleteGateRequest,
    ListGatesRequest, ListGatesResponse, Collector,
};
use crate::gate_server::GateServer;

#[derive(Debug)]
pub struct GatewayServiceImpl {
    gates: RwLock<HashMap<String, GateServer>>,
}

impl GatewayServiceImpl {
    fn validate_gate_services(services: &[Any]) -> Result<(), Status> {
        if services.is_empty() {
            return Err(Status::invalid_argument("Gate must have at least one service"));
        }
        
        let mut collector_count = 0;
        let collector_type_url = "type.googleapis.com/gateway.Collector";
        
        for service in services {
            if service.type_url == collector_type_url {
                collector_count += 1;
                
                // Try to decode the Collector to validate it
                match prost::Message::decode(&*service.value) {
                    Ok(collector) => {
                        let collector: Collector = collector;
                        if collector.namespace.is_empty() {
                            return Err(Status::invalid_argument("Collector namespace cannot be empty"));
                        }
                        if collector.destination.is_empty() {
                            return Err(Status::invalid_argument("Collector destination cannot be empty"));
                        }
                    }
                    Err(_) => {
                        return Err(Status::invalid_argument("Invalid Collector service data"));
                    }
                }
            } else {
                return Err(Status::invalid_argument(format!("Unsupported service type: {}", service.type_url)));
            }
        }
        
        if collector_count == 0 {
            return Err(Status::invalid_argument("Gate must have at least one Collector service"));
        }
        
        if collector_count > 1 {
            return Err(Status::invalid_argument("Gate can only have one Collector service"));
        }
        
        Ok(())
    }
}

impl Default for GatewayServiceImpl {
    fn default() -> Self {
        Self {
            gates: RwLock::new(HashMap::new()),
        }
    }
}

#[tonic::async_trait]
impl GatewayService for GatewayServiceImpl {
    async fn create_gate(
        &self,
        request: Request<CreateGateRequest>,
    ) -> Result<Response<GateProto>, Status> {
        let req = request.into_inner();
        
        if req.gate_id.is_empty() {
            return Err(Status::invalid_argument("gate_id cannot be empty"));
        }
        
        let gate = req.gate.ok_or_else(|| {
            Status::invalid_argument("gate field is required")
        })?;
        
        if gate.path.is_empty() {
            return Err(Status::invalid_argument("gate path cannot be empty"));
        }
        
        // Validate services
        Self::validate_gate_services(&gate.services)?;
        
        let mut gates = self.gates.write().await;
        
        if gates.contains_key(&req.gate_id) {
            return Err(Status::already_exists(format!("gate with id '{}' already exists", req.gate_id)));
        }
        
        let new_proto = GateProto {
            id: req.gate_id.clone(),
            name: format!("/gates/{}", req.gate_id),
            path: gate.path.clone(),
            services: gate.services,
        };
        
        // Create the GateServer - this will start the socket and initialize everything
        let gate_server = GateServer::new(new_proto.clone()).await?;
        
        gates.insert(req.gate_id, gate_server);
        
        Ok(Response::new(new_proto))
    }

    async fn get_gate(
        &self,
        request: Request<GetGateRequest>,
    ) -> Result<Response<GateProto>, Status> {
        let req = request.into_inner();
        
        if req.id.is_empty() {
            return Err(Status::invalid_argument("gate id cannot be empty"));
        }
        
        let gates = self.gates.read().await;
        
        match gates.get(&req.id) {
            Some(gate_server) => {
                let gate_guard = gate_server.gate.read().await;
                Ok(Response::new(gate_guard.proto.clone()))
            }
            None => Err(Status::not_found(format!("gate with id '{}' not found", req.id))),
        }
    }

    async fn delete_gate(
        &self,
        request: Request<DeleteGateRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        
        if req.id.is_empty() {
            return Err(Status::invalid_argument("gate id cannot be empty"));
        }
        
        let mut gates = self.gates.write().await;
        
        match gates.remove(&req.id) {
            Some(mut gate_server) => {
                info!("Deleting gate '{}'", req.id);
                // Explicitly shutdown the gate server and wait for completion
                gate_server.shutdown().await?;
                Ok(Response::new(()))
            },
            None => Err(Status::not_found(format!("gate with id '{}' not found", req.id))),
        }
    }

    async fn list_gates(
        &self,
        request: Request<ListGatesRequest>,
    ) -> Result<Response<ListGatesResponse>, Status> {
        let req = request.into_inner();
        
        let gates = self.gates.read().await;
        
        let mut gate_list = Vec::new();
        for gate_server in gates.values() {
            let gate_guard = gate_server.gate.read().await;
            gate_list.push(gate_guard.proto.clone());
        }
        gate_list.sort_by(|a, b| a.id.cmp(&b.id));
        
        if req.limit > 0 {
            let limit = req.limit as usize;
            if gate_list.len() > limit {
                gate_list.truncate(limit);
            }
        }
        
        let response = ListGatesResponse {
            gates: gate_list,
        };
        
        Ok(Response::new(response))
    }
}
