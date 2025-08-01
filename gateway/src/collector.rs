use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use tracing::info;

use crate::otel_protos::collector::logs::v1::{
    logs_service_server::LogsService,
    ExportLogsServiceRequest, ExportLogsServiceResponse, ExportLogsPartialSuccess,
};
use crate::gateway::Collector;
use crate::Gate;

#[derive(Debug, Clone)]
pub struct CollectorLogsServiceImpl {
    pub gate: Arc<RwLock<Gate>>,
}

#[tonic::async_trait]
impl LogsService for CollectorLogsServiceImpl {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let req = request.into_inner();
        
        let gate_guard = self.gate.read().await;
        let gate_id = &gate_guard.proto.id;
        
        // Extract collector info from the gate's services
        let collector = gate_guard.proto.services.iter()
            .find(|service| service.type_url == "type.googleapis.com/gateway.Collector")
            .and_then(|service| prost::Message::decode(&*service.value).ok())
            .ok_or_else(|| Status::internal("Failed to decode Collector service"))?;
        let collector: Collector = collector;
        
        info!("Received logs export request for gate '{}' (namespace: '{}', destination: '{}')", 
              gate_id, collector.namespace, collector.destination);
        
        // Write received log records to stdout
        for (i, resource_logs) in req.resource_logs.iter().enumerate() {
            println!("=== Resource Logs {} for Gate '{}' ===", i + 1, gate_id);
            
            if let Some(resource) = &resource_logs.resource {
                println!("Resource attributes:");
                for attr in &resource.attributes {
                    println!("  {}: {:?}", attr.key, attr.value);
                }
            }
            
            for scope_logs in &resource_logs.scope_logs {
                if let Some(scope) = &scope_logs.scope {
                    println!("Scope: {} (version: {})", scope.name, scope.version);
                }
                
                for (j, log_record) in scope_logs.log_records.iter().enumerate() {
                    println!("  Log Record {}:", j + 1);
                    println!("    Timestamp: {}", log_record.time_unix_nano);
                    println!("    Severity: {:?}", log_record.severity_number());
                    println!("    Severity Text: {}", log_record.severity_text);
                    
                    if let Some(body) = &log_record.body {
                        println!("    Body: {body:?}");
                    }
                    
                    if !log_record.attributes.is_empty() {
                        println!("    Attributes:");
                        for attr in &log_record.attributes {
                            println!("      {}: {:?}", attr.key, attr.value);
                        }
                    }
                    
                    if !log_record.trace_id.is_empty() {
                        println!("    Trace ID: {:?}", log_record.trace_id);
                    }
                    
                    if !log_record.span_id.is_empty() {
                        println!("    Span ID: {:?}", log_record.span_id);
                    }
                }
            }
            println!();
        }
        
        let response = ExportLogsServiceResponse {
            partial_success: Some(ExportLogsPartialSuccess {
                rejected_log_records: 0,
                error_message: String::new(),
            }),
        };
        
        Ok(Response::new(response))
    }
}