
pub mod config;
pub mod service;
pub mod collector;
pub mod gate_server;

pub mod gateway {
    tonic::include_proto!("gateway");
}

mod otel_protos {
    pub mod collector {
        pub mod logs {
            pub mod v1 {
                tonic::include_proto!("opentelemetry.proto.collector.logs.v1");
            }
        }
    }
    pub mod common {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.common.v1");
        }
    }
    pub mod logs {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.logs.v1");
        }
    }
    pub mod resource {
        pub mod v1 {
            tonic::include_proto!("opentelemetry.proto.resource.v1");
        }
    }
}

use gateway::{
    gateway_service_server::GatewayServiceServer,
    Gate as GateProto,
};



#[derive(Debug)]
pub struct Gate {
    pub proto: GateProto,
}



pub fn create_service() -> GatewayServiceServer<service::GatewayServiceImpl> {
    GatewayServiceServer::new(service::GatewayServiceImpl::default())
}
