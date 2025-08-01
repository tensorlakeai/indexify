fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("../proto/gateway.proto")?;
    
    tonic_build::configure()
        .build_server(true)
        .compile_protos(
            &["../proto/opentelemetry/proto/collector/logs/v1/logs_service.proto"],
            &["../proto"],
        )?;
    
    Ok(())
}
