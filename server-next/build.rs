use std::error::Error;

use vergen::EmitBuilder;

fn main() -> Result<(), Box<dyn Error>> {
    //   let out_dir = std::path::Path::new("indexify_proto/src/");

    EmitBuilder::builder()
        .all_build()
        .all_cargo()
        .all_git()
        .all_rustc()
        .all_sysinfo()
        .emit()?;

    //    tonic_build::configure()
    //        .out_dir(out_dir)
    //        .type_attribute(
    //            "CreateContentRequest",
    //            "#[derive(serde::Deserialize, serde::Serialize)]",
    //        )
    //        .type_attribute(
    //            "ContentMetadata",
    //            "#[derive(serde::Deserialize, serde::Serialize)]",
    //        )
    //        .extern_path(".google.protobuf.Any", "::prost_wkt_types::Any")
    //        .extern_path(".google.protobuf.Timestamp",
    // "::prost_wkt_types::Timestamp")        .extern_path(".google.protobuf.
    // Value", "::prost_wkt_types::Value")        .compile(
    //            &["protos/coordinator_service.proto", "protos/raft.proto"],
    //            &["protos"],
    //        )
    //        .unwrap();

    Ok(())
}
