use std::path::Path;

use prost_build::Config;

fn main() -> Result<(), String> {
    use std::io::Write;

    let out = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());

    println!("{:?}", out);

    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");

    let version = rustc_version::version().unwrap();
    println!("cargo:rustc-env=RUSTC_VERSION={version}");

    let path = "src/proto.rs";

    // We don't include the proto files in releases so that downstreams
    // do not need to have PROTOC included
    if Path::new("proto/logical_plan.proto").exists() {
        println!("cargo:rerun-if-changed=proto/logical_plan.proto");
        println!("cargo:rerun-if-changed=proto/physical_plan.proto");

        let mut config = Config::new();

        config.protoc_arg("--experimental_allow_proto3_optional");

        tonic_build::configure()
            .compile_with_config(
                config,
                &["proto/logical_plan.proto", "proto/physical_plan.proto"],
                &["proto"],
            )
            .map_err(|e| format!("protobuf compilation failed: {e}"))?;

        let generated_source_path = out.join("_.rs");

        println!("{:?}", generated_source_path);

        let code = std::fs::read_to_string(generated_source_path).unwrap();
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .unwrap();

        file.write_all(code.as_str().as_ref()).unwrap();
    }

    Ok(())
}
