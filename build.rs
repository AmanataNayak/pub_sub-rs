fn main() -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    tonic_build::configure()
        .bytes(&["."])
        .build_server(true)
        .compile_protos(&["proto/pubsub.proto"], &["proto"])?;

    Ok(())
}