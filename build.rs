fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Protos are vendored verbatim from keryx-node (rpc/grpc/core/proto) so the
    // shim builds standalone, without depending on the node workspace.
    // Server codegen is only used by the in-process mock node in tests.
    tonic_build::configure()
        .build_server(true)
        .compile_protos(&["proto/messages.proto"], &["proto"])?;
    Ok(())
}
