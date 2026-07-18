fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc");
    unsafe { std::env::set_var("PROTOC", protoc) };
    prost_build::Config::new()
        .compile_protos(
            &[
                "../../proto/inference/v1/inference.proto",
                "../../proto/inference/internal/v1/streams.proto",
            ],
            &["../../proto"],
        )
        .expect("compile inference protobufs");
}
