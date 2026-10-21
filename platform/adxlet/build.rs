fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    // Cargo runs build scripts from the package root. Keep dependency paths
    // relative so a reused build script cannot watch an older copied tree.
    let directory = std::path::PathBuf::from("../../third_party/sandboxd");
    let file = directory.join("sandbox-api.proto");
    println!("cargo:rerun-if-changed={}", file.display());
    tonic_build::configure().compile_protos(&[file], &[directory])?;
    Ok(())
}
