// build.rs
fn main() {
    for var in [
        "FIRECRACKER_BIN",
        "JAILER_BIN",
        "SNAPSHOT_EDITOR_BIN",
        "CRADLE_GUEST_FLAKE",
    ] {
        println!("cargo:rerun-if-env-changed={var}");
        if let Ok(value) = std::env::var(var) {
            println!("cargo:rustc-env={var}={value}");
        }
    }
}
