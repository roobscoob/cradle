use std::sync::Arc;

use fctools::{runtime::tokio::TokioRuntime, vmm::installation::VmmInstallation};

mod agent_link;
mod cpu_template;
mod frame;
mod http;
mod materialize;
mod nix_build;
mod ops;
mod user_flake;

const FIRECRACKER_BIN: &str = env!("FIRECRACKER_BIN");
const JAILER_BIN: &str = env!("JAILER_BIN");
const SNAPSHOT_EDITOR_BIN: &str = env!("SNAPSHOT_EDITOR_BIN");

const BIND_ADDR: &str = "0.0.0.0:8080";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,fctools=debug,cradle=debug".into()),
        )
        .with_target(true)
        .init();

    let installation = VmmInstallation::new(FIRECRACKER_BIN, JAILER_BIN, SNAPSHOT_EDITOR_BIN);
    installation
        .verify("v1.15.1", &TokioRuntime)
        .await
        .expect("firecracker version verification failed");

    // Log the resolved firecracker binary + the actual --version banner so
    // when we suspect a firecracker bug we can compare against upstream
    // changelogs without re-deriving what's installed.
    tracing::info!("firecracker binary: {FIRECRACKER_BIN}");
    tracing::info!("jailer binary: {JAILER_BIN}");
    match tokio::process::Command::new(FIRECRACKER_BIN)
        .arg("--version")
        .output()
        .await
    {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            for line in stdout.lines() {
                tracing::info!("firecracker --version: {line}");
            }
        }
        Err(e) => tracing::warn!("failed to run firecracker --version: {e}"),
    }

    // Build the server-controlled artifacts (kernel + initrd + cmdline +
    // default storeDisk) once at startup. Slow on a cold cache, instant
    // on warm.
    tracing::info!("building server artifacts (kernel, initrd, cmdline, default-storeDisk)");
    let artifacts = nix_build::build_server_artifacts(None)
        .await
        .expect("build server artifacts");
    tracing::info!(
        "server artifacts ready: kernel={}, initrd={}, agent={}",
        artifacts.default_kernel.display(),
        artifacts.default_initrd.display(),
        artifacts.agent_static.display()
    );

    let frames = frame::FrameStore::new().expect("create FrameStore");
    let state = Arc::new(http::AppState {
        installation: Arc::new(installation),
        frames,
        artifacts: Arc::new(artifacts),
    });

    let app = http::router(state);

    let listener = tokio::net::TcpListener::bind(BIND_ADDR)
        .await
        .unwrap_or_else(|e| panic!("bind {BIND_ADDR}: {e}"));

    tracing::info!("cradle listening on http://{BIND_ADDR}");
    axum::serve(listener, app).await.expect("axum serve");
}
