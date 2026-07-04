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

/// Default bind address; override with `CRADLE_BIND_ADDR` to pin the API to
/// one interface (e.g. a tailnet IP on a host that must not expose it to the
/// LAN).
const BIND_ADDR_DEFAULT: &str = "0.0.0.0:8080";

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

    // The central (durable) store: REQUIRED. A frame id is a durability
    // promise — there is deliberately no local-only mode to accidentally
    // develop against (see store::central).
    let central_path = std::env::var("CRADLE_CENTRAL_STORE").expect(
        "CRADLE_CENTRAL_STORE must point at the central store directory \
         (the durable tier frame ids are committed to before they're returned)",
    );
    let central: std::sync::Arc<dyn store::ContentStore> = std::sync::Arc::new(
        store::DirStore::open(&central_path)
            .unwrap_or_else(|e| panic!("open central store {central_path}: {e}")),
    );
    match central.list_frames().await {
        Ok(ids) => tracing::info!(
            "central store at {central_path}: {} frame(s) available",
            ids.len()
        ),
        Err(e) => panic!("central store at {central_path} unusable: {e}"),
    }

    let frames = frame::FrameStore::new(central).expect("create FrameStore");
    // Refuse to run on a filesystem that can't report holes: Diff-snapshot
    // dirty-page extraction would silently zero clean pages in every child
    // frame (see ops::probe_store_fs / ops::read_dirty_pages). Also warns
    // once if reflink is unavailable (correct but O(image) captures).
    ops::probe_store_fs(frames.root()).expect("frame store filesystem check");
    let state = Arc::new(http::AppState {
        installation: Arc::new(installation),
        frames,
        artifacts: Arc::new(artifacts),
    });

    let app = http::router(state);

    let bind_addr =
        std::env::var("CRADLE_BIND_ADDR").unwrap_or_else(|_| BIND_ADDR_DEFAULT.into());
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .unwrap_or_else(|e| panic!("bind {bind_addr}: {e}"));

    tracing::info!("cradle listening on http://{bind_addr}");
    axum::serve(listener, app).await.expect("axum serve");
}
