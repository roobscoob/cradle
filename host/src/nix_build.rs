use std::{io, path::PathBuf, process::Stdio};

use tokio::{io::BufReader, process::Command, sync::mpsc};

const GUEST_FLAKE: &str = env!("CRADLE_GUEST_FLAKE");

/// Server-controlled artifacts resolved once at host startup.
///
/// The four `default_*` paths come from the `guest` nixosSystem and form a
/// coherent boot bundle (kernel + initrd + storeDisk + cmdline). They MUST
/// stay paired — microvm.nix's `kernelParams` embed `init=/nix/store/<this-
/// system>/init`, which only resolves inside the matching storeDisk's closure.
///
/// `agent_static` is the path to the statically-linked agent derivation. It's
/// passed via `specialArgs.cradleAgent` into per-request user-flake wrappers
/// so the agent ends up in the user's initrd too.
///
/// User-uploaded flakes produce their own 4-path bundle from the wrapper-flake
/// build in [`crate::user_flake`].
#[derive(Debug, Clone)]
pub struct ServerArtifacts {
    pub default_kernel: PathBuf,
    pub default_initrd: PathBuf,
    pub default_store_disk: PathBuf,
    pub default_cmdline: PathBuf,
    pub agent_static: PathBuf,
    /// Path to the statically-linked `pty-bridge` derivation. Threaded via
    /// `specialArgs.cradlePtyBridge` into per-request user-flake wrappers
    /// so the bridge binary ends up in the user's image too.
    pub pty_bridge_static: PathBuf,
}

/// Build the default boot bundle + agent-static from the workspace flake.
/// Run once at host startup; the resulting paths live in `/nix/store` and
/// are stable for the host's lifetime.
pub async fn build_server_artifacts(
    progress: Option<mpsc::Sender<String>>,
) -> io::Result<ServerArtifacts> {
    let attrs = [
        format!("{GUEST_FLAKE}#default-kernel"),
        format!("{GUEST_FLAKE}#default-initrd"),
        format!("{GUEST_FLAKE}#default-storeDisk"),
        format!("{GUEST_FLAKE}#default-cmdline"),
        format!("{GUEST_FLAKE}#agent-static"),
        format!("{GUEST_FLAKE}#pty-bridge-static"),
    ];
    let paths = run_nix_build(&attrs, progress).await?;
    Ok(ServerArtifacts {
        default_kernel: paths[0].clone(),
        default_initrd: paths[1].clone(),
        default_store_disk: paths[2].clone(),
        default_cmdline: paths[3].clone(),
        agent_static: paths[4].clone(),
        pty_bridge_static: paths[5].clone(),
    })
}

/// Build one or more flake outputs and return their `out` paths in order.
///
/// Shared by [`build_server_artifacts`] (startup) and the user-flake
/// wrapper build (per request) — same `nix build --json` parsing, same
/// stderr progress streaming.
pub(crate) async fn run_nix_build(
    attrs: &[String],
    progress: Option<mpsc::Sender<String>>,
) -> io::Result<Vec<PathBuf>> {
    let mut args: Vec<&str> = vec![
        "build",
        "--no-link",
        "--no-write-lock-file",
        "--log-format",
        "internal-json",
        "-v",
        "--json",
    ];
    for attr in attrs {
        args.push(attr);
    }

    let mut child = Command::new("nix")
        .args(&args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        // Cancellation story: builds have no timeout by design (arbitrarily
        // long is fine), but they must be cancellable — build_frame races
        // this future against the client's cancel signal, and dropping it
        // must take the nix process down with it.
        .kill_on_drop(true)
        .spawn()?;

    let stderr = child.stderr.take().expect("stderr piped");
    let stderr_task = tokio::spawn(async move {
        // Lossy line reads: nix stderr is not guaranteed UTF-8, and a strict
        // `lines()` loop would silently stop streaming at the first raw byte.
        let mut stderr = BufReader::new(stderr);
        while let Some(line) = crate::ops::next_line_lossy(&mut stderr).await {
            if let Some(text) = extract_progress(&line) {
                eprintln!("\x1b[35m[nix]\x1b[0m {text}");
                if let Some(tx) = progress.as_ref() {
                    let _ = tx.send(text).await;
                }
            }
        }
    });

    let output = child.wait_with_output().await?;
    let _ = stderr_task.await;

    if !output.status.success() {
        return Err(io::Error::other(format!(
            "nix build failed with status {}",
            output.status
        )));
    }

    parse_paths(&output.stdout, attrs.len())
}

/// Pull a human-readable line out of one stderr line of `nix --log-format
/// internal-json`. Returns `None` to drop noise.
pub(crate) fn extract_progress(line: &str) -> Option<String> {
    let Some(rest) = line.strip_prefix("@nix ") else {
        // Non-event stderr (warnings, raw errors) — surface verbatim.
        return Some(line.to_string());
    };
    let event: serde_json::Value = serde_json::from_str(rest).ok()?;
    let action = event.get("action")?.as_str()?;
    match action {
        "start" => {
            let text = event.get("text")?.as_str()?;
            if text.is_empty() { None } else { Some(text.to_string()) }
        }
        "msg" => event.get("msg")?.as_str().map(str::to_string),
        _ => None,
    }
}

fn parse_paths(stdout: &[u8], expected_len: usize) -> io::Result<Vec<PathBuf>> {
    let results: serde_json::Value = serde_json::from_slice(stdout).map_err(|e| {
        io::Error::other(format!(
            "failed to parse nix build --json output: {e}\nraw stdout:\n{}",
            String::from_utf8_lossy(stdout)
        ))
    })?;
    let arr = results.as_array().ok_or_else(|| {
        io::Error::other(format!(
            "nix --json output not an array. raw stdout:\n{}",
            String::from_utf8_lossy(stdout)
        ))
    })?;
    if arr.len() != expected_len {
        return Err(io::Error::other(format!(
            "expected {expected_len} nix outputs, got {}",
            arr.len()
        )));
    }
    arr.iter()
        .enumerate()
        .map(|(i, v)| {
            v["outputs"]["out"]
                .as_str()
                .map(PathBuf::from)
                .ok_or_else(|| io::Error::other(format!("missing 'out' for index {i}")))
        })
        .collect()
}
