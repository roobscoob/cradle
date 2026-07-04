//! Per-operation VM lifecycle: build a fresh frame, or step a parent frame.
//!
//! Each op runs Firecracker under the **jailer**, so it gets its own chroot
//! at `<store.root>/jails/firecracker/<jail_id>/root/`. The vsock UDS lives at
//! the jail-internal path `/vsock.sock` (so the snapshot embeds that stable
//! path), but the actual file is at `<jail_path>/vsock.sock` on the host —
//! unique per op. That's how we get same-parent step concurrency without
//! UDS-path collisions.
//!
//! After capturing the snapshot, the VM is hard-killed — the snapshot already
//! captured everything; we don't want it to burn another cycle.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use agent_protocol::{AgentMessage, ExitResult, HostMessage, VSOCK_GUEST_CID, VSOCK_HOST_PORT};
use fctools::{
    process_spawner::DirectProcessSpawner,
    runtime::tokio::TokioRuntime,
    vm::{
        Vm,
        api::VmApi,
        configuration::{InitMethod, VmConfiguration, VmConfigurationData},
        models::{
            BootSource, CreateSnapshot, Drive, LoadSnapshot, MachineConfiguration, MemoryBackend,
            MemoryBackendType, SnapshotType, VsockDevice,
        },
        shutdown::{VmShutdownAction, VmShutdownMethod},
    },
    vmm::{
        arguments::{VmmApiSocket, VmmArguments, VmmLogLevel, jailer::JailerArguments},
        executor::jailed::{JailedVmmExecutor, VirtualPathResolver, VirtualPathResolverError},
        id::VmmId,
        installation::VmmInstallation,
        ownership::VmmOwnershipModel,
        resource::{MovedResourceType, Resource, ResourceType, system::ResourceSystem},
    },
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::UnixListener,
    sync::{mpsc, oneshot},
};
use ulid::Ulid;

use store::{BlobSrc, Cas, ContentStore, Hash, MemTree};

use crate::{
    agent_link::AgentLink,
    frame::{ArtifactHashes, Frame, FrameId, FrameStore},
    nix_build::ServerArtifacts,
    user_flake::{UserArtifacts, build_user_artifacts},
};

// vsock CIDs/port are shared with the agent via `agent-protocol`
// (VSOCK_GUEST_CID / VSOCK_HOST_PORT) so the two sides can't drift. The host
// pre-creates an AF_UNIX listener at `<jail>/vsock.sock_<port>`; firecracker
// forwards the guest's connection to (host CID 2, port) there. The agent
// dials out and the host accepts — see `accept_agent`.
const VM_START_TIMEOUT: Duration = Duration::from_secs(30);
/// Cap on how long we wait for the agent to (re)connect after a restore.
/// The agent reconnects on its own once its heartbeat write fails, so this
/// only bounds pathological cases (agent crashed, vsock broken).
const AGENT_AWAKE_TIMEOUT: Duration = Duration::from_secs(30);
/// Cap on how long userspace has to come up to the point where the agent
/// connects out and sends `Hello`. The agent's systemd unit gates on
/// `multi-user.target` (see `host/base-module.nix`), so a successful
/// connection implies the whole user-flake's userspace is up. User flakes
/// can pull in slow services; keep this generous.
const USERSPACE_READY_TIMEOUT: Duration = Duration::from_secs(120);

/// Per-connection wait for the agent's `Hello` after we accept it. A clean
/// connection delivers Hello in ~1ms; if it doesn't arrive in this window
/// the connection is "born dead" (a reset-window race) — we drop it, which
/// propagates a reset to the guest so the agent reconnects, and we accept
/// the next one.
const HELLO_TIMEOUT: Duration = Duration::from_secs(1);

const VCPU_COUNT: u8 = 1;
const MEM_SIZE_MIB: usize = 1024;

// Jail-internal paths. These end up embedded in snapshots, so they're stable
// across all builds/steps. The matching host paths live inside each jail's
// per-op root and don't collide with each other.
const JAIL_API_SOCKET: &str = "/api.sock";
const JAIL_VSOCK_UDS: &str = "/vsock.sock";
// Snapshot outputs land on the jail-output scratch volume, bind-mounted at
// `<jail>/out` per op (see JailOutMount). A plain fs (ext4) — fc's sparse
// 4 KiB diff writes were paying btrfs CoW extent churn on the store volume.
const JAIL_SNAP_OUT: &str = "/out/snap.out";
const JAIL_MEM_OUT: &str = "/out/mem.out";

/// Host-side root of the jail-output scratch volume (`CRADLE_JAIL_OUT`).
pub fn jail_out_root() -> &'static Path {
    static ROOT: std::sync::LazyLock<PathBuf> = std::sync::LazyLock::new(|| {
        std::env::var_os("CRADLE_JAIL_OUT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/mnt/cradle-out"))
    });
    &ROOT
}

/// Per-op scratch dir on the jail-output volume, bind-mounted at
/// `<jail>/out` so the chrooted firecracker can write its snapshot there.
/// Unmounts (lazily) and removes the scratch on drop — every op exit path.
/// The host's mount namespace is private to the systemd unit, so even a
/// crashed host leaks nothing past the unit's lifetime; host-restart sweeps
/// leftover scratch dirs.
struct JailOutMount {
    target: PathBuf,
    scratch: PathBuf,
}

impl JailOutMount {
    fn mount(jail_path: &Path) -> std::io::Result<Self> {
        use std::os::unix::ffi::OsStrExt;
        let jail_id = jail_path
            .parent()
            .and_then(|p| p.file_name())
            .ok_or_else(|| std::io::Error::other("jail path has no id component"))?;
        let scratch = jail_out_root().join(jail_id);
        std::fs::create_dir_all(&scratch)?;
        let target = jail_path.join("out");
        std::fs::create_dir_all(&target)?;
        let src = std::ffi::CString::new(scratch.as_os_str().as_bytes())
            .map_err(std::io::Error::other)?;
        let tgt = std::ffi::CString::new(target.as_os_str().as_bytes())
            .map_err(std::io::Error::other)?;
        let ret = unsafe {
            libc::mount(
                src.as_ptr(),
                tgt.as_ptr(),
                std::ptr::null(),
                libc::MS_BIND,
                std::ptr::null(),
            )
        };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { target, scratch })
    }
}

impl Drop for JailOutMount {
    fn drop(&mut self) {
        use std::os::unix::ffi::OsStrExt;
        if let Ok(tgt) = std::ffi::CString::new(self.target.as_os_str().as_bytes()) {
            // Lazy detach: never blocks on a straggling open fd.
            unsafe { libc::umount2(tgt.as_ptr(), libc::MNT_DETACH) };
        }
        let _ = std::fs::remove_dir_all(&self.scratch);
    }
}

/// `VirtualPathResolver` that canonicalizes our artifact filenames before
/// they're placed inside the jail. The default `FlatVirtualPathResolver`
/// uses the source file's basename verbatim, which differs between a
/// `/nix/store/<hash>-microvm-store-disk.erofs` (at build time) and a
/// `<frame_dir>/store_disk` (at step time). Firecracker embeds the block
/// device's jail-internal path into the snapshot, so restore fails with
/// ENOENT if those two paths don't match. Canonicalizing both sides to
/// `/store_disk` makes the snapshot's embedded path stable across the
/// entire build → step → step → … chain. Kernel and initrd are normalized
/// for hygiene too, even though they're not strictly required to match
/// (they live in guest memory after boot, not on the host filesystem).
#[derive(Debug, Clone, Default)]
struct CradleResolver;

impl VirtualPathResolver for CradleResolver {
    fn resolve_virtual_path(
        &self,
        outside_path: &std::path::Path,
    ) -> Result<PathBuf, VirtualPathResolverError> {
        let name = outside_path
            .file_name()
            .ok_or(VirtualPathResolverError::InitialPathHasNoFilename)?
            .to_string_lossy();
        let canonical = if name.ends_with("microvm-store-disk.erofs") || name == "store_disk" {
            "store_disk"
        } else if name.contains("-vmlinux") || name == "vmlinux" || name == "kernel" {
            "kernel"
        } else if name.contains("-initrd") || name == "initrd" {
            "initrd"
        } else if name == "snapshot" {
            "snapshot"
        } else if name == "mem" {
            "mem"
        } else {
            // Fall back to plain-filename mapping for anything we don't recognize.
            return Ok(PathBuf::from(format!("/{name}")));
        };
        Ok(PathBuf::from(format!("/{canonical}")))
    }
}

#[derive(Debug)]
pub enum BuildEvent {
    /// One line of progress output (`source` = "nix" or "boot").
    Log { source: &'static str, line: String },
    /// Coarse-grained lifecycle phase marker. Emitted at every transition
    /// so the client can tell whether we're stuck in nix, mid-prepare, mid-boot,
    /// mid-snapshot, etc., even when no per-line output is flowing.
    Phase(&'static str),
    /// Agent finished initial handshake.
    Ready,
}

#[derive(Debug)]
pub enum StepEvent {
    /// Coarse-grained lifecycle phase marker — see `BuildEvent::Phase`.
    Phase(&'static str),
    /// A chunk of bytes from the guest process's stdout or stderr.
    Stream {
        stream: agent_protocol::Stream,
        data: Vec<u8>,
    },
}

/// Caller → step_frame messages routed to the guest agent mid-eval. The
/// SSE handler never sends any (one-way transport); the WS handler relays
/// these from inbound frames so an interactive client can carry stdin
/// (and ultimately the SSH session for a dropbear-wrapped step).
#[derive(Debug)]
pub enum StepInput {
    /// Append bytes to the guest child's stdin.
    Stdin(Vec<u8>),
    /// Close the guest child's stdin (EOF on its read end).
    StdinClose,
    /// Kill the running guest process tree.
    Kill,
}

// The step request shape is shared with the CLI via `client-protocol` so the
// two sides can't skew (a serde mismatch here used to surface as a silently
// dropped error frame on the client).
pub use client_protocol::EvalRequest;

/// What happened to a step's eval, as reported by the agent. Both variants
/// represent agent-clean outcomes — the agent finished its work and the VM
/// is in a known state, so `step_frame` snapshots and returns a child frame.
/// Anything fuzzier (agent disconnected, connection broken mid-eval) is
/// returned as `Err` from `step_frame` instead, so no frame is produced.
#[derive(Debug)]
pub enum StepOutcome {
    /// Child process ran and exited (cleanly or by signal).
    Exited(ExitResult),
    /// Agent's `Command::spawn` failed before the child ran (e.g. ENOENT on
    /// the binary path, EACCES on the cwd). VM state is provably unchanged
    /// from the parent frame in this case.
    SpawnFailed(std::io::Error),
    /// The child spawned and ran, but the agent's `wait()` errored. Unlike
    /// `SpawnFailed`, the process executed and may have mutated the frame —
    /// only its exit status is unknown. Still snapshotted.
    WaitFailed(std::io::Error),
}

/// Build a frame from scratch.
///
/// The kernel + initrd + cmdline always come from the server-controlled
/// `artifacts` (resolved once at host startup; the initrd has the agent
/// baked in). The storeDisk source depends on `user_flake_dir`:
///
/// - `Some(dir)`: run the wrapper-flake build to produce a custom storeDisk
///   from the user's uploaded `nixosModules.guest`. Nix progress streams as
///   `BuildEvent::Log { source: "nix" }`.
/// - `None`: use `artifacts.default_store_disk` (no per-request nix work).
pub async fn build_frame(
    installation: Arc<VmmInstallation>,
    store: Arc<FrameStore>,
    artifacts: Arc<ServerArtifacts>,
    user_flake_dir: Option<PathBuf>,
    events: mpsc::Sender<BuildEvent>,
    mut cancel: oneshot::Receiver<()>,
) -> Result<FrameId, OpError> {
    // Phase 1 — no VM yet: resolve the boot bundle. Raced against `cancel`
    // so a client disconnect aborts a long nix build instead of letting it
    // run detached to completion; dropping the future kills the child `nix`
    // process (`kill_on_drop` in `run_nix_build`). Builds are deliberately
    // NOT time-bounded — arbitrarily long is fine, they just have to be
    // cancellable.
    let bundle: UserArtifacts = tokio::select! {
        biased;
        _ = &mut cancel => return Err(OpError::vmm("client cancelled".into())),
        r = resolve_bundle(&store, &artifacts, user_flake_dir, &events) => r?,
    };

    // Phase 2 — build the config and run.
    let _ = events.send(BuildEvent::Phase("preparing")).await;
    let cmdline = read_cmdline(&bundle.cmdline).await?;
    let runtime = new_op_runtime(&store)?;
    let (mut rs, executor, jail_path, serial_log_path) = (
        runtime.resource_system,
        runtime.executor,
        runtime.jail_path,
        runtime.serial_log_path,
    );
    tracing::info!("serial log: {}", serial_log_path.display());

    let kernel = create_moved(&mut rs, bundle.kernel.clone())?;
    let initrd = create_moved(&mut rs, bundle.initrd.clone())?;
    let store_disk = create_moved(&mut rs, bundle.store_disk.clone())?;
    let vsock_uds = create_produced(&mut rs, JAIL_VSOCK_UDS)?;

    let config = VmConfiguration::New {
        init_method: InitMethod::ViaApiCalls,
        data: base_config_data(kernel, initrd, store_disk, vsock_uds, cmdline),
    };

    let mut vm = Vm::prepare(executor, rs, (*installation).clone(), config)
        .await
        .map_err(|e| OpError::vmm(format!("Vm::prepare: {e:?}")))?;
    let _out_mount = JailOutMount::mount(&jail_path).map_err(OpError::io)?;

    // From here on we own a live `vm`: every exit path must reach
    // `hard_kill` below (a leaked firecracker process pins its KVM fds and
    // 1 GiB of guest RAM until host restart). The boot→snapshot work is
    // extracted so an early `?` return can't skip the kill, and so it can be
    // raced against `cancel` — the same shape `step_frame` uses.
    let outcome = tokio::select! {
        biased;
        _ = &mut cancel => Err(OpError::vmm("client cancelled".into())),
        r = run_build_after_prepare(&mut vm, &events, &jail_path, serial_log_path, &bundle, &store) => r,
    };

    hard_kill(vm).await;
    outcome
}

/// The boot-bundle half of a build: the per-request wrapper-flake nix build
/// for an uploaded flake, or the pre-built server defaults. No VM exists yet,
/// so `build_frame` can cancel this by simply dropping the future.
///
/// All four boot artifacts (kernel, initrd, storeDisk, cmdline) come as a
/// coherent bundle from ONE nixosSystem — either the host's pre-built `guest`
/// config (default path) or a per-request wrapper around the user's
/// `nixosModules.guest` (user-flake path). They cannot be mixed across
/// configs: microvm.nix encodes `init=/nix/store/<this-system>/init` in
/// `kernelParams`, and that path only resolves inside the matching system's
/// closure.
async fn resolve_bundle(
    store: &Arc<FrameStore>,
    artifacts: &ServerArtifacts,
    user_flake_dir: Option<PathBuf>,
    events: &mpsc::Sender<BuildEvent>,
) -> Result<UserArtifacts, OpError> {
    match user_flake_dir {
        Some(dir) => {
            let _ = events.send(BuildEvent::Phase("nix_build")).await;
            let (nix_tx, nix_rx) = mpsc::channel::<String>(64);
            let nix_forward = spawn_log_forwarder(nix_rx, "nix", events.clone());
            let scratch = store
                .root()
                .join("user-builds")
                .join(format!("ub-{}", Ulid::new()));
            tokio::fs::create_dir_all(&scratch)
                .await
                .map_err(OpError::io)?;
            let result = build_user_artifacts(
                &dir,
                &scratch,
                &artifacts.agent_static,
                &artifacts.pty_bridge_static,
                Some(nix_tx),
            )
            .await
            .map_err(OpError::io)?;
            let _ = nix_forward.await;
            Ok(result)
        }
        None => Ok(UserArtifacts {
            kernel: artifacts.default_kernel.clone(),
            initrd: artifacts.default_initrd.clone(),
            store_disk: artifacts.default_store_disk.clone(),
            cmdline: artifacts.default_cmdline.clone(),
        }),
    }
}

/// Everything between `Vm::prepare` and `hard_kill` in a build. Extracted so
/// `build_frame` can race it against a cancel signal in a `tokio::select!`,
/// dropping this future cleanly while still reaching `hard_kill(vm)` outside.
async fn run_build_after_prepare(
    vm: &mut Vm<JailedVmmExecutor<CradleResolver>, DirectProcessSpawner, TokioRuntime>,
    events: &mpsc::Sender<BuildEvent>,
    jail_path: &Path,
    serial_log_path: PathBuf,
    bundle: &UserArtifacts,
    store: &Arc<FrameStore>,
) -> Result<FrameId, OpError> {
    // Create the guest→host listener before the guest boots, so it's there
    // when the agent dials out.
    let listener = create_agent_listener(jail_path)?;

    let _ = events.send(BuildEvent::Phase("booting")).await;
    vm.start(VM_START_TIMEOUT)
        .await
        .map_err(|e| OpError::vmm(format!("Vm::start: {e:?}")))?;

    let _serial_tasks = spawn_serial_taps(vm, serial_log_path, Some(events.clone()));

    // Wait for the agent to connect out and send `Hello`. The agent only
    // dials after `cradle-agent.service` is running, which is gated on
    // `multi-user.target` (see `base-module.nix`), so a successful Hello
    // is the build's "userspace ready" signal. We just accept — the agent
    // is the one that connects.
    let _ = events
        .send(BuildEvent::Phase("waiting_for_userspace"))
        .await;
    accept_agent(&listener, USERSPACE_READY_TIMEOUT)
        .await
        .map_err(|e| {
            OpError::vmm(format!(
                "agent didn't connect within {}s — userspace stuck or agent failed to dial: {e:?}",
                USERSPACE_READY_TIMEOUT.as_secs()
            ))
        })?;
    let _ = events.send(BuildEvent::Ready).await;

    let _ = events.send(BuildEvent::Phase("snapshotting")).await;
    let inputs = FreshInputs {
        kernel: &bundle.kernel,
        initrd: &bundle.initrd,
        store_disk: &bundle.store_disk,
        cmdline: &bundle.cmdline,
    };
    snapshot_into_frame(vm, store, FrameInputs::Fresh(inputs)).await
}

/// Step a frame: restore the parent's VM state, run one command, snapshot the result.
///
/// `cancel` lets the caller request an early abort (e.g. client SSE disconnect).
/// Cancellation drops the inner work future and always reaches `hard_kill(vm)`,
/// so no firecracker process / KVM resources / jail mounts leak.
pub async fn step_frame(
    installation: Arc<VmmInstallation>,
    store: Arc<FrameStore>,
    parent: Arc<Frame>,
    eval: EvalRequest,
    events: mpsc::Sender<StepEvent>,
    inputs: mpsc::Receiver<StepInput>,
    mut cancel: oneshot::Receiver<()>,
) -> Result<(FrameId, StepOutcome), OpError> {
    let t0 = std::time::Instant::now();
    tracing::info!("step_frame: entered");

    let cmdline = read_cmdline(&parent.cmdline()).await?;
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "step_frame: read_cmdline done"
    );

    let runtime = new_op_runtime(&store)?;
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "step_frame: new_op_runtime done"
    );

    let (mut rs, executor, jail_path, serial_log_path) = (
        runtime.resource_system,
        runtime.executor,
        runtime.jail_path,
        runtime.serial_log_path,
    );
    tracing::info!("serial log: {}", serial_log_path.display());

    // Moved resources from the parent frame's directory. CradleResolver
    // maps each filename to the canonical `/kernel`, `/initrd`, `/store_disk`,
    // `/snapshot`, `/mem` paths inside the jail — the same paths the build
    // used, so the snapshot's embedded block-device path resolves correctly.
    // The parent's mem image may still be materializing (captures return
    // their id before the background patch lands). Instant when complete.
    parent.await_mem().await.map_err(OpError::io)?;
    let kernel = create_moved(&mut rs, parent.kernel())?;
    let initrd = create_moved(&mut rs, parent.initrd())?;
    let store_disk = create_moved(&mut rs, parent.store_disk())?;
    let snapshot_in = create_moved(&mut rs, parent.snapshot())?;
    let mem_in = create_moved(&mut rs, parent.mem())?;
    let vsock_uds = create_produced(&mut rs, JAIL_VSOCK_UDS)?;
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "step_frame: resources built"
    );

    let data = base_config_data(kernel, initrd, store_disk, vsock_uds, cmdline);
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "step_frame: about to emit Phase(preparing)"
    );
    let load_snapshot = LoadSnapshot {
        // Required so this restored VM's next CreateSnapshot can be a Diff
        // (only pages dirtied during this step).
        track_dirty_pages: Some(true),
        mem_backend: MemoryBackend {
            backend_type: MemoryBackendType::File,
            backend: mem_in,
        },
        snapshot: snapshot_in,
        resume_vm: Some(true),
        network_overrides: Vec::new(),
    };
    let config = VmConfiguration::RestoredFromSnapshot {
        load_snapshot,
        data,
    };

    let _ = events.send(StepEvent::Phase("preparing")).await;
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "step_frame: Phase(preparing) emitted, calling Vm::prepare"
    );
    let mut vm = Vm::prepare(executor, rs, (*installation).clone(), config)
        .await
        .map_err(|e| OpError::vmm(format!("Vm::prepare: {e:?}")))?;
    tracing::info!(
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "step_frame: Vm::prepare done"
    );
    let _out_mount = JailOutMount::mount(&jail_path).map_err(OpError::io)?;

    // From here on we own a live `vm` — wrap everything in a `select!` against
    // cancel so a client disconnect always reaches `hard_kill` below.
    //
    // Deliberately NO event send in the cancel arm: cancel means the client
    // is gone, so nobody would see it — and an `.await` on a full event
    // channel here would wedge this task forever with the VM alive (the
    // handler stops draining events once it decides to cancel).
    let outcome = tokio::select! {
        biased;
        _ = &mut cancel => Err(OpError::vmm("client cancelled".into())),
        r = run_step_after_prepare(&mut vm, &events, &jail_path, serial_log_path, &parent, &store, eval, inputs) => r,
    };

    hard_kill(vm).await;
    outcome
}

/// Everything between `Vm::prepare` and `hard_kill` in a step. Extracted so
/// `step_frame` can race it against a cancel signal in a `tokio::select!`,
/// dropping this future cleanly while still reaching `hard_kill(vm)` outside.
async fn run_step_after_prepare(
    vm: &mut Vm<JailedVmmExecutor<CradleResolver>, DirectProcessSpawner, TokioRuntime>,
    events: &mpsc::Sender<StepEvent>,
    jail_path: &Path,
    serial_log_path: PathBuf,
    parent: &Arc<Frame>,
    store: &Arc<FrameStore>,
    eval: EvalRequest,
    inputs: mpsc::Receiver<StepInput>,
) -> Result<(FrameId, StepOutcome), OpError> {
    // Create the guest→host listener BEFORE vm.start, so it exists when the
    // restored agent dials out. The agent reconnects on its own after the
    // restore (its heartbeat write fails), so we never probe or hammer.
    let listener = create_agent_listener(jail_path)?;

    let _ = events.send(StepEvent::Phase("restoring")).await;
    let t_restore = std::time::Instant::now();
    vm.start(VM_START_TIMEOUT)
        .await
        .map_err(|e| OpError::vmm(format!("Vm::start: {e:?}")))?;
    let restore_ms = t_restore.elapsed().as_millis() as u64;

    let _serial_tasks = spawn_serial_taps(vm, serial_log_path, None);

    // `attaching` is just waiting for the agent to connect out. No probing,
    // no hammering — the host stays silent so the guest kernel has
    // uncontested CPU to finish its post-restore vsock reset, then the
    // agent dials and we accept.
    let _ = events.send(StepEvent::Phase("attaching")).await;
    let t_attach = std::time::Instant::now();
    let mut link = accept_agent(&listener, AGENT_AWAKE_TIMEOUT).await?;
    tracing::info!(
        restore_ms,
        attach_ms = t_attach.elapsed().as_millis() as u64,
        "restore spans"
    );

    // `evaluating` is exactly send-eval + drain responses to ProcessExit.
    let _ = events.send(StepEvent::Phase("evaluating")).await;
    send_eval(&mut link, &eval).await?;
    let outcome = run_eval(&mut link, events, inputs).await?;

    // Keep the agent link alive until AFTER the snapshot. Dropping it
    // before the pause let the guest witness the EOF in the milliseconds
    // before freezing — the snapshot then captured the agent mid-reconnect
    // with the vsock driver mid-reset, and restoring that half-state cost
    // ~200-350ms of guest-side untangling before the agent dialed out
    // (measured; a calm in-session capture reattaches in ~15ms). With the
    // link held through the pause, the frozen agent never sees the EOF:
    // on the next restore its heartbeat write fails and it redials — the
    // fast path, and the one this comment always promised.
    let _ = events.send(StepEvent::Phase("snapshotting")).await;
    let frame_id = snapshot_into_frame(vm, store, FrameInputs::Parent(parent)).await?;
    drop(link);

    Ok((frame_id, outcome))
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

struct OpRuntime {
    resource_system: ResourceSystem<DirectProcessSpawner, TokioRuntime>,
    executor: JailedVmmExecutor<CradleResolver>,
    /// Host-side path of the jail's root directory:
    /// `<chroot_base>/firecracker/<jail_id>/root/`.
    jail_path: PathBuf,
    /// Path of the per-VM serial transcript file. Always-on, append-only;
    /// every line from the guest's stdout/stderr lands here regardless of
    /// SSE consumers or tracing filters. Useful for postmortem when the
    /// agent or vsock misbehaves.
    serial_log_path: PathBuf,
}

fn new_op_runtime(store: &FrameStore) -> Result<OpRuntime, OpError> {
    // VmmId allows alphanumeric + hyphens; ULID is alphanumeric. Prefix with a
    // letter to be safe across any future id validators.
    let jail_id_str = format!("c{}", Ulid::new().to_string().to_lowercase());
    let jail_id = VmmId::new(jail_id_str.clone())
        .map_err(|e| OpError::vmm(format!("invalid jail id: {e:?}")))?;

    let chroot_base = store.root().join("jails");
    let jail_path = chroot_base
        .join("firecracker")
        .join(&jail_id_str)
        .join("root");

    let serial_log_path = store
        .root()
        .join("serial")
        .join(format!("{jail_id_str}.log"));

    let vmm_args = VmmArguments::new(VmmApiSocket::Enabled(PathBuf::from(JAIL_API_SOCKET)))
        .log_level(VmmLogLevel::Info);
    let jailer_args = JailerArguments::new(jail_id).chroot_base_dir(chroot_base);
    let executor = JailedVmmExecutor::new(vmm_args, jailer_args, CradleResolver);

    let resource_system = ResourceSystem::new(
        DirectProcessSpawner,
        TokioRuntime,
        VmmOwnershipModel::Shared,
    );

    Ok(OpRuntime {
        resource_system,
        executor,
        jail_path,
        serial_log_path,
    })
}

async fn read_cmdline(path: &Path) -> Result<String, OpError> {
    let raw = tokio::fs::read_to_string(path).await.map_err(OpError::io)?;
    Ok(raw.trim().to_string())
}

fn base_config_data(
    kernel: Resource,
    initrd: Resource,
    store_disk: Resource,
    vsock_uds: Resource,
    cmdline: String,
) -> VmConfigurationData {
    VmConfigurationData {
        boot_source: BootSource {
            kernel_image: kernel,
            boot_args: Some(cmdline),
            initrd: Some(initrd),
        },
        machine_configuration: MachineConfiguration {
            vcpu_count: VCPU_COUNT,
            mem_size_mib: MEM_SIZE_MIB,
            smt: None,
            // Track dirty pages so a later step can capture a Diff snapshot
            // (the cheap dirty-set oracle for O(dirty) tree ingest).
            track_dirty_pages: Some(true),
            huge_pages: None,
        },
        drives: vec![Drive {
            drive_id: "store".into(),
            block: Some(store_disk),
            cache_type: None,
            io_engine: None,
            is_read_only: Some(true),
            is_root_device: false,
            partuuid: None,
            rate_limiter: None,
            socket: None,
        }],
        pmem_devices: vec![],
        network_interfaces: vec![],
        cpu_template: crate::cpu_template::cradle_cpu_template(),
        balloon_device: None,
        vsock_device: Some(VsockDevice {
            guest_cid: VSOCK_GUEST_CID,
            uds: vsock_uds,
        }),
        logger_system: None,
        metrics_system: None,
        memory_hotplug_configuration: None,
        mmds_configuration: None,
        entropy_device: None,
    }
}

fn create_moved(
    rs: &mut ResourceSystem<DirectProcessSpawner, TokioRuntime>,
    path: PathBuf,
) -> Result<Resource, OpError> {
    rs.create_resource(
        path,
        ResourceType::Moved(MovedResourceType::HardLinkedOrCopied),
    )
    .map_err(|e| OpError::vmm(format!("create_resource(moved): {e:?}")))
}

fn create_produced(
    rs: &mut ResourceSystem<DirectProcessSpawner, TokioRuntime>,
    jail_relative_path: &str,
) -> Result<Resource, OpError> {
    rs.create_resource(PathBuf::from(jail_relative_path), ResourceType::Produced)
        .map_err(|e| OpError::vmm(format!("create_resource(produced): {e:?}")))
}

fn spawn_serial_taps(
    vm: &mut Vm<JailedVmmExecutor<CradleResolver>, DirectProcessSpawner, TokioRuntime>,
    serial_log_path: PathBuf,
    boot_events: Option<mpsc::Sender<BuildEvent>>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    if let Ok(pipes) = vm.take_pipes() {
        let stdout = pipes.stdout.into_inner();
        let stderr = pipes.stderr.into_inner();
        match boot_events {
            Some(tx) => {
                handles.push(spawn_serial_to_events(
                    stdout,
                    "boot",
                    serial_log_path.clone(),
                    tx.clone(),
                ));
                handles.push(spawn_serial_to_tracing(stderr, "fc", serial_log_path));
            }
            None => {
                handles.push(spawn_serial_to_tracing(
                    stdout,
                    "guest",
                    serial_log_path.clone(),
                ));
                handles.push(spawn_serial_to_tracing(stderr, "fc", serial_log_path));
            }
        }
    }
    handles
}

/// Open the per-VM serial log file in append mode. Two parallel taps each
/// hold their own writer; the kernel synchronizes O_APPEND writes so lines
/// don't interleave mid-line. None on failure (we just skip the file write
/// rather than tear down the tap entirely).
async fn open_serial_log(path: &Path) -> Option<tokio::fs::File> {
    match tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
    {
        Ok(f) => Some(f),
        Err(e) => {
            tracing::error!("serial log open {} failed: {e}", path.display());
            None
        }
    }
}

async fn append_serial_line(file: &mut Option<tokio::fs::File>, prefix: &str, line: &str) {
    let Some(f) = file else { return };
    let unix_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0);
    let formatted = format!("{unix_us}us [{prefix}] {line}\n");
    if f.write_all(formatted.as_bytes()).await.is_err() {
        // Drop the file handle so we stop trying.
        *file = None;
    }
}

/// Read the next `\n`-terminated line, decoding lossily. Unlike
/// `AsyncBufReadExt::lines()`, a non-UTF-8 byte doesn't end the stream:
/// serial output (kernel + arbitrary guest binaries) and nix stderr are not
/// guaranteed UTF-8, and `next_line()` returning `Err(InvalidData)` used to
/// silently truncate both transcripts at the first raw byte. Returns `None`
/// on EOF or a real I/O error.
pub(crate) async fn next_line_lossy<R>(reader: &mut R) -> Option<String>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let mut raw = Vec::new();
    match reader.read_until(b'\n', &mut raw).await {
        Ok(0) => None,
        Ok(_) => {
            while matches!(raw.last(), Some(b'\n') | Some(b'\r')) {
                raw.pop();
            }
            Some(String::from_utf8_lossy(&raw).into_owned())
        }
        Err(_) => None,
    }
}

fn spawn_serial_to_events<R>(
    reader: R,
    source: &'static str,
    serial_log_path: PathBuf,
    tx: mpsc::Sender<BuildEvent>,
) -> tokio::task::JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut file = open_serial_log(&serial_log_path).await;
        let mut reader = BufReader::new(reader);
        while let Some(line) = next_line_lossy(&mut reader).await {
            append_serial_line(&mut file, source, &line).await;
            if tx.send(BuildEvent::Log { source, line }).await.is_err() {
                // SSE consumer gone — keep draining to the file so the
                // serial transcript stays complete.
                while let Some(line) = next_line_lossy(&mut reader).await {
                    append_serial_line(&mut file, source, &line).await;
                }
                return;
            }
        }
    })
}

fn spawn_serial_to_tracing<R>(
    reader: R,
    prefix: &'static str,
    serial_log_path: PathBuf,
) -> tokio::task::JoinHandle<()>
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut file = open_serial_log(&serial_log_path).await;
        let mut reader = BufReader::new(reader);
        while let Some(line) = next_line_lossy(&mut reader).await {
            append_serial_line(&mut file, prefix, &line).await;
            tracing::debug!(target: "cradle::serial", "[{prefix}] {line}");
        }
    })
}

fn spawn_log_forwarder(
    mut rx: mpsc::Receiver<String>,
    source: &'static str,
    tx: mpsc::Sender<BuildEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(line) = rx.recv().await {
            if tx.send(BuildEvent::Log { source, line }).await.is_err() {
                break;
            }
        }
    })
}

/// Create the guest→host vsock listener at `<jail>/vsock.sock_<HOST_PORT>`.
/// Firecracker forwards a guest connection to (host CID 2, HOST_PORT) to
/// this AF_UNIX socket. Must exist before the guest dials (we create it
/// before `vm.start`). With `VmmOwnershipModel::Shared` the jailed
/// firecracker runs as our uid, so it can connect to the socket we own.
fn create_agent_listener(jail_path: &Path) -> Result<UnixListener, OpError> {
    let path = jail_path.join(format!("vsock.sock_{VSOCK_HOST_PORT}"));
    // Defensive: a fresh per-op jail shouldn't have a stale socket, but
    // bind() fails on EADDRINUSE if one somehow exists.
    let _ = std::fs::remove_file(&path);
    UnixListener::bind(&path).map_err(OpError::io)
}

/// Accept the agent's connection and confirm the byte path via `Hello`.
///
/// The agent dials out and the host accepts here — no host-side probing,
/// no hammering. After a restore the agent reconnects on its own (its
/// heartbeat write fails), so we just wait on `accept()`. If we accept a
/// "born dead" connection (rare reset-window race), the Hello won't
/// arrive within `HELLO_TIMEOUT`; we drop it (which resets the guest end
/// and prompts the agent to reconnect) and accept the next one. Bounded
/// by `total_timeout`.
async fn accept_agent(
    listener: &UnixListener,
    total_timeout: Duration,
) -> Result<AgentLink, OpError> {
    let deadline = tokio::time::Instant::now() + total_timeout;
    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Err(OpError::vmm(format!(
                "agent didn't connect within {}s — process didn't start or vsock isn't working",
                total_timeout.as_secs()
            )));
        }
        let remaining = deadline - now;

        let (stream, _addr) = match tokio::time::timeout(remaining, listener.accept()).await {
            Ok(Ok(pair)) => pair,
            Ok(Err(e)) => return Err(OpError::io(e)),
            Err(_) => continue, // overall deadline handled at loop top
        };

        let mut link = AgentLink::from_stream(stream);
        match tokio::time::timeout(HELLO_TIMEOUT, link.recv()).await {
            Ok(Ok(Some(AgentMessage::Hello))) => return Ok(link),
            Ok(Ok(Some(other))) => {
                tracing::warn!("expected Hello, got {other:?}; dropping, awaiting reconnect");
            }
            Ok(Ok(None)) => {
                tracing::warn!("agent closed before Hello; dropping, awaiting reconnect");
            }
            Ok(Err(e)) => {
                tracing::warn!("recv error before Hello: {e}; dropping, awaiting reconnect");
            }
            Err(_) => {
                tracing::warn!(
                    "no Hello within {HELLO_TIMEOUT:?} (born dead); dropping, awaiting reconnect"
                );
            }
        }
        // link dropped here → guest end resets → agent reconnects → we
        // loop back to accept the fresh connection.
    }
}

/// Send the `Eval` message on an attached link. Cheap (~one network write);
/// extracted so the caller can attribute send time to the `evaluating`
/// phase rather than `attaching`.
async fn send_eval(link: &mut AgentLink, eval: &EvalRequest) -> Result<(), OpError> {
    link.send(&HostMessage::Eval {
        pwd_path: eval.cwd.clone(),
        binary_path: eval.binary.clone(),
        argv: eval.argv.clone(),
    })
    .await
    .map_err(OpError::io)
}

async fn run_eval(
    link: &mut AgentLink,
    events: &mpsc::Sender<StepEvent>,
    mut inputs: mpsc::Receiver<StepInput>,
) -> Result<StepOutcome, OpError> {
    // Once the inputs channel closes, the SSE handler (which dropped its
    // tx on construction) and a hung-up WS handler both end up here.
    // Disable the branch via a per-branch guard so a closed channel
    // doesn't busy-spin the select.
    let mut inputs_open = true;
    loop {
        tokio::select! {
            biased;
            // Forward caller inputs (stdin, kill) to the agent. The WS
            // handler relays these from inbound websocket messages.
            i = inputs.recv(), if inputs_open => {
                match i {
                    Some(StepInput::Stdin(data)) => {
                        link.send(&HostMessage::Stdin(data)).await.map_err(OpError::io)?;
                    }
                    Some(StepInput::StdinClose) => {
                        link.send(&HostMessage::StdinClose).await.map_err(OpError::io)?;
                    }
                    Some(StepInput::Kill) => {
                        link.send(&HostMessage::Kill).await.map_err(OpError::io)?;
                    }
                    None => {
                        inputs_open = false;
                    }
                }
            }
            r = link.recv() => {
                let msg = r.map_err(OpError::io)?;
                match msg {
                    Some(m) => {
                        if let Some(out) = consume_agent_msg(events, m).await? {
                            return Ok(out);
                        }
                    }
                    None => {
                        // Agent disconnected mid-eval. VM may have a half-run child.
                        // Bail without snapshotting; the caller doesn't get a frame.
                        return Err(OpError::vmm("agent closed during eval".into()));
                    }
                }
            }
        }
    }
}

/// Handle one `AgentMessage`. Returns `Ok(Some(outcome))` on a terminal
/// message (`ProcessExit`/`ProcessErr`), `Ok(None)` on a chunk that was
/// forwarded as a StepEvent, or `Err` on protocol violation.
async fn consume_agent_msg(
    events: &mpsc::Sender<StepEvent>,
    m: AgentMessage,
) -> Result<Option<StepOutcome>, OpError> {
    match m {
        AgentMessage::StreamChunk { stream, data } => {
            let _ = events.send(StepEvent::Stream { stream, data }).await;
            Ok(None)
        }
        AgentMessage::ProcessExit(r) => Ok(Some(StepOutcome::Exited(r))),
        // SpawnFailed is a clean agent-side outcome: agent reported back
        // that spawn() failed, VM state is unchanged. Treat it like an
        // exit so step_frame still snapshots and produces a child frame.
        AgentMessage::ProcessErr(e) => Ok(Some(StepOutcome::SpawnFailed(e))),
        // The child spawned and ran but wait() errored — state may have
        // changed, so this must not masquerade as a spawn failure. Still a
        // clean terminal outcome: snapshot and report honestly.
        AgentMessage::ProcessWaitErr(e) => Ok(Some(StepOutcome::WaitFailed(e))),
        AgentMessage::Hello => {
            // Hello is consumed during accept and should not reappear
            // mid-eval. Treat as a protocol violation.
            Err(OpError::vmm("unexpected Hello mid-eval".into()))
        }
        AgentMessage::Heartbeat => {
            // Agent's liveness write; nothing to do host-side — its
            // purpose is the *agent* observing the write succeed/fail.
            Ok(None)
        }
    }
}

enum FrameInputs<'a> {
    Fresh(FreshInputs<'a>),
    Parent(&'a Arc<Frame>),
}

/// Source paths for the four pre-snapshot artifacts of a fresh build —
/// kernel + initrd + cmdline come from server `ServerArtifacts`, store_disk
/// is either default or per-request user-built.
struct FreshInputs<'a> {
    kernel: &'a Path,
    initrd: &'a Path,
    store_disk: &'a Path,
    cmdline: &'a Path,
}

async fn snapshot_into_frame(
    vm: &mut Vm<JailedVmmExecutor<CradleResolver>, DirectProcessSpawner, TokioRuntime>,
    store: &Arc<FrameStore>,
    inputs: FrameInputs<'_>,
) -> Result<FrameId, OpError> {
    vm.pause()
        .await
        .map_err(|e| OpError::vmm(format!("vm.pause: {e:?}")))?;

    // Fresh builds capture a Full snapshot (no base). Steps capture a Diff —
    // only pages dirtied since the parent was restored. The Diff mem file is a
    // cheap dirty-set *oracle*; we never restore it directly. The on-disk frame
    // always stores a complete `mem` (so restore just mmaps a file), and the
    // canonical representation is the page-tree.
    let snapshot_type = match &inputs {
        FrameInputs::Fresh(_) => SnapshotType::Full,
        FrameInputs::Parent(_) => SnapshotType::Diff,
    };

    // create_snapshot() calls start_initialization on these Resources itself,
    // so they MUST be Uninitialized when we hand them over. Register them now,
    // not at config-build time (jailer's prepare() would've initialized them).
    let rs = vm.get_resource_system_mut();
    let snap_out = rs
        .create_resource(PathBuf::from(JAIL_SNAP_OUT), ResourceType::Produced)
        .map_err(|e| OpError::vmm(format!("create_resource(snap_out): {e:?}")))?;
    let mem_out = rs
        .create_resource(PathBuf::from(JAIL_MEM_OUT), ResourceType::Produced)
        .map_err(|e| OpError::vmm(format!("create_resource(mem_out): {e:?}")))?;

    let mut spans = CaptureSpans::default();
    let t_total = std::time::Instant::now();
    let t_fc = std::time::Instant::now();
    let snapshot = vm
        .create_snapshot(CreateSnapshot {
            snapshot_type: Some(snapshot_type),
            snapshot: snap_out,
            mem_file: mem_out,
        })
        .await
        .map_err(|e| OpError::vmm(format!("create_snapshot: {e:?}")))?;
    spans.fc_ms = t_fc.elapsed().as_millis() as u64;

    let (id, dir) = store.allocate().map_err(OpError::io)?;
    // Until `finalize` registers the frame, the directory is unreachable by
    // any id — remove it if we error out or the whole step future is dropped
    // (client cancel mid-snapshot), instead of stranding an up-to-1-GiB dir
    // until process exit.
    let mut dir_guard = FrameDirGuard(Some(dir.clone()));
    let (k, i, s, c) = match &inputs {
        FrameInputs::Fresh(f) => (
            f.kernel.to_path_buf(),
            f.initrd.to_path_buf(),
            f.store_disk.to_path_buf(),
            f.cmdline.to_path_buf(),
        ),
        FrameInputs::Parent(p) => (p.kernel(), p.initrd(), p.store_disk(), p.cmdline()),
    };
    // Reflink the per-frame static files (kernel/initrd/store_disk/cmdline are
    // identical down a lineage → share blocks in O(1); snapshot is fresh).
    // Falls back to a full copy across filesystems (e.g. a fresh build's nix
    // store sources) — quietly, since that's expected there.
    reflink_or_copy_quiet(k, dir.join("kernel")).await?;
    reflink_or_copy_quiet(i, dir.join("initrd")).await?;
    reflink_or_copy_quiet(s, dir.join("store_disk")).await?;
    reflink_or_copy_quiet(c, dir.join("cmdline")).await?;
    reflink_or_copy_quiet(snapshot.snapshot_path.clone(), dir.join("snapshot")).await?;

    let child_mem = dir.join("mem");
    // A step's dirty pages: the commit ships them from memory (they're the
    // whole diff, already read) and the background patch task writes them
    // into the child image after the id has been returned.
    let mut dirty: Vec<(u64, Vec<u8>)> = Vec::new();
    let (mem_tree, parent_id) = match &inputs {
        FrameInputs::Fresh(_) => {
            // Full snapshot: mem_out is the complete image. Copy it in and
            // build the whole page-tree (hashing pages; storing only nodes).
            copy(&snapshot.mem_file_path, &child_mem).await?;
            // Flush the 1 GiB copy NOW, on the build's own clock: left
            // buffered, it becomes write-back debt and balance_dirty_pages
            // throttles the next writer on this volume — the first step
            // after a build paid ~4s of stalls for it.
            let cm = child_mem.clone();
            tokio::task::spawn_blocking(move || std::fs::File::open(&cm)?.sync_data())
                .await
                .map_err(|e| OpError::io(std::io::Error::other(format!("sync join: {e}"))))?
                .map_err(OpError::io)?;
            let tree = ingest_full(store.cas(), &child_mem)
                .await
                .map_err(OpError::io)?;
            (tree, None)
        }
        FrameInputs::Parent(parent) => {
            let tree = diff_ingest(
                store.cas(),
                parent,
                &snapshot.mem_file_path,
                &mut spans,
                &mut dirty,
            )
            .await
            .map_err(OpError::io)?;
            (tree, Some(parent.id.clone()))
        }
    };

    // Artifact hashes: a step inherits kernel/initrd/store_disk/cmdline from
    // its parent (byte-identical down a lineage); only the fresh snapshot is
    // hashed. A seed hashes all five, once.
    let t_art = std::time::Instant::now();
    let snapshot_hash = hash_file(dir.join("snapshot")).await.map_err(OpError::io)?;
    let artifacts = match &inputs {
        FrameInputs::Fresh(_) => ArtifactHashes {
            kernel: hash_file(dir.join("kernel")).await.map_err(OpError::io)?,
            initrd: hash_file(dir.join("initrd")).await.map_err(OpError::io)?,
            store_disk: hash_file(dir.join("store_disk")).await.map_err(OpError::io)?,
            cmdline: hash_file(dir.join("cmdline")).await.map_err(OpError::io)?,
            snapshot: snapshot_hash,
        },
        FrameInputs::Parent(parent) => ArtifactHashes {
            snapshot: snapshot_hash,
            ..parent.artifacts
        },
    };

    spans.art_ms = t_art.elapsed().as_millis() as u64;

    // The durability event: everything the central store lacks for this frame
    // (pages, tree nodes, artifacts) plus its record land durably BEFORE the
    // id exists anywhere. A frame id is a promise any machine can cash.
    let tc = std::time::Instant::now();
    let page_src = match &inputs {
        FrameInputs::Fresh(_) => PageSource::Image(&child_mem),
        FrameInputs::Parent(_) => PageSource::Dirty(&dirty),
    };
    let uploaded = commit_frame_central(
        store,
        &id,
        parent_id.as_ref(),
        &mem_tree,
        &dir,
        page_src,
        &artifacts,
    )
    .await
    .map_err(OpError::io)?;
    spans.commit_ms = tc.elapsed().as_millis() as u64;
    tracing::info!(
        frame = %id, uploaded_blobs = uploaded,
        commit_ms = spans.commit_ms,
        "central commit OK — frame durable"
    );

    tracing::info!(
        frame = %id,
        fc = spans.fc_ms, read_hash = spans.read_hash_ms,
        update = spans.update_ms, art = spans.art_ms, commit = spans.commit_ms,
        dirty_pages = spans.dirty_pages,
        total = t_total.elapsed().as_millis() as u64,
        "capture spans"
    );

    match &inputs {
        FrameInputs::Fresh(_) => {
            // The seed's image was written (and flushed) synchronously.
            store.finalize(id.clone(), dir, mem_tree, artifacts).await;
        }
        FrameInputs::Parent(parent) => {
            // The frame is durable and its id is about to be returned; the
            // LOCAL image (a cache artifact) finishes in the background.
            // Restores of this frame gate on the ready signal.
            let (_, ready_tx) = store
                .finalize_pending(id.clone(), dir, mem_tree, artifacts)
                .await;
            let store = Arc::clone(store);
            let parent = Arc::clone(parent);
            let frame_id = id.clone();
            tokio::spawn(async move {
                let r = patch_child_mem(&store, &parent, &frame_id, &child_mem, dirty, &mem_tree)
                    .await;
                match r {
                    Ok(()) => {
                        let _ = ready_tx.send(crate::frame::MemState::Ready);
                    }
                    Err(e) => {
                        tracing::error!(frame = %frame_id, "child mem patch failed: {e}");
                        let _ = ready_tx.send(crate::frame::MemState::Failed);
                    }
                }
            });
        }
    }
    dir_guard.0 = None;
    Ok(id)
}

/// Background half of a step capture: materialize the child's full on-disk
/// mem image (reflink parent + patch dirty pages). Runs after the frame id
/// has been returned — restore waits on the ready gate, everything else
/// never needed this file. Also hosts the env-gated reconstruct measurement,
/// which wants the finished image to compare against.
async fn patch_child_mem(
    store: &Arc<FrameStore>,
    parent: &Arc<Frame>,
    id: &FrameId,
    child_mem: &Path,
    dirty: Vec<(u64, Vec<u8>)>,
    tree: &MemTree,
) -> std::io::Result<()> {
    let t0 = std::time::Instant::now();
    let parent_mem_path = parent.mem();
    let child_mem_path = child_mem.to_path_buf();
    let reflinked =
        tokio::task::spawn_blocking(move || reflink_or_copy(&parent_mem_path, &child_mem_path))
            .await
            .map_err(|e| std::io::Error::other(format!("reflink join: {e}")))??;
    if !reflinked {
        tracing::warn!(
            "mem reflink unsupported on this filesystem — fell back to a full copy; \
             child-frame creation is O(image), not O(dirty). Use XFS/btrfs/zfs for the store root."
        );
    }
    let copy_ms = t0.elapsed().as_millis() as u64;

    let child_mem_owned = child_mem.to_path_buf();
    let dirty = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<(u64, Vec<u8>)>> {
        apply_dirty(&child_mem_owned, &dirty)?;
        Ok(dirty)
    })
    .await
    .map_err(|e| std::io::Error::other(format!("patch join: {e}")))??;
    let patch_ms = t0.elapsed().as_millis() as u64 - copy_ms;
    tracing::info!(frame = %id, copy_ms, patch_ms, "async patch: child mem image ready");

    // Measurement (CRADLE_RECONSTRUCT_TEST): rebuild the just-captured child
    // image purely from the parent's image (reflink-by-content) + this
    // capture's dirty set — exactly what a cross-machine restore would do —
    // and byte-compare against the real image.
    if std::env::var_os("CRADLE_RECONSTRUCT_TEST").is_some() {
        let cas = store.cas();
        let ti = std::time::Instant::now();
        let mut index = crate::materialize::LocalIndex::default();
        index.add_image(cas, parent.mem(), &parent.mem_tree).await?;
        let index_ms = ti.elapsed().as_millis() as u64;

        let overlay = DirtyOverlay {
            inner: cas,
            pages: dirty
                .iter()
                .map(|(_, b)| (store::Hash::of(b), b.clone()))
                .collect(),
        };
        let recon = child_mem.with_extension("recon");
        match crate::materialize::reconstruct(&overlay, tree, &index, &recon).await {
            Ok(s) => {
                let recon_ok = files_equal(child_mem, &recon).await.unwrap_or(false);
                tracing::info!(
                    index_ms, plan_ms = s.plan_ms, fetch_ms = s.fetch_ms, exec_ms = s.exec_ms,
                    cloned_pages = s.cloned_pages, gap_pages = s.gap_pages, recon_ok,
                    "reconstruct measurement — child rebuilt from parent image + dirty set"
                );
            }
            Err(e) => tracing::error!("reconstruct measurement failed: {e}"),
        }
        let _ = tokio::fs::remove_file(&recon).await;
    }
    Ok(())
}

/// Where a commit finds page bytes: a seed reads ranges of its (complete,
/// synchronously-written) image file; a step serves them straight from the
/// in-memory dirty set — its child image doesn't exist yet.
enum PageSource<'a> {
    Image(&'a Path),
    Dirty(&'a [(u64, Vec<u8>)]),
}

/// Assemble and durably commit everything the central store is missing for
/// this frame. The want/have runs over the mem tree (`diff_between` prunes
/// every subtree the store already holds), pages upload from `page_src`,
/// nodes from the scratch CAS, artifacts as whole files.
/// Returns the number of blobs uploaded.
async fn commit_frame_central(
    store: &FrameStore,
    id: &FrameId,
    parent: Option<&FrameId>,
    mem_tree: &MemTree,
    dir: &Path,
    page_src: PageSource<'_>,
    artifacts: &ArtifactHashes,
) -> std::io::Result<u64> {
    let central = store.central();
    let page = store::memtree::PAGE as u64;

    let holder = HolderShim(central.as_ref());
    let d = store::memtree::diff_between(store.cas(), &holder, mem_tree).await?;

    // For a step, every missing leaf is by construction one of this
    // capture's dirty pages (anything else was the parent's, which central
    // already holds). Index them by hash for the lookup below.
    let dirty_by_hash: std::collections::HashMap<Hash, &[u8]> = match &page_src {
        PageSource::Dirty(dirty) => dirty
            .iter()
            .map(|(_, b)| (Hash::of(b), b.as_slice()))
            .collect(),
        PageSource::Image(_) => std::collections::HashMap::new(),
    };

    // A tree repeats content (zero pages above all): dedup so each blob is
    // packed once. DirStore also guards, but don't ship duplicates at all.
    let mut seen = std::collections::HashSet::new();
    let mut blobs: Vec<(Hash, BlobSrc)> = Vec::new();
    for m in &d.missing {
        if !seen.insert(m.hash) {
            continue;
        }
        if m.level == 0 {
            let src = match &page_src {
                PageSource::Image(image) => {
                    let offset = m.page_base * page;
                    let len = page.min(mem_tree.len - offset);
                    BlobSrc::FileRange {
                        path: image.to_path_buf(),
                        offset,
                        len,
                    }
                }
                PageSource::Dirty(_) => {
                    let bytes = dirty_by_hash.get(&m.hash).ok_or_else(|| {
                        std::io::Error::other(format!(
                            "missing leaf {} is not in this capture's dirty set — \
                             tree/central invariant breach",
                            m.hash
                        ))
                    })?;
                    BlobSrc::Mem(bytes.to_vec())
                }
            };
            blobs.push((m.hash, src));
        } else {
            blobs.push((m.hash, BlobSrc::Mem(store.cas().get(&m.hash).await?)));
        }
    }

    let art = [
        (artifacts.kernel, "kernel"),
        (artifacts.initrd, "initrd"),
        (artifacts.store_disk, "store_disk"),
        (artifacts.cmdline, "cmdline"),
        (artifacts.snapshot, "snapshot"),
    ];
    let art_hashes: Vec<Hash> = art.iter().map(|&(h, _)| h).collect();
    let art_missing = central.missing(&art_hashes).await?;
    for (hash, name) in art {
        if art_missing.contains(&hash) && seen.insert(hash) {
            let path = dir.join(name);
            let len = tokio::fs::metadata(&path).await?.len();
            blobs.push((hash, BlobSrc::FileRange { path, offset: 0, len }));
        }
    }

    let uploaded = blobs.len() as u64;
    let record = FrameStore::record(id, parent, mem_tree, artifacts);
    central.commit(blobs, Some(&record)).await?;
    Ok(uploaded)
}

/// `Cas` adapter over the central store for `diff_between`'s holder side —
/// only `has` is ever called there (the src side supplies node bytes).
struct HolderShim<'a>(&'a dyn ContentStore);

impl Cas for HolderShim<'_> {
    async fn put(&self, _bytes: &[u8]) -> std::io::Result<Hash> {
        Err(std::io::Error::other("HolderShim is has-only"))
    }
    async fn get(&self, _hash: &Hash) -> std::io::Result<Vec<u8>> {
        Err(std::io::Error::other("HolderShim is has-only"))
    }
    async fn has(&self, hash: &Hash) -> std::io::Result<bool> {
        self.0.has(hash).await
    }
}

/// Streaming blake3 of a file, off the async threads.
async fn hash_file(path: PathBuf) -> std::io::Result<Hash> {
    tokio::task::spawn_blocking(move || {
        let mut f = std::fs::File::open(&path)?;
        Hash::of_reader(&mut f)
    })
    .await
    .map_err(|e| std::io::Error::other(format!("hash join: {e}")))?
}

/// Removes an allocated-but-not-finalized frame directory on drop. Disarm by
/// clearing the inner Option once `finalize` has registered the frame.
struct FrameDirGuard(Option<PathBuf>);

impl Drop for FrameDirGuard {
    fn drop(&mut self) {
        if let Some(dir) = self.0.take() {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

/// Fresh-build ingest: hash every page of the full mem image into a tree,
/// storing only inner nodes — the image itself is the page-byte store.
async fn ingest_full<C: Cas>(cas: &C, mem_path: &Path) -> std::io::Result<MemTree> {
    let t0 = std::time::Instant::now();
    let tree = store::memtree::build_nodes_from_path(cas, mem_path).await?;
    let build_ms = t0.elapsed().as_millis() as u64;
    tracing::info!(mem_root = %tree.root, mem_len = tree.len, build_ms, "build ingest OK");
    Ok(tree)
}

/// Step ingest, O(dirty): the Diff snapshot's sparse mem file gives the dirty
/// pages. We (a) materialize the child's full on-disk mem = parent's mem +
/// those dirty pages, so restore still loads a complete file, and (b) ingest
/// the tree by `update`-ing the parent's tree with just the dirty pages.
/// Per-component timings of a step capture, logged as one greppable line —
/// the ruler every latency change is measured against (work.md §11).
#[derive(Debug, Clone, Copy, Default)]
struct CaptureSpans {
    fc_ms: u64,
    /// Reading the dirty set off the sparse diff + hashing it.
    read_hash_ms: u64,
    update_ms: u64,
    art_ms: u64,
    commit_ms: u64,
    dirty_pages: u64,
}

async fn diff_ingest<C: Cas>(
    cas: &C,
    parent: &Frame,
    diff_mem_path: &Path,
    spans: &mut CaptureSpans,
    dirty_out: &mut Vec<(u64, Vec<u8>)>,
) -> std::io::Result<MemTree> {
    let diff_size = tokio::fs::metadata(diff_mem_path).await?.len();

    // Read the dirty set off the sparse diff (on the jail-out scratch) and
    // hash it — the only synchronous work left. The child's full mem image
    // is NOT written here: the commit ships these bytes from memory, and
    // the image materializes in a background task after the id returns
    // (patch_child_mem).
    let t0 = std::time::Instant::now();
    let diff_path = diff_mem_path.to_path_buf();
    let (dirty, leaves) = tokio::task::spawn_blocking(
        move || -> std::io::Result<(Vec<(u64, Vec<u8>)>, Vec<(u64, store::Hash)>)> {
            let dirty = read_dirty_pages(&diff_path)?;
            let leaves = dirty
                .iter()
                .map(|(p, b)| (*p, store::Hash::of(b)))
                .collect();
            Ok((dirty, leaves))
        },
    )
    .await
    .map_err(|e| std::io::Error::other(format!("dirty-extract join: {e}")))??;
    let dirty_pages = dirty.len();
    let read_hash_ms = t0.elapsed().as_millis() as u64;

    let tu = std::time::Instant::now();
    let tree = store::memtree::update_hashes(cas, &parent.mem_tree, leaves).await?;
    let update_ms = tu.elapsed().as_millis() as u64;

    tracing::info!(
        mem_root = %tree.root, dirty_pages, diff_size,
        read_hash_ms, update_ms,
        "diff ingest OK — O(dirty)"
    );
    spans.read_hash_ms = read_hash_ms;
    spans.update_ms = update_ms;
    spans.dirty_pages = dirty_pages as u64;

    *dirty_out = dirty;
    Ok(tree)
}

/// `Cas` overlay for the reconstruct measurement: dirty pages resolve from
/// memory (they're no longer stored as blobs), everything else (tree nodes)
/// falls through to the scratch CAS.
struct DirtyOverlay<'a, C> {
    inner: &'a C,
    pages: std::collections::HashMap<Hash, Vec<u8>>,
}

impl<C: Cas> Cas for DirtyOverlay<'_, C> {
    async fn put(&self, bytes: &[u8]) -> std::io::Result<Hash> {
        self.inner.put(bytes).await
    }
    async fn get(&self, hash: &Hash) -> std::io::Result<Vec<u8>> {
        if let Some(b) = self.pages.get(hash) {
            return Ok(b.clone());
        }
        self.inner.get(hash).await
    }
    async fn has(&self, hash: &Hash) -> std::io::Result<bool> {
        if self.pages.contains_key(hash) {
            return Ok(true);
        }
        self.inner.has(hash).await
    }
}

/// Walk a sparse Diff-snapshot mem file with SEEK_DATA/SEEK_HOLE to find the
/// pages Firecracker actually wrote (the dirty set), as (page_index, bytes).
///
/// CORRECTNESS PRECONDITION: the filesystem must report holes. A Diff mem
/// file contains ONLY the dirtied pages; clean pages are holes that read
/// back as zeros. On a filesystem that doesn't report holes (9p, virtiofs,
/// most FUSE), SEEK_DATA claims the whole file is data, so this would return
/// every clean page as zeros — and `apply_dirty` would then overwrite the
/// child's reflinked parent memory with those zeros, silently corrupting the
/// frame (both the image and the tree agree on the corrupt bytes, so even a
/// byte-compare "verifies"). `probe_store_fs` at host startup refuses to run
/// on such a filesystem.
fn read_dirty_pages(diff_path: &Path) -> std::io::Result<Vec<(u64, Vec<u8>)>> {
    use std::io::{Read, Seek, SeekFrom};
    use std::os::unix::io::AsRawFd;

    let page = store::memtree::PAGE as u64;
    let mut f = std::fs::File::open(diff_path)?;
    let size = f.metadata()?.len();
    let fd = f.as_raw_fd();

    let mut pages: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut next_page: u64 = 0; // guard against re-emitting a boundary page
    let mut pos: i64 = 0;
    while (pos as u64) < size {
        let data = unsafe { libc::lseek(fd, pos, libc::SEEK_DATA) };
        if data < 0 {
            let err = std::io::Error::last_os_error();
            // ENXIO = no data at/after pos (rest is hole/EOF): done.
            if err.raw_os_error() == Some(libc::ENXIO) {
                break;
            }
            return Err(err);
        }
        let hole = unsafe { libc::lseek(fd, data, libc::SEEK_HOLE) };
        if hole < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let first = (data as u64 / page).max(next_page);
        let last = (hole as u64).div_ceil(page); // exclusive
        for p in first..last {
            let off = p * page;
            let len = std::cmp::min(page, size - off) as usize;
            let mut buf = vec![0u8; len];
            f.seek(SeekFrom::Start(off))?;
            f.read_exact(&mut buf)?;
            pages.push((p, buf));
        }
        next_page = last;
        pos = hole;
    }
    Ok(pages)
}

/// Verify the frame-store filesystem is safe to run on. Called once at host
/// startup, against the store root (jails — and therefore Diff snapshot mem
/// files — live under the same root).
///
/// Hard requirement: hole reporting (SEEK_DATA/SEEK_HOLE). Without it,
/// `read_dirty_pages` reads a Diff snapshot's clean pages as dirty zeros and
/// every child frame restores with mostly-zeroed guest RAM — silent
/// corruption, so refuse to start (see the doc comment there).
///
/// Soft requirement: reflink (FICLONE). Without it every capture degrades to
/// a full image copy — correct but O(image); warn loudly once instead of
/// per-op. `check_reflink: false` for volumes that only host snapshot
/// outputs (the jail-out scratch is deliberately a plain fs — no clones
/// happen there, so the warning would be noise).
pub fn probe_store_fs(root: &Path, check_reflink: bool) -> std::io::Result<()> {
    use std::os::unix::io::AsRawFd;

    // Hole probe: an ftruncate-extended file is all hole on any filesystem
    // that tracks sparseness, so SEEK_DATA from 0 must find no data (ENXIO).
    // Finding "data" means the fs fakes hole queries (9p/virtiofs/FUSE).
    let hole_probe = root.join(".fs-probe-hole");
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&hole_probe)?;
    f.set_len((2 * store::memtree::PAGE) as u64)?;
    let seek = unsafe { libc::lseek(f.as_raw_fd(), 0, libc::SEEK_DATA) };
    let seek_err = std::io::Error::last_os_error();
    drop(f);
    let _ = std::fs::remove_file(&hole_probe);
    if seek >= 0 {
        return Err(std::io::Error::other(format!(
            "frame store at {} is on a filesystem that does not report holes \
             (SEEK_DATA on an all-hole file returned data at offset {seek}); \
             Diff-snapshot dirty-page extraction would silently corrupt child \
             frames. Put the store on btrfs/XFS/zfs.",
            root.display()
        )));
    }
    if seek_err.raw_os_error() != Some(libc::ENXIO) {
        return Err(std::io::Error::other(format!(
            "frame store at {}: SEEK_DATA probe failed ({seek_err}); cannot \
             verify hole reporting, refusing to run",
            root.display()
        )));
    }

    // Reflink probe: performance only, warn instead of fail.
    if !check_reflink {
        tracing::info!("fs probe OK at {}: holes reported (reflink not required here)", root.display());
        return Ok(());
    }
    let src = root.join(".fs-probe-reflink-src");
    let dst = root.join(".fs-probe-reflink-dst");
    let reflinked = std::fs::write(&src, [0u8; 16]).and_then(|()| reflink_or_copy(&src, &dst));
    let _ = std::fs::remove_file(&src);
    let _ = std::fs::remove_file(&dst);
    match reflinked {
        Ok(true) => tracing::info!("store fs probe OK: holes reported, reflink supported"),
        Ok(false) => tracing::warn!(
            "store fs at {} cannot reflink — captures will fall back to full \
             O(image) copies. Use btrfs/XFS/zfs for O(dirty) forks.",
            root.display()
        ),
        Err(e) => tracing::warn!("reflink probe failed ({e}); captures may fall back to copies"),
    }
    Ok(())
}

/// FICLONE ioctl request on Linux (`_IOW(0x94, 9, int)`). Defined locally so we
/// don't depend on the `libc` version exporting the constant.
const FICLONE: libc::c_ulong = 0x4004_9409;

/// Clone `src` into a fresh `dst` via reflink (FICLONE): O(1) metadata, blocks
/// shared copy-on-write, holes preserved — so patching a few dirty pages
/// afterward makes child-frame creation O(dirty). Falls back to a full byte
/// copy when the filesystem can't reflink (XFS/btrfs/zfs can; ext4/tmpfs/9p
/// generally can't). Returns Ok(true) on reflink, Ok(false) on copy fallback.
fn reflink_or_copy(src: &Path, dst: &Path) -> std::io::Result<bool> {
    use std::os::unix::io::AsRawFd;
    let s = std::fs::File::open(src)?;
    let d = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)?;
    let ret = unsafe { libc::ioctl(d.as_raw_fd(), FICLONE, s.as_raw_fd()) };
    if ret == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        // Reflink unsupported here (wrong fs, cross-device, bad arg): full copy.
        Some(libc::EOPNOTSUPP) | Some(libc::EXDEV) | Some(libc::ENOTTY) | Some(libc::EINVAL) => {
            drop(d);
            std::fs::copy(src, dst)?;
            Ok(false)
        }
        _ => Err(err),
    }
}

/// Patch dirty pages onto the (already parent-reflinked) child mem file.
fn apply_dirty(mem_path: &Path, dirty: &[(u64, Vec<u8>)]) -> std::io::Result<()> {
    use std::os::unix::fs::FileExt;
    let page = store::memtree::PAGE as u64;
    let f = std::fs::OpenOptions::new().write(true).open(mem_path)?;
    for (p, bytes) in dirty {
        f.write_all_at(bytes, p * page)?;
    }
    Ok(())
}

/// Read up to `buf.len()` bytes, filling across short reads; returns the number
/// read (0 at EOF).
async fn read_chunk(f: &mut tokio::fs::File, buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = f.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

/// Stream-compare two files for byte equality without loading either whole.
async fn files_equal(a: &Path, b: &Path) -> std::io::Result<bool> {
    let mut fa = tokio::fs::File::open(a).await?;
    let mut fb = tokio::fs::File::open(b).await?;
    let mut ba = vec![0u8; 1 << 20];
    let mut bb = vec![0u8; 1 << 20];
    loop {
        let na = read_chunk(&mut fa, &mut ba).await?;
        let nb = read_chunk(&mut fb, &mut bb).await?;
        if na != nb {
            return Ok(false);
        }
        if na == 0 {
            return Ok(true);
        }
        if ba[..na] != bb[..nb] {
            return Ok(false);
        }
    }
}

async fn copy(src: &Path, dst: &Path) -> Result<(), OpError> {
    tokio::fs::copy(src, dst)
        .await
        .map(|_| ())
        .map_err(OpError::io)
}

/// Reflink `src` → `dst` (shared blocks, O(1)) when the filesystem supports it,
/// else fall back to a full copy. Quiet — used for the per-frame static files,
/// where a cross-fs fresh-build source copying is normal and not worth a warn.
async fn reflink_or_copy_quiet(src: PathBuf, dst: PathBuf) -> Result<(), OpError> {
    match tokio::task::spawn_blocking(move || reflink_or_copy(&src, &dst)).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(OpError::io(e)),
        Err(e) => Err(OpError::io(std::io::Error::other(format!("reflink join: {e}")))),
    }
}

async fn hard_kill(
    mut vm: Vm<JailedVmmExecutor<CradleResolver>, DirectProcessSpawner, TokioRuntime>,
) {
    let _ = vm
        .shutdown([VmShutdownAction {
            graceful: false,
            method: VmShutdownMethod::Kill,
            timeout: None,
        }])
        .await;
    let _ = vm.cleanup().await;
}

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum OpError {
    Io(std::io::Error),
    Vmm(String),
}

impl OpError {
    fn io(e: std::io::Error) -> Self {
        OpError::Io(e)
    }
    fn vmm(s: String) -> Self {
        OpError::Vmm(s)
    }
}

impl std::fmt::Display for OpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpError::Io(e) => write!(f, "io: {e}"),
            OpError::Vmm(s) => write!(f, "vmm: {s}"),
        }
    }
}

impl std::error::Error for OpError {}
