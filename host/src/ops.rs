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

use agent_protocol::{AgentMessage, ExitResult, HostMessage};
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

use store::MemTree;

use crate::{
    agent_link::AgentLink,
    frame::{Frame, FrameId, FrameStore},
    nix_build::ServerArtifacts,
    user_flake::{UserArtifacts, build_user_artifacts},
};

const GUEST_CID: u32 = 3;
/// Port the agent dials on the host. The host pre-creates an AF_UNIX
/// listener at `<jail>/vsock.sock_<HOST_PORT>`; firecracker forwards the
/// guest's connection to (host CID 2, HOST_PORT) there. The agent dials
/// out and the host accepts — see `accept_agent`.
const HOST_PORT: u32 = 1024;
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
const JAIL_SNAP_OUT: &str = "/snap.out";
const JAIL_MEM_OUT: &str = "/mem.out";

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

#[derive(Debug, Clone, serde::Deserialize)]
pub struct EvalRequest {
    pub binary: String,
    pub argv: Vec<String>,
    pub cwd: String,
}

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
) -> Result<FrameId, OpError> {
    // All four boot artifacts (kernel, initrd, storeDisk, cmdline) come as
    // a coherent bundle from ONE nixosSystem — either the host's pre-built
    // `guest` config (default path) or a per-request wrapper around the
    // user's `nixosModules.guest` (user-flake path). They cannot be mixed
    // across configs: microvm.nix encodes
    // `init=/nix/store/<this-system>/init` in `kernelParams`, and that path
    // only resolves inside the matching system's closure.
    let bundle: UserArtifacts = match user_flake_dir {
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
            result
        }
        None => UserArtifacts {
            kernel: artifacts.default_kernel.clone(),
            initrd: artifacts.default_initrd.clone(),
            store_disk: artifacts.default_store_disk.clone(),
            cmdline: artifacts.default_cmdline.clone(),
        },
    };

    // Build the config and run.
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

    // Create the guest→host listener before the guest boots, so it's there
    // when the agent dials out.
    let listener = create_agent_listener(&jail_path)?;

    let _ = events.send(BuildEvent::Phase("booting")).await;
    vm.start(VM_START_TIMEOUT)
        .await
        .map_err(|e| OpError::vmm(format!("Vm::start: {e:?}")))?;

    let _serial_tasks = spawn_serial_taps(&mut vm, serial_log_path, Some(events.clone()));

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
    let frame_id = snapshot_into_frame(&mut vm, &store, FrameInputs::Fresh(inputs)).await?;

    hard_kill(vm).await;
    Ok(frame_id)
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

    // From here on we own a live `vm` — wrap everything in a `select!` against
    // cancel so a client disconnect always reaches `hard_kill` below.
    let outcome = tokio::select! {
        biased;
        _ = &mut cancel => {
            let _ = events.send(StepEvent::Phase("cancelled")).await;
            Err(OpError::vmm("client cancelled".into()))
        }
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
    vm.start(VM_START_TIMEOUT)
        .await
        .map_err(|e| OpError::vmm(format!("Vm::start: {e:?}")))?;

    let _serial_tasks = spawn_serial_taps(vm, serial_log_path, None);

    // `attaching` is just waiting for the agent to connect out. No probing,
    // no hammering — the host stays silent so the guest kernel has
    // uncontested CPU to finish its post-restore vsock reset, then the
    // agent dials and we accept.
    let _ = events.send(StepEvent::Phase("attaching")).await;
    let mut link = accept_agent(&listener, AGENT_AWAKE_TIMEOUT).await?;

    // `evaluating` is exactly send-eval + drain responses to ProcessExit.
    let _ = events.send(StepEvent::Phase("evaluating")).await;
    send_eval(&mut link, &eval).await?;
    let outcome = run_eval(&mut link, events, inputs, None).await?;

    // Drop the connection before snapshotting. The agent will detect the
    // dead connection via its heartbeat write on the next restore and
    // reconnect to the next step's listener.
    drop(link);

    let _ = events.send(StepEvent::Phase("snapshotting")).await;
    let frame_id = snapshot_into_frame(vm, store, FrameInputs::Parent(parent)).await?;

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
            guest_cid: GUEST_CID,
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
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            append_serial_line(&mut file, source, &line).await;
            if tx.send(BuildEvent::Log { source, line }).await.is_err() {
                // SSE consumer gone — keep draining to the file so the
                // serial transcript stays complete.
                while let Ok(Some(line)) = lines.next_line().await {
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
        let mut lines = BufReader::new(reader).lines();
        while let Ok(Some(line)) = lines.next_line().await {
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
    let path = jail_path.join(format!("vsock.sock_{HOST_PORT}"));
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
    first_msg: Option<AgentMessage>,
) -> Result<StepOutcome, OpError> {
    // Process the message the caller already pulled off the wire (the one
    // used to confirm the connection survived TRANSPORT_RESET) before
    // entering the select loop.
    if let Some(m) = first_msg {
        if let Some(out) = consume_agent_msg(events, m).await? {
            return Ok(out);
        }
    }

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
    Parent(&'a Frame),
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
    store: &FrameStore,
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

    let snapshot = vm
        .create_snapshot(CreateSnapshot {
            snapshot_type: Some(snapshot_type),
            snapshot: snap_out,
            mem_file: mem_out,
        })
        .await
        .map_err(|e| OpError::vmm(format!("create_snapshot: {e:?}")))?;

    let (id, dir) = store.allocate().map_err(OpError::io)?;
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
    let mem_tree = match inputs {
        FrameInputs::Fresh(_) => {
            // Full snapshot: mem_out is the complete image. Copy it in and
            // build the whole page-tree.
            copy(&snapshot.mem_file_path, &child_mem).await?;
            ingest_full(store.cas(), &child_mem)
                .await
                .map_err(OpError::io)?
        }
        FrameInputs::Parent(parent) => {
            diff_ingest(store.cas(), parent, &snapshot.mem_file_path, &child_mem)
                .await
                .map_err(OpError::io)?
        }
    };

    store.finalize(id.clone(), dir, mem_tree).await;
    Ok(id)
}

/// Fresh-build ingest: build the whole page-tree from the full mem image.
async fn ingest_full(cas: &store::LocalCas, mem_path: &Path) -> std::io::Result<MemTree> {
    let t0 = std::time::Instant::now();
    let tree = store::memtree::build_from_path(cas, mem_path).await?;
    let build_ms = t0.elapsed().as_millis() as u64;
    tracing::info!(mem_root = %tree.root, mem_len = tree.len, build_ms, "build ingest OK");
    Ok(tree)
}

/// Step ingest, O(dirty): the Diff snapshot's sparse mem file gives the dirty
/// pages. We (a) materialize the child's full on-disk mem = parent's mem +
/// those dirty pages, so restore still loads a complete file, and (b) ingest
/// the tree by `update`-ing the parent's tree with just the dirty pages.
async fn diff_ingest(
    cas: &store::LocalCas,
    parent: &Frame,
    diff_mem_path: &Path,
    child_mem: &Path,
) -> std::io::Result<MemTree> {
    let diff_size = tokio::fs::metadata(diff_mem_path).await?.len();

    // Materialize the child's full mem: reflink the parent's image (O(1)
    // metadata, blocks shared copy-on-write, holes preserved), then patch the
    // dirty pages onto it. Reflink + patch is O(dirty); a full copy was O(image).
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

    let diff_path = diff_mem_path.to_path_buf();
    let child_mem_owned = child_mem.to_path_buf();
    let dirty = tokio::task::spawn_blocking(move || -> std::io::Result<Vec<(u64, Vec<u8>)>> {
        let dirty = read_dirty_pages(&diff_path)?;
        apply_dirty(&child_mem_owned, &dirty)?;
        Ok(dirty)
    })
    .await
    .map_err(|e| std::io::Error::other(format!("dirty-extract join: {e}")))??;
    let dirty_pages = dirty.len();
    let patch_ms = t0.elapsed().as_millis() as u64 - copy_ms;

    let tu = std::time::Instant::now();
    let tree = store::memtree::update(cas, &parent.mem_tree, dirty).await?;
    let update_ms = tu.elapsed().as_millis() as u64;

    tracing::info!(
        mem_root = %tree.root, dirty_pages, diff_size,
        copy_ms, patch_ms, update_ms,
        "diff ingest OK — O(dirty)"
    );

    // Measurement (CRADLE_RECONSTRUCT_TEST): rebuild the just-captured child
    // image purely from the parent's image (reflink-by-content) + the CAS
    // (fill) — exactly what a cross-machine restore would do — and time it,
    // without touching the real child mem. Reconstruct into a temp, byte-compare
    // to the real one, delete. `index_ms` (building the parent index) is a
    // setup cost that would be persistent/amortized in real use, so it's logged
    // separately from the reconstruct itself.
    if std::env::var_os("CRADLE_RECONSTRUCT_TEST").is_some() {
        let ti = std::time::Instant::now();
        let mut index = crate::materialize::LocalIndex::default();
        index.add_image(cas, parent.mem(), &parent.mem_tree).await?;
        let index_ms = ti.elapsed().as_millis() as u64;

        let recon = child_mem.with_extension("recon");
        match crate::materialize::reconstruct(cas, &tree, &index, &recon).await {
            Ok(s) => {
                let recon_ok = files_equal(child_mem, &recon).await.unwrap_or(false);
                tracing::info!(
                    index_ms, plan_ms = s.plan_ms, fetch_ms = s.fetch_ms, exec_ms = s.exec_ms,
                    cloned_pages = s.cloned_pages, gap_pages = s.gap_pages, recon_ok,
                    "reconstruct measurement — child rebuilt from parent image + CAS"
                );
            }
            Err(e) => tracing::error!("reconstruct measurement failed: {e}"),
        }
        let _ = tokio::fs::remove_file(&recon).await;
    }

    Ok(tree)
}

/// Walk a sparse Diff-snapshot mem file with SEEK_DATA/SEEK_HOLE to find the
/// pages Firecracker actually wrote (the dirty set), as (page_index, bytes).
/// If the file is NOT sparse, this returns every page — correct, just not a
/// speedup — so it degrades gracefully.
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
