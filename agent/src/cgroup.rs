use std::ffi::{CStr, CString};
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_ID: AtomicU64 = AtomicU64::new(1);

/// One cgroup per eval. Drop rmdirs it.
///
/// `procs_path_cstr` is pre-built so `pre_exec` (post-fork, pre-exec, where
/// heap allocation can deadlock) can open it with raw libc and no allocation.
pub struct Cgroup {
    path: PathBuf,
    procs_path_cstr: CString,
    kill_path: PathBuf,
}

impl Cgroup {
    pub fn create() -> io::Result<Self> {
        let parent = agent_cgroup_root()?;
        // Uniqueness: NEXT_ID is monotonic within this process, and
        // `reap_stale` (startup) seeds it past any eval-N dir left behind by
        // a previous run — so a collision here is a real error, not a case
        // to paper over.
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!("eval-{id}"));
        fs::create_dir(&path)?;
        let procs_path_cstr = CString::new(format!("{}/cgroup.procs", path.display()))
            .expect("cgroup path has no NUL");
        let kill_path = path.join("cgroup.kill");
        Ok(Self {
            path,
            procs_path_cstr,
            kill_path,
        })
    }

    pub fn procs_path_cstr(&self) -> &CStr {
        &self.procs_path_cstr
    }

    /// Atomically SIGKILL every task in the cgroup. Idempotent.
    pub fn kill(&self) {
        if let Err(e) = fs::write(&self.kill_path, b"1") {
            // ENOENT = cgroup dir already gone; nothing to do.
            if e.kind() != io::ErrorKind::NotFound {
                log::error!("cgroup.kill write failed: {e}");
            }
        }
    }

    /// Kill, wait (bounded) for the process tree to actually drain, then
    /// rmdir. `cgroup.kill` returns before the SIGKILLed tasks are reaped, so
    /// an immediate rmdir races slow-exiting descendants into EBUSY — and a
    /// leaked eval-N dir gets captured into the snapshot and inherited by
    /// every fork of that frame. Called before ProcessExit is sent (i.e.
    /// before the host snapshots); the sync `Drop` remains as a best-effort
    /// fallback for early-error paths.
    pub async fn destroy(&self) {
        self.kill();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match fs::read_to_string(self.path.join("cgroup.procs")) {
                Ok(s) if s.trim().is_empty() => break,
                Err(_) => break, // dir gone already
                Ok(_) => {}
            }
            if tokio::time::Instant::now() >= deadline {
                log::error!(
                    "cgroup {} still has tasks after 5s; leaking dir",
                    self.path.display()
                );
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        if let Err(e) = fs::remove_dir(&self.path) {
            if e.kind() != io::ErrorKind::NotFound {
                log::error!("rmdir {} failed: {e}", self.path.display());
            }
        }
    }
}

impl Drop for Cgroup {
    fn drop(&mut self) {
        // Best-effort fallback for paths that never reached `destroy` (spawn
        // failure, task abort). rmdir fails with EBUSY if any task is still
        // in the cgroup; `create` skips over leftovers and `reap_stale`
        // retries them on the next agent start, so just log.
        if let Err(e) = fs::remove_dir(&self.path) {
            if e.kind() != io::ErrorKind::NotFound {
                log::error!("rmdir {} failed: {e}", self.path.display());
            }
        }
    }
}

/// Return the absolute path of the cgroup the agent itself lives in.
///
/// We run as a systemd service with `Delegate=yes`, so systemd creates
/// `/sys/fs/cgroup/<service-path>/` for us and lets us manage that
/// subtree. The service path comes from `/proc/self/cgroup`, which on
/// cgroup v2 is a single line `0::/system.slice/cradle-agent.service`
/// (or similar). Joining that under `/sys/fs/cgroup` gives the real
/// directory.
fn agent_cgroup_root() -> io::Result<PathBuf> {
    let content = fs::read_to_string("/proc/self/cgroup")?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("0::") {
            return Ok(PathBuf::from(format!("/sys/fs/cgroup{rest}")));
        }
    }
    Err(io::Error::new(
        io::ErrorKind::NotFound,
        "no cgroup v2 line (0::…) in /proc/self/cgroup",
    ))
}

/// Sanity-check: the systemd-delegated cgroup exists and is writable.
/// (Nothing to create — systemd already did that for us.)
pub fn ensure_parent() -> io::Result<()> {
    let parent = agent_cgroup_root()?;
    if !parent.exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("agent cgroup {} not found", parent.display()),
        ));
    }
    Ok(())
}

/// Best-effort cleanup of eval cgroups left behind by a previous agent run
/// (e.g. after a service restart) — and the uniqueness anchor for `create`:
/// NEXT_ID is advanced past every eval-N seen, so even a dir that survives
/// reaping (a task stuck in D-state that SIGKILL can't reap) can never
/// collide with a new eval. The filesystem is the source of truth for which
/// names are taken; one scan here beats guessing at create time.
pub fn reap_stale() {
    let parent = match agent_cgroup_root() {
        Ok(p) => p,
        Err(_) => return,
    };
    let entries = match fs::read_dir(&parent) {
        Ok(it) => it,
        Err(_) => return,
    };
    let mut max_seen: u64 = 0;
    for entry in entries.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        // Only touch directories we recognise as ours (`eval-<number>`, the
        // only shape `create` produces); don't reap anything systemd put here.
        let Some(n) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|s| s.strip_prefix("eval-"))
            .and_then(|s| s.parse::<u64>().ok())
        else {
            continue;
        };
        max_seen = max_seen.max(n);
        let _ = fs::write(path.join("cgroup.kill"), b"1");
        // The kill returns before the tasks are reaped; give the rmdir a few
        // bounded retries instead of giving up on the first EBUSY. (Runs
        // once at agent startup, so the short blocking sleeps are fine.)
        for _ in 0..10 {
            match fs::remove_dir(&path) {
                Ok(()) => break,
                Err(e) if e.kind() == io::ErrorKind::NotFound => break,
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(10)),
            }
        }
    }
    NEXT_ID.fetch_max(max_seen + 1, Ordering::Relaxed);
}

/// Write our own pid into `cgroup.procs` using only async-signal-safe syscalls.
/// Called from `Command::pre_exec`, i.e. post-fork pre-exec — no heap
/// allocation, no Rust formatting machinery.
pub fn migrate_self(procs_path: &CStr) -> io::Result<()> {
    // SAFETY: open/getpid/write/close are async-signal-safe and we pass a
    // valid CStr pointer.
    unsafe {
        let fd = libc::open(procs_path.as_ptr(), libc::O_WRONLY | libc::O_CLOEXEC);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let pid = libc::getpid();
        let mut buf = [0u8; 12];
        let bytes = format_pid(pid, &mut buf);
        let mut written = 0usize;
        while written < bytes.len() {
            let n = libc::write(
                fd,
                bytes.as_ptr().add(written) as *const _,
                bytes.len() - written,
            );
            if n < 0 {
                let err = io::Error::last_os_error();
                libc::close(fd);
                return Err(err);
            }
            written += n as usize;
        }
        libc::close(fd);
    }
    Ok(())
}

fn format_pid(pid: libc::pid_t, buf: &mut [u8; 12]) -> &[u8] {
    let mut n = if pid < 0 { 0 } else { pid as u32 };
    let mut i = buf.len();
    if n == 0 {
        i -= 1;
        buf[i] = b'0';
    } else {
        while n > 0 {
            i -= 1;
            buf[i] = b'0' + (n % 10) as u8;
            n /= 10;
        }
    }
    &buf[i..]
}
