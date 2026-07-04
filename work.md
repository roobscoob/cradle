# Cradle — design + work log (2026-05-29)

A detailed record of the distributed VM-fork architecture, the decisions behind it,
what's been built and measured, and the open design for the remote store (P4).
Written so it can be picked up cold later.

---

## 1. Goal

Cradle is a distributed Firecracker microVM snapshot/fork engine. A "frame" is an
immutable VM checkpoint; "stepping" a frame restores it, runs a command, and
captures a child frame.

**Targets:**
- **< 0.5 s average** start (restore) *and* snapshot (capture). Long tail is fine.
- Scale guest RAM **256 MiB → 16 GiB**, with cost tracking *change/footprint*, not
  provisioned size.
- Distributed: **6 machines, 1 Gbps per machine, a ~10 Gbps content store**.

**Topology consequence:** 6 × 1 Gbps = 6 Gbps peak demand < 10 Gbps store, so the
store is never the aggregate bottleneck — the **per-machine 1 Gbps link is the
binding constraint**, and it never contends at the store. This kills the need for
peer-to-peer; a central store is fine.

---

## 2. Core model

A frame's memory **is** a content-addressed Merkle page-tree:
- 4 KiB page leaves (`PAGE`), grouped `FANOUT = 64` at a time into inner nodes
  (postcard-encoded `Vec<Hash>`), up to a single root. Tree height is a pure
  function of length (depth 3–4 across 256 MiB–16 GiB).
- `MemTree { root: Hash, len: u64 }` — a hash + byte length. Two trees that share
  an untouched subtree share it *by hash*.

**The tree is the source of truth and the index; the page bytes live once, in big
reflink-chained contiguous image files — NOT as small per-blob files.** Dedup needs
the content *hashes* (tiny), not the bytes stored as little objects.

**Everything is a diff from the previous state**, up a lineage that bottoms out at a
**seeded base** (a captured artifact). There is no "diff from zero."

- **Capture** = `update(parent_tree, parent→child changes)` → child tree. Changes
  come from Firecracker's Diff snapshot. O(dirty).
- **Materialize** = `reflink(nearest local ancestor image) + patch the changed
  pages`. **Eager**: the full image is materialized and `mmap`-ed before boot — no
  lazy faulting. Native, jitter-free runtime.
- **Transport (cross-machine)** = move only the cache-miss set; warm restore ≈ the
  dirty set.

**Cost model:** `bytes_over_wire ≈ guest_size × touch_fraction × miss_fraction`.
- `touch_fraction` (dirty/working set) is **absolute, not proportional** — a 16 GiB
  guest running a command touches about as much as a 1 GiB one. So guest size is a
  provisioning *ceiling*, not a cost driver.
- `miss_fraction` (content not already on this machine) is the only real free
  variable → the whole performance story reduces to **cache hit rate**.
- Levers: content-addressing/tree, local cache, affinity all push `miss ↓`. Eager
  materialize + reflink make the *local* cost O(dirty) regardless.

---

## 3. Key decisions and WHY (settled — do not relitigate)

These were hashed out at length; the rationale isn't obvious from the code.

1. **Eager, not lazy (no UFFD).** Lazy/demand paging (UFFD) makes the *whole runtime*
   hostage to the network — the guest stalls on a fault for a network RTT at random
   points for as long as it runs, plus a per-fault VM-exit + syscall (~µs/page) that
   is fine for a sparse tail but catastrophic beyond ~0.5% of pages. Eager loads the
   full image up front → native, predictable runtime. The only reason eager is
   affordable is reflink makes materialize cheap (O(dirty)).

2. **Zero is NOT special.** A "still-zero" page is just *unchanged from the previous
   state*. Holes are born once at Firecracker's sparse Diff snapshot (+ ballooning's
   `MADV_DONTNEED`) and **propagate by reflink** — never re-derived by scanning for
   zeros. The real axis is **cache-hit vs cache-miss**, not zero vs non-zero; zero is
   just the subtree every machine trivially has. → We deleted all "canonical-zero"
   machinery (per-level zero-subtree constants, `is_full_subtree`, hole-aware
   assemble). Content-addressing dedups zeros for free.

3. **Big files, not small files.** Dedup requires the *hashes* (the tree), not the
   page *bytes* stored as small blobs. So bytes live once in contiguous reflink-chained
   images; the tree is a lightweight index over them. File-per-blob CAS is slow
   (see §6 — base build ~20 s, `update_ms` ~300–660 ms on btrfs) and pathological over
   a network.

4. **Reflink everywhere, patches on the wire.** Capture reflinks the parent image;
   the server stores reflink-chained images on *its* local fs; the client reflinks
   *its* local ancestor. The network carries a **patch** (offsets → bytes), never
   filesystem operations — so reflink stays local on both ends (where it works) and
   reflink-over-network is never needed.

5. **Bandwidth is the bottleneck.** The tree-diff (content hashes) is what saves the
   wire — it lets a machine skip any content it already holds, including cross-lineage.

---

## 4. The phase plan

| Phase | What | Status |
|---|---|---|
| **P1** | Tree-diff primitive (`diff_between`) — the index that says what's missing | ✅ done, tested |
| **P2** | Reflink capture (`copy → FICLONE`) — child image O(dirty) | ✅ done, validated on btrfs |
| **P3** | Content-addressed materialize + `hash→(image,offset)` index — reflink-by-content | ✅ done, validated (`recon_ok=true`) |
| **—** | Drop the `verify` shadow; reflink the static frame files too | ✅ done |
| **P4** | Remote store + want/have patch transport (the distributed payoff) | **NEXT** (designed, not built) |
| **P5** | Affinity + pre-warm scheduling (makes the *average* hit budget) | future |
| **parallel** | Ballooning (makes a 16 GiB guest's live footprint ~200 MiB of holes) | not built |

**Milestone reached 2026-05-29:** P1–P3 are the single-machine "local foundation,"
validated end-to-end with real numbers — the deliberate "stop and measure before
architecting P4" checkpoint.

---

## 5. What's implemented (code map)

### `store` crate (cross-platform; `cargo test -p store` → 32 tests pass)

`store/src/cas.rs`
- `Hash` — blake3 digest (32 bytes), `Hash::of(bytes)`.
- `Cas` trait — `put` / `get` / `has`, all returning `Send` futures.
- `LocalCas` — filesystem CAS, blob at `<root>/<aa>/<bb>/<full-hex>` (file-per-blob;
  see §6 for its cost). Designed to be swapped for a remote backend.

`store/src/memtree.rs`
- Tree build/restore: `build`, `build_from_reader`, `build_from_path`, `fold_to_root`,
  `assemble`, plus `update`/`rebuild` (O(dirty) parent→child; concurrent leaf puts +
  concurrent sibling rebuild for Send-safe fan-out).
- **P1 — tree-diff:**
  - `Missing { hash, level, page_base }` — a blob the holder lacks + where it sits
    (level 0 = leaf at `page_base*PAGE`; ≥1 = inner node spanning `FANOUT^level` pages).
  - `Diff { missing: Vec<Missing> }`, `Diff::missing_blobs()`.
  - `diff_between<S: Cas, H: Cas>(src, holder, tree)` — walk top-down, prune any
    subtree the **holder** has (`has`), expand misses by reading nodes from **src**
    (`get`). `diff(cas, tree)` = `diff_between(cas, cas, tree)` (trivially empty —
    plumbing check). `diff_node` is the boxed recursive `Send` walker.
  - Tests: `diff_self_is_empty`, `diff_against_empty_holder_is_all_blobs`,
    `diff_lineage_delta` (the core claim: `missing == blobs(F) \ blobs(X)`),
    `diff_placement_round_trips`, `diff_zero_len`, `diff_future_is_spawnable`.
- **P3 — materialize planner:**
  - `Located<S> { source, src_page_base }`, `Locate` trait (`type Source: Copy+Send;
    fn locate(&self, hash) -> Option<Located<Source>>`).
  - `Op<S>` = `Clone { src, dest_page_base, pages }` | `Gap { hash, dest_page_base,
    pages }`.
  - `plan_materialize<C: Cas, L: Locate>(nodes, tree, loc) -> Vec<Op>` — walk F's tree;
    a subtree whose hash the locator knows → one `Clone`; else descend (recovering
    cross-lineage sub-matches) and emit `Gap` for absent leaves. `subtree_pages`,
    `plan_node` helpers.
  - `index_blobs(cas, tree) -> Vec<(Hash, level, page_base)>` — enumerate a tree's
    blobs with positions, to build a content index.
  - Tests: `plan_all_local_is_one_clone`, `plan_nothing_local_is_all_gaps`,
    `plan_cross_offset_clone_round_trips` (cross-lineage clone by hash reconstructs F),
    `plan_future_is_spawnable`, `index_blobs_covers_tree_and_locates_pages`.

### `host` crate (Linux-only — libc/Firecracker; can't compile on the Windows dev box)

`host/src/ops.rs`
- `snapshot_into_frame` — capture orchestration; `FrameInputs::Fresh | Parent`.
- `ingest_full` (was `shadow_ingest_full`) — fresh-build path; builds the tree from
  the full mem. **`verify` removed** (no more assemble-and-byte-compare).
- `diff_ingest` — step path: `reflink_or_copy(parent.mem, child_mem)` + `apply_dirty`
  + `update` the tree. Logs `copy_ms / patch_ms / update_ms`. **`verify` removed.**
  Contains the gated reconstruct measurement (below).
- **P2:** `reflink_or_copy(src, dst) -> io::Result<bool>` — FICLONE
  (`const FICLONE = 0x4004_9409`) + full-copy fallback on `EOPNOTSUPP/EXDEV/ENOTTY/
  EINVAL`, `Ok(false)` → caller warns once. `reflink_or_copy_quiet` — same, quiet,
  used for the static frame files (kernel/initrd/store_disk/cmdline/snapshot), which
  now reflink from the parent instead of full-copying.
- Helpers: `read_dirty_pages` (SEEK_DATA/HOLE on the sparse Diff), `apply_dirty`
  (`write_all_at` the dirty pages), `copy`, `files_equal`.

`host/src/materialize.rs` (P3 host side; `mod materialize;` in `main.rs`)
- `LocalIndex { images: Vec<PathBuf>, map: HashMap<Hash, Located<u32>> }` —
  `add_image(cas, image, tree)` indexes every blob via `index_blobs`; implements
  `Locate` (Source = u32 index into `images`).
- `materialize_local(nodes, tree, index, dest)` — plan + execute; errors on any Gap
  (no network yet).
- `reconstruct(cas, tree, index, dest) -> ReconStats` — like materialize but **fills
  gaps from the CAS** (the single-machine stand-in for the P4 network fetch).
  `ReconStats { plan_ms, fetch_ms, exec_ms, cloned_pages, gap_pages }`.
- `clone_range(src, dst, src_off, len, dst_off)` — FICLONERANGE
  (`const FICLONERANGE = 0x4020_940D`, `struct FileCloneRange`) + read/write copy
  fallback when reflink unsupported or length not page-aligned.
- Tests: `materialize_from_two_local_sources` (cross-image reflink-by-content
  reconstructs the target), `materialize_errors_on_gap`.
- Driven by `CRADLE_RECONSTRUCT_TEST=1` → the gated block in `diff_ingest`
  reconstructs the just-captured child from the parent's image + CAS into a temp,
  byte-compares (`recon_ok`), logs the breakdown.

---

## 6. Measured results (on a btrfs loopback, 1 GiB guest)

**The journey:**

| stage | total step | snapshotting | notes |
|---|---|---|---|
| start of session | 5.80 s | 5258 ms | `copy_ms ~490–625`, `verify_ms ~2725` |
| drop `verify` | 3.00 s | ~2622 ms | (with recon measurement on; ~1.9 s without) |
| reflink static files | **~0.72–0.85 s** | **~339–472 ms** | the win |

**On btrfs, reflink fires** (`copy_ms = 0`, no warn). On tmpfs (`/tmp`) it falls back
to a full copy — that's why the store must be on a reflink fs (btrfs/xfs/zfs).

**Reconstruction (`CRADLE_RECONSTRUCT_TEST=1`), 1 GiB child from parent:**
- `recon_ok = true` — FICLONERANGE reflink-by-content proven byte-identical.
- `cloned_pages ≈ 260351` / `gap_pages ≈ 1713–1793` of 262144 → **~99.3% reflinked
  locally; only ~6.7 MiB filled.** That ~6.7 MiB is what would cross the network on a
  warm cross-machine restore (~64 ms at 1 Gbps).
- `plan_ms ~26–33`, `fetch_ms ~125–129` (gap pages from CAS), `exec_ms ~216–266`
  (reflink clones + gap writes), `index_ms ~324–411` (building the parent index —
  a setup cost that would be persistent/amortized in real use, not per-restore).

**What now dominates the ~340 ms snapshot:** `update_ms ~291–328` — the CAS tree
write. It's the **file-per-blob CAS on a COW fs**: temp+rename per 4 KiB blob, ~140
µs/page; a fresh full 1 GiB build is `build_ms ~19–20 s` (262k tiny blobs). This is
exactly what the packed/contiguous storage replaces; it's the last local lever
(batch writes / drop per-blob fsync+rename) but we're already inside budget.

**Start side is already tiny:** Firecracker `load snapshot` ~10 ms, restoring ~11–16
ms; `attaching`/`evaluating` (~330–380 ms) is the guest *running the command*, not
cradle overhead.

---

## 7. Remote store (P4) — design discussion + conclusions

The question: how does a "remote machine" store and serve frames?

### Reflink-over-network is the central constraint
- **Reads can be a mount.** A client computes its miss-set from the tree and `pread`s
  the missing byte-ranges of the store's image files (NFS/9p). Dumb byte-server; all
  the intelligence is client-side. Works *if* the store holds contiguous images (range
  reads on one file), not file-per-blob (RTT per tiny file = death).
- **Writes + reflink do NOT cross the network.** FICLONE/FICLONERANGE are local-fs ops
  (NFSv4.2 CLONE exists but is spotty). So reflink-chaining must happen on whichever
  machine owns the fs. → capture stays local (fast, local reflink), frames are
  *replicated* to the store, which reflink-chains on *its own* local fs.

### Why NOT opaque S3 / object storage
S3 has **no reflink / no block sharing between objects**. That forces a lose-lose:
- **Full-image objects:** no dedup *and* no sparseness (objects have no holes) → a
  mostly-zero 16 GiB guest stores 16 GiB per frame. Catastrophic.
- **Content-addressed chunk objects:** dedup, but you must pick a chunk size and both
  are bad:
  - small (4 KiB): great dedup/delta, but per-object overhead/latency, no multi-get.
  - coarse (256 KiB+): good read throughput, but **murders the warm delta** — a
    scattered 4 KiB change dirties its whole chunk, turning the measured ~6.7 MiB
    delta into ~600 MiB (64× amplification).
- Reflink dodges this because it's 4 KiB-granular sharing *under* a contiguous file —
  fine delta *and* contiguous reads at once. Object storage can't.
- (Cold-read *throughput* on S3 is fine with coarse chunks + parallelism — it
  saturates 1 Gbps. The real casualty is the warm-delta efficiency.)
- Note: "S3" would also have to be self-hosted on the LAN (MinIO/Garage), not
  AWS-over-internet (latency/egress).

**Conclusion:** don't use opaque object storage for the store. Ship a **thin store
daemon** that keeps reflink-chained images on its *local* btrfs/xfs and serves a
patch protocol (optionally an S3-style HTTP range API for ergonomics). Deploy story:
"run `cradle-store` on a box with a btrfs/xfs data dir" — one binary, still easy.

### The transport: want/have patch protocol (Git/rsync-shaped)
Instead of dumb-store + client range-reads, the client ships its *intent* and gets a
*patch* back. The patch = `diff_between(src = F, holder = client's have)` with bytes
attached. The wire carries `(offset → bytes)`; the client reflinks its local ancestor
and applies the patch — reflink stays local, network is just bytes.

**Expressing "what the client has" — the spectrum (cheapest complexity first):**

1. **Exact 2-RT (recommended for the LAN).** RT1: get F's tree (+ optionally the
   ancestral delta via an ancestor ref). Worker diffs the tree against its *real*
   local cache (exact, cross-lineage-aware, no approximation). RT2: request exactly
   the missing hashes; store streams them. **Zero wasted bytes, no bloom, no false
   positives.** Only cost is one extra round trip — and on a sub-ms LAN that's
   negligible. *Key realization:* you can't enumerate what you need until you have the
   tree (which lives on the store), so any "request what I need" model is inherently
   get-tree-then-request = 2 RT.

2. **Ancestor-only, 1-RT.** Worker: "I have ancestor X, want F." Store sends tree +
   the full X→F delta; worker is complete. Re-sends any cross-lineage content the
   worker already had (redundancy = the overlap). Good when the delta is small (warm)
   or cross-lineage overlap is low. Ancestor ref is exact and ~free — keep it.

3. **Ancestor + Bloom, 1-RT.** Worker also sends a Bloom filter of the hashes it holds
   (a probabilistic projection of its `hash→location` index — nodes + leaves, so the
   store can prune at coarse granularity). Store prunes `X-tree ∪ bloom`. Catches the
   cross-lineage redundancy that the ancestor misses.
   - **Correctness gotcha:** Bloom *false positives* make the store *omit* content the
     client lacks → a hole. (Blooms never false-negative, so "absent" is safe to
     send.) So the bloom must be *verified client-side*: during materialize, any hash
     neither in the local index nor in the patch is an FP gap → a small RT2 for those
     exact hashes. Worst case 2 RT, normally 1.
   - **Only worth it** when the delta is *big* AND cross-lineage overlap is *high* AND
     round trips are *expensive* — i.e., a WAN cold start. **Not the LAN case.**

**Decision for the LAN:** start with **exact 2-RT** (option 1), keep the **ancestor
ref** (exact, cheap, pre-bundles the ancestral bulk in RT1), and **defer the Bloom**
to a WAN/high-overlap regime. The Bloom trades exactness + simplicity to save ~1 ms;
bad deal on a LAN.

### How it slots into existing code
Client materialize stays `plan_materialize(F_tree, local_index)`:
- `Clone` ops (located locally — ancestral via the ancestor's image, cross-lineage via
  the index) → local reflink (`clone_range`).
- `Gap` ops → filled by the store's patch stream (instead of the local CAS as in the
  `reconstruct` measurement).
The store's patch is exactly the Gap set, computed server-side via the want/have. The
`Cas` trait is the seam: local-fs impl (reflink, warm tier) + remote impl (the daemon).

---

## 8. Open questions (settle as P4/P5 progress)
1. **Local CAS write cost / file-per-blob.** `update_ms ~300 ms`, base build ~20 s on
   btrfs. The packed/contiguous-image storage replaces it; interim: batch writes,
   drop per-blob fsync+rename.
2. **Client cache eviction** + how the `hash→(image,offset)` index is persisted and
   kept incremental (naive rebuild was ~400 ms/GiB → don't do it per restore).
3. **Capture-local-then-replicate vs capture-direct-to-store.** Local capture keeps
   the ~340 ms number and keeps reflink local; favor it.
4. **Dedicated store box** (7th machine, or one designated) vs peer — topology favors
   dedicated (store isn't the bottleneck).
5. **Reflink chain depth** before compaction (periodically re-materialize a full image
   to bound fs metadata).
6. **Affinity scoring** (P5): rank machines by lineage locality / working-set overlap.
7. **Ballooning** + the freed-page→zero handling: capture's change set must be
   `(write-dirty) ∪ (balloon-freed → zero)`, since balloon-freeing changes content
   without a guest write (dirty-tracking would miss it). Confirm whether Firecracker's
   Diff already represents freed pages as holes, or we fold the free-page report in.

---

## 9. Test harness — `recon-test.sh`
Sets up a btrfs loopback (reflink works even on a tmpfs-backed image) and launches the
host with the store on btrfs. Env overrides: `CRADLE_IMG` / `CRADLE_MNT` /
`CRADLE_SIZE` (default 32G). **Idempotent — skips setup if already mounted/created**,
so to resize you must `sudo umount /mnt/cradle && rm -f /var/tmp/cradle-btrfs.img`
first.

- Clean snapshot timing: run **without** `CRADLE_RECONSTRUCT_TEST` (the measurement
  block otherwise pads the snapshot phase).
- Reconstruction timing: run **with** `CRADLE_RECONSTRUCT_TEST=1` and read the
  `reconstruct measurement …` line (`exec_ms`, `cloned_pages`, `gap_pages`,
  `recon_ok`).
- Reflink confirmed when `frame store root: /mnt/cradle/…`, no `mem reflink
  unsupported` warn, and `copy_ms ≈ 0`.

Verify the store crate anytime: `cargo test -p store` (32 tests, cross-platform).
The host crate is Linux-only — build/test it on Linux.

---

## 10. One-line status
Two-tier store landed (2026-07-04): local scratch (btrfs loopback on the dev box's
SSD pool) + central `ContentStore` (pack-file `DirStore` on tank). Frame ids commit
before they're returned, frames survive host restarts (cold fetch rematerializes
from central), cross-lineage dedup measurable in shrinking `uploaded_blobs`. Warm
step ~1.7 s end-to-end; §11 is the plan to ~100 ms. The P4 daemon remains next for
fleet — it slots behind the existing `ContentStore` trait.

---

## 11. Central-store semantics + step-latency plan (2026-07-04)

Decisions from the two-tier build-out, recorded so the daemon inherits a spec.

### The commit contract (absolute)
`ContentStore::commit` returning MEANS the frame is durable. Not applied, not
weak — a frame id is only released after commit returns, and any machine may
cash it at any later time. No ack tiers in prod, ever. How *cheaply* the
receiver honors this is its own business: NVMe journal, group commit across
workers (one fsync covers every commit in the same few ms — only exists once
there's a single choke point), replication. Worker-visible commit time trends
to `1 RTT + miss_bytes/bandwidth + receiver's journal fsync`.

Corollary: `commit` must be blind-retryable — networks lose acks after the
work succeeded; content addressing + record id make replay a no-op.

### DirStore (dev backing) knowingly breaks the contract
No fsync anywhere; durability rides the next ZFS txg (~5 s). Machine crash
(power/kernel, NOT process death) revokes the newest ~5 s of acked frames.
Loss is suffix-shaped — ZFS commits atomic prefixes of the per-dataset op
stream, so a lineage rolls back to an earlier tip, never gets holes, and a
visible record never references missing bytes. Accepted for dev: recovery is
"re-run the last command". Structural safety is a hard ZFS coupling — on a
reordering fs (ext4/xfs) even the no-fsync structure would be unsafe.

### Measured commit anatomy (kokuzo, 1 GiB guest, `ls` step, ~13 MB dirty)
want/have tree diff ~100–200 ms (file-per-node reads — dies with node packs)
→ assemble ~50 ms → pack write ~50–100 ms buffered → terminal fsync on raw
tank ~500–900 ms (~9 MB/s sync). The fsync line is what the contract costs on
spinning rust; with it deleted (above), commit ≈ walk + assemble + buffered
write. History: 5 barriers → 1 (ZFS ordering) → 0 (dev exemption).

### Plan to ~100 ms snapshotting (phases; fleet work stays deferred)
Measured step anatomy: restore 5 ms · guest ~260 ms · fc diff ~150 ms ·
ingest ~280 ms · commit (no fsync) ~200–350 ms · plumbing ~50 ms.
1. **Code (in flight):** sub-span timing in the capture path; node packs
   (append-only log + in-RAM `hash→(seg,off)` index, verify-on-read; kills
   ~400 file-create/rename ceremonies per step); ext4 jail-output scratch
   (fc's sparse 4 KiB writes were paying btrfs CoW extent churn); commit
   pages from memory + child-image patch moved off the critical path behind
   a per-frame ready gate. Target: fc 40–80 ms, update 20–40 ms, walk ~10 ms
   → snapshotting ~120–190 ms.
2. **(dissolved by the dev exemption — was: make the fsync cheap via SLOG.**
   sdo can still be split L2ARC/SLOG later as a NAS improvement; ~16–32 GB
   is the right SLOG size, not 2 TB.)
3. **Dirty-set reduction:** free-page reporting via the balloon (see §8.7 —
   freed-page semantics must fold into the change set). ~13 MB dirty for
   `ls` is mostly kernel bookkeeping; 2–5× shrink multiplies through fc
   write, hashing, upload, and the fleet's 1 Gbps math. This carries
   ~150 ms → ≤100 ms.

### Blade memory rule (fleet worker profile: ~32 GB RAM, 0.5–2 TB SSD)
Host RAM holds O(index); storage holds O(history); the page cache adapts —
it's evictable under guest pressure, anonymous host memory isn't. Rejected on
this rule: in-RAM node store, tmpfs snapshot outputs. Node-pack index ~80 B/
node; coarse (L1-subtree) global content index + fine per-lineage lazy index
is the persistent-cache shape (leaf-level global index is ~400 MB at 20
images — over budget; coarse catches runs (zeros, kernel text), lineage-fine
keeps warm fetches O(dirty), scattered cross-lineage singles degrade to
batched 4 KiB fetches).

### Local-tier persistence (agreed direction, not yet built)
Frames/*images* are already the persistent page store (reflink = zero page &
kernel text stored once, shared everywhere) — persistence = stop wiping the
tier + startup reconciliation + eviction (lineage-LRU; btrfs frees extents
when the last referrer dies). Node packs persist with verify-on-read: a
CAS-backed cache needs no crash consistency — torn entry = miss = refetch.
Deferred with eviction/affinity/prefetch until single-machine is done.
