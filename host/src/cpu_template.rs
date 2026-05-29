//! Firecracker CPU template selection.
//!
//! Under nested KVM (WSL2/Hyper-V) some XSAVE-managed CPU state does not
//! round-trip faithfully through `KVM_GET/SET_XSAVE` on snapshot restore: the
//! guest enables those components (in `XCR0` for user state, `IA32_XSS` for
//! supervisor state), and after resume every `XRSTORS` faults `#GP`. The
//! kernel's FPU-restore exception fixup then reloads the *init* FPU state,
//! which `#GP`s the same way, so it recurses until the task stack guard page
//! is hit. Two component families trip this on our hardware:
//! - **AVX-512** (XCR0 bits 5/6/7: opmask, ZMM_Hi256, Hi16_ZMM)
//! - **CET** (XSS bits 11/12: shadow-stack user/supervisor state)
//!
//! Empirically the post-resume `XRSTORS` mask was `0x18e7` (FP/SSE/AVX +
//! AVX-512 + CET); masking AVX-512 dropped it to `0x1807`, and masking CET too
//! brings it to `0x7` (FP/SSE/AVX only), which always round-trips. We mask the
//! guest CPUID so the kernel never enables either family.
//!
//! Mode is chosen at compile time:
//! - **dev** (default): host runs under WSL2 on the laptop (Alder Lake, no
//!   AVX-512) or the desktop (Zen 4, AVX-512 + CET). We mask AVX-512 + CET —
//!   a no-op for what the laptop lacks, the actual restore fix on the desktop,
//!   and the laptop∩desktop xstate common denominator either way.
//! - **prod** (`--features prod`): host runs on the server (Tiger Lake) on
//!   bare-metal KVM, where the XSAVE round-trip works. No masking — native
//!   CPUID, full ISA including AVX-512.
//!
//! The template is applied at fresh boot (the build path); the snapshot then
//! captures the already-masked CPUID, so every descendant restore is
//! consistent. Restores don't re-apply it — Firecracker takes CPUID from the
//! snapshot.

use fctools::vm::models::CpuTemplate;

/// CPU template applied to every VM at fresh boot. `None` passes the host
/// CPUID through unchanged.
#[cfg(feature = "prod")]
pub fn cradle_cpu_template() -> Option<CpuTemplate> {
    // Prod runs only on the server's single CPU on bare-metal KVM, where the
    // nested-virt XSAVE bug doesn't apply. Keep native CPUID for full guest
    // performance (AVX-512 included).
    None
}

#[cfg(not(feature = "prod"))]
pub fn cradle_cpu_template() -> Option<CpuTemplate> {
    use fctools::vm::models::X86CpuTemplate;
    Some(CpuTemplate::X86(X86CpuTemplate {
        kvm_capabilities: Vec::new(),
        msr_modifiers: Vec::new(),
        cpuid_modifiers: dev_cpuid_mask(),
    }))
}

/// CPUID modifiers that strip AVX-512 and CET from the guest: the feature bits
/// in leaf 0x7 (so nothing detects/uses them) and the xstate-component bits in
/// leaf 0xD (so the kernel never enables them in `XCR0`/`IA32_XSS` — the part
/// that actually prevents the post-restore `XRSTORS` `#GP`).
#[cfg(not(feature = "prod"))]
fn dev_cpuid_mask() -> Vec<fctools::vm::models::X86CpuidModifier> {
    use fctools::vm::models::X86CpuidRegister::{Eax, Ebx, Ecx, Edx};
    vec![
        // leaf 0x7, subleaf 0 — AVX-512 + CET feature flags.
        cpuid(
            0x7,
            0x0,
            &[
                // EBX: AVX-512 F=16 DQ=17 IFMA=21 PF=26 ER=27 CD=28 BW=30 VL=31
                (Ebx, &[16, 17, 21, 26, 27, 28, 30, 31]),
                // ECX: AVX-512 VBMI=1 VBMI2=6 VNNI=11 BITALG=12 VPOPCNTDQ=14;
                //      CET_SS (shadow stack) = 7
                (Ecx, &[1, 6, 7, 11, 12, 14]),
                // EDX: AVX-512 4VNNIW=2 4FMAPS=3 VP2INTERSECT=8 FP16=23;
                //      CET_IBT = 20
                (Edx, &[2, 3, 8, 20, 23]),
            ],
        ),
        // leaf 0x7, subleaf 1 — AVX512_BF16=5. NOT AVX_VNNI=4: that's the
        // non-512 VNNI (uses YMM/AVX state, round-trips fine) — leave it.
        cpuid(0x7, 0x1, &[(Eax, &[5])]),
        // leaf 0xD, subleaf 0 — user (XCR0) xstate components. Clearing
        // opmask(5)/ZMM_Hi256(6)/Hi16_ZMM(7) keeps AVX-512 out of XCR0.
        cpuid(0xD, 0x0, &[(Eax, &[5, 6, 7])]),
        // leaf 0xD, subleaf 1 — supervisor (IA32_XSS) xstate components.
        // Clearing CET_USER(11)/CET_SUPERVISOR(12) is what remained in the
        // restore-time XRSTORS mask (0x1807) after AVX-512 was masked; CET
        // doesn't round-trip through nested KVM either.
        cpuid(0xD, 0x1, &[(Ecx, &[11, 12])]),
    ]
}

/// Build one CPUID-leaf modifier from a set of per-register bits to clear.
#[cfg(not(feature = "prod"))]
fn cpuid(
    leaf: u32,
    subleaf: u32,
    regs: &[(fctools::vm::models::X86CpuidRegister, &[u8])],
) -> fctools::vm::models::X86CpuidModifier {
    use fctools::vm::models::{X86CpuidModifier, X86CpuidRegisterModifier};
    X86CpuidModifier {
        leaf: format!("0x{leaf:x}"),
        subleaf: format!("0x{subleaf:x}"),
        // KVM_CPUID_FLAG_SIGNIFICANT_INDEX: leaves 0x7 and 0xD are
        // subleaf-indexed, so KVM tags their entries with this flag and
        // Firecracker matches the modifier on (leaf, subleaf, flags).
        flags: 1,
        modifiers: regs
            .iter()
            .map(|(register, bits)| X86CpuidRegisterModifier {
                register: *register,
                bitmap: clear_bitmap(bits),
            })
            .collect(),
    }
}

/// A Firecracker 32-bit CPUID register bitmap that forces the given bit
/// indices to 0 and passes everything else through. Format: `0b` followed by
/// 32 chars, bit 31 leftmost; `0` = clear, `x` = passthrough.
#[cfg(not(feature = "prod"))]
fn clear_bitmap(bits: &[u8]) -> String {
    let mut chars = [b'x'; 32];
    for &b in bits {
        debug_assert!(b < 32, "cpuid bit index out of range: {b}");
        chars[31 - b as usize] = b'0';
    }
    let mut s = String::with_capacity(34);
    s.push_str("0b");
    s.push_str(std::str::from_utf8(&chars).expect("ascii"));
    s
}

#[cfg(all(test, not(feature = "prod")))]
mod tests {
    use super::*;

    #[test]
    fn bitmap_is_msb_first_and_32_wide() {
        // bit 0 -> rightmost; bit 31 -> just after "0b".
        assert_eq!(clear_bitmap(&[0]), format!("0b{}0", "x".repeat(31)));
        assert_eq!(clear_bitmap(&[31]), format!("0b0{}", "x".repeat(31)));
        // every bitmap we emit is exactly 2 + 32 chars.
        assert_eq!(clear_bitmap(&[5, 6, 7]).len(), 34);
    }

    #[test]
    fn avx512_xstate_components_cleared_in_leaf_d() {
        let leaf_d = dev_cpuid_mask()
            .into_iter()
            .find(|m| m.leaf == "0xd" && m.subleaf == "0x0")
            .expect("leaf 0xD subleaf 0 modifier present");
        let eax = &leaf_d.modifiers[0];
        // bits 5,6,7 cleared => positions 26,25,24 from the left after "0b".
        let body = eax.bitmap.trim_start_matches("0b");
        assert_eq!(body.as_bytes()[31 - 5], b'0');
        assert_eq!(body.as_bytes()[31 - 6], b'0');
        assert_eq!(body.as_bytes()[31 - 7], b'0');
        // x87/SSE/AVX (0,1,2) must stay passthrough.
        assert_eq!(body.as_bytes()[31 - 0], b'x');
        assert_eq!(body.as_bytes()[31 - 2], b'x');
    }
}
