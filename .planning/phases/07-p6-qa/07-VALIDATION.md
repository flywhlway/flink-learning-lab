---
phase: 7
slug: p6-qa
status: verified
nyquist_compliant: true
wave_0_complete: true
created: 2026-07-19
verified: 2026-07-18T16:58:00Z
---

# Phase 7 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | 仓库门禁脚本（bash）+ Python 计数；无 JUnit 强制 |
| **Config file** | `scripts/qa_check.sh` + `scripts/eng_audit.sh` |
| **Quick run command** | `grep -rl --include='*.java' 'public static void main' examples \| wc -l`；文档行数快检 |
| **Full suite command** | `bash scripts/qa_check.sh` && `bash scripts/eng_audit.sh` |
| **Estimated runtime** | ~60–180s（含 mvn compile） |

---

## Sampling Rate

- **After every task commit:** mains 快检；相关模块 `mvn -pl … -am -DskipTests compile`
- **After every plan wave:** `bash scripts/qa_check.sh`（Wave 1 允许红；Wave 4 必须绿）
- **Before `/gsd-verify-work`:** qa_check + eng_audit 双绿
- **Max feedback latency:** 180 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 07-00-01 | 00 | 0 | QA-01 | T-07-01 | 门禁脚本不可被绕过假绿 | gate | `bash scripts/qa_check.sh` | ✅ | ✅ green |
| 07-00-02 | 00 | 0 | ENG-01…04 | T-07-02 | ENG 审计可复现 | audit | `bash scripts/eng_audit.sh` | ✅ | ✅ green |
| 07-01-* | 01 | 1 | QA-02 | — | mains≥100 可编译 | gate | qa_check ④ + mvn | ✅ | ✅ green |
| 07-02-* | 02 | 2 | QA-02 | — | md lines≥30000 | gate | qa_check 行数段 | ✅ | ✅ green |
| 07-03-* | 03 | 3 | QA-01/02 ENG | — | 终态全绿 + 状态一致 | smoke | qa_check + eng_audit | ✅ | ✅ green |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [x] 升级 `scripts/qa_check.sh`：案例≥100、文档≥30000、compile 硬失败、违禁词含「省略」
- [x] 新增 `scripts/eng_audit.sh` 覆盖 ENG-01…04
- [x] 更新 `scripts/README.md` 索引
- [x] 清洗存量「省略」误杀点（再启用词表）
- [x] 可选 `scripts/count_docs.py` 分目录诊断

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions | Verifier result |
|----------|-------------|------------|-------------------|-----------------|
| OrbStack arm64 实测全绿 | D-09 / ENG-03 | 沙箱不可替代本机 Docker/mvn | 在 OrbStack 本机执行双绿 | ✅ 本机 arm64 + orbstack；qa_check/eng_audit exit 0 |
| README/PHASES 完成态措辞 | QA-02 | 语义一致性需人读 | 对照 PHASES P6 行与根 README | ✅ 措辞对齐；未打 P6 tag（D-12） |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 180s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** verified by gsd-verifier 2026-07-18T16:58:00Z — dual green on OrbStack arm64 (mains=100, md=30697)
