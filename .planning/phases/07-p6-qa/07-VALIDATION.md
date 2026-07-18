---
phase: 7
slug: p6-qa
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-07-19
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
| 07-00-01 | 00 | 0 | QA-01 | T-07-01 | 门禁脚本不可被绕过假绿 | gate | `bash scripts/qa_check.sh`（升级后） | ❌ W0 | ⬜ pending |
| 07-00-02 | 00 | 0 | ENG-01…04 | T-07-02 | ENG 审计可复现 | audit | `bash scripts/eng_audit.sh` | ❌ W0 | ⬜ pending |
| 07-01-* | 01 | 1 | QA-02 | — | mains≥100 可编译 | gate | qa_check ④ + mvn | ❌ until demos | ⬜ pending |
| 07-02-* | 02 | 2 | QA-02 | — | md lines≥30000 | gate | qa_check 行数段 | ❌ until docs | ⬜ pending |
| 07-03-* | 03 | 3 | QA-01/02 ENG | — | 终态全绿 + 状态一致 | smoke | qa_check + eng_audit | ❌ until final | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] 升级 `scripts/qa_check.sh`：案例≥100、文档≥30000、compile 硬失败、违禁词含「省略」
- [ ] 新增 `scripts/eng_audit.sh` 覆盖 ENG-01…04
- [ ] 更新 `scripts/README.md` 索引
- [ ] 清洗存量「省略」误杀点（再启用词表）
- [ ] 可选 `scripts/count_docs.py` 分目录诊断

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| OrbStack arm64 实测全绿 | D-09 / ENG-03 | 沙箱不可替代本机 Docker/mvn | 在 OrbStack 本机执行 `bash scripts/qa_check.sh` 与 `bash scripts/eng_audit.sh`，确认 exit 0 |
| README/PHASES 完成态措辞 | QA-02 | 语义一致性需人读 | 对照 PHASES P6 行与根 README 完成态表述 |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 180s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
