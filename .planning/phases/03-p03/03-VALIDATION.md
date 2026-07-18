---
phase: 3
slug: p03
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-07-18
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Sourced from `03-RESEARCH.md` § Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | JUnit 5.10.2 + Maven Surefire 3.2.5 (existing p03) |
| **Config file** | `projects/p03-vehicle-monitoring/pom.xml` surefire plugin |
| **Quick run command** | `cd projects/p03-vehicle-monitoring && mvn -q test` |
| **Full suite command** | `mvn -q test` + `bash scripts/verify.sh` + `verify_dashboard.sh` + `loadtest.sh` + `drill_watermark_stall.sh` (OrbStack) |
| **Estimated runtime** | ~120–300 seconds (unit quick; e2e/drills longer) |

---

## Sampling Rate

- **After every task commit:** Run `cd projects/p03-vehicle-monitoring && mvn -q test`
- **After every plan wave:** Unit tests + `bash scripts/verify.sh` (CEP regression)
- **Before `/gsd-verify-work`:** Full suite must be green on OrbStack
- **Max feedback latency:** 300 seconds for phase-gate scripts

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| TBD | 00 | 0 | VEH-05 | T-03-01 | Dashboard JSON + DS checks only; no secrets in JSON | smoke | `bash scripts/verify_dashboard.sh` | ❌ W0 | ⬜ pending |
| TBD | 00 | 0 | VEH-06 | — | Scripts fail non-zero when infra down | script | `bash -n scripts/loadtest.sh` / drill | ❌ W0 | ⬜ pending |
| TBD | 01+ | 1+ | VEH-05 | T-03-02 | CH metrics insert uses constant table; no string concat from events | e2e | CH `count() FROM vehicle_window_metrics >= 1` | ❌ W0 | ⬜ pending |
| TBD | 02+ | 2+ | VEH-06 | — | Stall→recover exits 0; MATCH after recover | e2e | `bash scripts/drill_watermark_stall.sh` | ❌ W0 | ⬜ pending |
| TBD | 02+ | 2+ | VEH-06 | — | baseline.md has env snapshot + metrics table | artifact | grep baseline headings | ❌ W0 | ⬜ pending |
| TBD | 03+ | 3+ | VEH-07 | — | ADR/RESUME/ARCHITECTURE paths openable | doc | `test -f docs/adr/0001-*.md` etc. | ❌ W0 | ⬜ pending |
| TBD | * | * | VEH-07 | T-03-01 | CEP verify still CH authority | regression | `PATTERN_ID=HARSH_THEN_FAULT bash scripts/verify.sh` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Task IDs filled by planner when PLAN.md files exist.*

---

## Wave 0 Requirements

- [ ] `projects/p03-vehicle-monitoring/scripts/verify_dashboard.sh` — dashboard file + Grafana API datasource/dashboard checks (VEH-05/D-15)
- [ ] `projects/p03-vehicle-monitoring/scripts/loadtest.sh` + `scripts/drill_watermark_stall.sh` — fail non-zero skeletons (VEH-06)
- [ ] Unit test stub for window `AggregateFunction` accumulator (`EventCountAggTest` or equiv)
- [ ] `sql/clickhouse_window_metrics.sql` + p03-init third POST hook
- [ ] Grafana `provisioning/dashboards/dashboards.yml` + ClickHouse datasource YAML + `GF_INSTALL_PLUGINS` (compose)
- Framework install: none — JUnit/Surefire already present

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Grafana panels visually show threshold lines | VEH-05 | API proves provisioning; human confirms panel layout | Open http://localhost:3000 (admin/flinklab), open p03 dashboard, confirm window + anomaly + Flink health panels |
| Flink UI Watermarks column during stall drill | VEH-06 | REST `/watermarks` may return empty; UI is teaching evidence | During drill stall phase, screenshot/note UI Watermarks; after recover confirm MATCH |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 300s
- [ ] `nyquist_compliant: true` set in frontmatter after plans land

**Approval:** pending
