---
phase: 3
slug: p03
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-07-18
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Sourced from `03-RESEARCH.md` § Validation Architecture.
> Task IDs aligned with `03-0{0,1,2,3}-PLAN.md`.

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
| 03-00-T1 | 00 | 0 | VEH-05 | T-03-02 | Dashboard path checks; localhost defaults | smoke | `bash scripts/verify_dashboard.sh` (expect ≠0 W0) | ❌→W0 | ⬜ pending |
| 03-00-T2 | 00 | 0 | VEH-06 | T-03-03 | Scripts fail non-zero when stub/infra down | script | `bash -n` + direct exec ≠0 | ❌→W0 | ⬜ pending |
| 03-00-T3 | 00 | 0 | VEH-05 | T-03-01/SC | Provisioning YAML + SSOT plugin row | config | `docker compose config -q` + file greps | ❌→W0 | ⬜ pending |
| 03-01-T1 | 01 | 1 | VEH-05 | T-03-04 | CH metrics insert constant table; vin reject quotes | unit+e2e | `mvn -Dtest=EventCountAggTest test` + CH count | ❌→01 | ⬜ pending |
| 03-01-T2 | 01 | 1 | VEH-05 | T-03-06 | Dashboard JSON no secrets; DS via provisioning | smoke | `bash scripts/verify_dashboard.sh` | ❌→01 | ⬜ pending |
| 03-01-T3 | 01 | 1 | VEH-05 | — | Thresholds documented as demo defaults | doc | grep ANOMALY-THRESHOLDS | ❌→01 | ⬜ pending |
| 03-02-T1 | 02 | 2 | VEH-06 | T-03-07 | loadtest defaults localhost:9094 | e2e | `bash scripts/loadtest.sh` → baseline.md | ❌→02 | ⬜ pending |
| 03-02-T2 | 02 | 2 | VEH-06 | T-03-08 | Stall→recover; CH MATCH via verify.sh | e2e | `bash scripts/drill_watermark_stall.sh` | ❌→02 | ⬜ pending |
| 03-03-T1 | 03 | 3 | VEH-07 | — | ADR/ARCHITECTURE paths openable | doc | `test -f docs/adr/0001-*.md` | ❌→03 | ⬜ pending |
| 03-03-T2 | 03 | 3 | VEH-07 | T-03-10/12 | RESUME cites measured paths; CH authority | doc+qa | `test -f RESUME.md` + `qa_check.sh` | ❌→03 | ⬜ pending |
| * | * | * | VEH-07 | T-03-08 | CEP verify still CH authority | regression | `PATTERN_ID=HARSH_THEN_FAULT bash scripts/verify.sh` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `projects/p03-vehicle-monitoring/scripts/verify_dashboard.sh` — dashboard file + Grafana API datasource/dashboard checks (VEH-05/D-15)
- [ ] `projects/p03-vehicle-monitoring/scripts/loadtest.sh` + `scripts/drill_watermark_stall.sh` — fail non-zero skeletons (VEH-06)
- [ ] Unit test stub for window `AggregateFunction` accumulator (`EventCountAggTest`)
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

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 300s
- [x] `nyquist_compliant: true` set in frontmatter after plans land

**Approval:** pending
