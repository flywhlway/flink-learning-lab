---
phase: 07-p6-qa
reviewed: 2026-07-18T17:00:00Z
depth: standard
files_reviewed: 18
files_reviewed_list:
  - scripts/qa_check.sh
  - scripts/eng_audit.sh
  - scripts/count_docs.py
  - examples/e12-05-streaming-rag-lite/src/main/java/com/flywhl/flinklab/e12/StreamingRagLiteJob.java
  - examples/e12-13-langgraph-mock/src/main/java/com/flywhl/flinklab/e12/LangGraphMockJob.java
  - examples/e12-23-online-learning-sample/src/main/java/com/flywhl/flinklab/e12/OnlineLearningSampleJob.java
  - examples/e01-hello-flink/src/main/java/com/flywhl/flinklab/e01/ProcessingTimeCountJob.java
  - examples/pom.xml
  - interview/L1.md
  - interview/L2.md
  - interview/L3.md
  - interview/L4.md
  - interview/L5.md
  - interview/L6.md
  - interview/L7.md
  - interview/L8.md
  - docs/01-runtime/README.md
  - docs/QA-REPORT.md
findings:
  critical: 1
  warning: 3
  info: 3
  total: 7
status: issues
advisory: true
---

# Phase 7: Code Review Report

**Reviewed:** 2026-07-18T17:00:00Z
**Depth:** standard (advisory — do not block)
**Files Reviewed:** 18
**Status:** issues

## Summary

Advisory review of Phase 7 P6 总装 QA gate scripts, sampled e12/e0x demos, and interview/docs expansions. Thresholds (mains≥100, md≥30000) match CONTEXT D-03/D-06; current counts are files=100 / doc_lines≈30697. One **Critical** gate defect: Markdown link existence check can false-green on `../` paths that escape the repo onto the host filesystem. Remaining items are non-blocking robustness and content-quality notes.

## Critical Issues

### CR-01: Link scanner false-green via path traversal

**File:** `scripts/qa_check.sh:36-37`
**Issue:** Relative link targets are joined as `"$(dirname "$f")/$tgt"` and validated only with `[ -e ... ]`. Enough `../` segments resolve outside the repository; if the host path exists, the gate prints `ok Markdown 相对链接` even though the link is not a valid in-repo document path. Reproduced: from `docs/00-landscape/*.md`, target `../../../../../../../../etc/passwd` resolves to `/etc/passwd` and `[ -e ]` returns true.
**Fix:** Resolve the candidate, require it stays under repo root, then test existence:

```bash
# after tgt="${link%%#*}" and scheme skips
root="$(pwd -P)"
cand="$(cd "$(dirname "$f")" && python3 -c 'import os,sys; print(os.path.realpath(sys.argv[1]))' "$tgt")" \
  || { bad "断链 $f → $link"; LINKFAIL=1; continue; }
case "$cand" in
  "$root"|"$root"/*) ;;
  *) bad "断链(越界) $f → $link"; LINKFAIL=1; continue ;;
esac
[ -e "$cand" ] || { bad "断链 $f → $link"; LINKFAIL=1; }
```

(Or equivalent `realpath` + prefix check without Python.)

## Warnings

### WR-01: Missing eng_audit soft-skips ENG hard contract

**File:** `scripts/qa_check.sh:67-71`
**Issue:** If `scripts/eng_audit.sh` is absent, qa_check only `warn`s and can still exit 0. That is a false-green path relative to D-10 (ENG-01…04 failure must fail the phase). File exists today, so latent rather than active.
**Fix:** Treat missing eng_audit as `bad` (same as failed eng_audit), or always require the script in the six-hard-gate set.

### WR-02: Compose Python fallback under-validates

**File:** `scripts/qa_check.sh:17-19`
**Issue:** When `docker compose` is unavailable, only `docker/docker-compose.yml` is `yaml.safe_load`’d. Override/profile merge errors that `docker compose config` would catch can pass — false green in docker-less CI/sandbox.
**Fix:** Prefer failing hard without docker (`bad "需要 docker compose"`), or parse the same file set compose would merge (compose.yaml + override + `COMPOSE_FILE`).

### WR-03: Interview expansion template padding vs D-05

**File:** `interview/L1.md`–`L8.md` (e.g. `interview/L2.md:610-624`)
**Issue:** Spot-check shows ~230 repeated blocks of「题干关键动作是…」「禁止复读题干」plus near-identical 反例/仓库对照脚手架. Forbidden-word scan is clean (no TODO/FIXME/省略), but this pattern conflicts with D-05「实质内容扩写，禁止注水」and inflates the ≥30k line gate with low-signal text. `docs/01-runtime/README.md` sample is substantive by contrast.
**Fix:** Collapse shared 口述/反例模板 into `interview/README.md` once; keep per-question unique 机制展开 only. Re-run `count_docs.py` after trim to confirm still ≥30000 on real content.

## Info

### IN-01: Case gate counts files, comment says methods

**File:** `scripts/qa_check.sh:44-47`
**Issue:** Comment claims「main 方法数」but `grep -rl ... | wc -l` counts files containing the substring. Currently files=100 and signatures=100, so no active false green; multi-main-in-one-file would under-count (false red).
**Fix:** Align comment with file口径, or switch to counting `public static void main\s*(` matches.

### IN-02: e12/e0x sampled demos are real Jobs

**File:** `examples/e12-05-streaming-rag-lite/.../StreamingRagLiteJob.java`, `e12-13-langgraph-mock/.../LangGraphMockJob.java`, `e12-23-online-learning-sample/.../OnlineLearningSampleJob.java`, `e01-hello-flink/.../ProcessingTimeCountJob.java`
**Issue:** None — samples use Labs datagen, keyed state / Async I/O / side output, uid + `env.execute`; modules registered in `examples/pom.xml`. No empty-shell main found in sample.
**Fix:** N/A

### IN-03: count_docs.py and qa_check ⑤ agree

**File:** `scripts/count_docs.py`, `scripts/qa_check.sh:50-53`
**Issue:** Both report ~30697 lines excl `.planning`/`.git`; newline-edge difference is negligible vs threshold 30000. ENG-01…04 logic and version substring sampling look intentional (e.g. `3.6.0` ⊆ `3.6.0-2.2`).
**Fix:** N/A

---

_Reviewed: 2026-07-18T17:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard (advisory)_
