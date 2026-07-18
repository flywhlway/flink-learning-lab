# Phase 7 Plan Verification — 07-PLAN-CHECK

**Phase:** 7 — P6 总装 QA  
**Plans verified:** 4 (`07-00` … `07-03`)  
**Checked:** 2026-07-19  
**Status:** REVISION NEEDED — 1 blocker；若干 warnings（可带过执行但建议修）

---

## Goal-Backward Summary

| Success Criterion (ROADMAP) | Delivering Plans | Status |
|-----------------------------|------------------|--------|
| `scripts/qa_check.sh` 全绿 | 07-00 六硬门先红 → 07-01/02 抬内容 → 07-03 OrbStack 双绿 | Covered（终态在 03） |
| 案例 ≥100、文档 ≥30k、README/PHASES 一致 | 07-01 mains；07-02 md；07-03 终稿 | Covered |
| ENG-01…04 终检关闭 | 07-00 eng_audit 骨架 → 07-03 严格 + 双绿 | Covered |

| Requirement | Plans (frontmatter) | Covering tasks | Status |
|-------------|---------------------|----------------|--------|
| QA-01 | 07-00, 07-03 | 00-T1（六硬门）；03-T1/T3（清净+全绿） | Covered |
| QA-02 | 07-01, 07-02, 07-03 | 01（案例）；02（文档）；03（状态轴） | Covered |
| ENG-01 | 07-00, 07-03 | 00-T2 抽样；03 终绿 | Covered |
| ENG-02 | 07-00, 07-03 | 00-T2 docs/README；02/03 登记 | Covered |
| ENG-03 | 07-00, 07-03 | 00-T2 证据指针；03 OrbStack+违禁词 | Covered |
| ENG-04 | 07-00, 07-03 | 00-T2 CHANGELOG soft；03-T1/T2 PHASES hard | Covered |

| Decision | Implementing plan/task | Status |
|----------|------------------------|--------|
| D-01 examples mains 口径 | 07-00 T1；07-01 | Covered |
| D-02 真实可编译 Demo / 禁空壳 | 07-01 T1/T2 | Covered |
| D-03 阈值 67→100 | 07-00 T1 | Covered |
| D-04 md excl `.planning` | 07-00 T1/T2；07-02 | Covered |
| D-05 实质扩写禁注水 | 07-02 T1–T3 | Covered |
| D-06 行数 ≥30000 硬门 | 07-00 T1；07-02 T3 | Covered |
| D-07 六硬门 + compile hard fail | 07-00 T1 | Covered |
| D-08 词表含「省略」、不裸「略」、排除集冻结 | 07-00 T1；07-03 T1 | Covered |
| D-09 OrbStack 实测全绿 | 07-03 T3 checkpoint | Covered |
| D-10 eng_audit 可脚本化 | 07-00 T2（独立文件 + qa_check 调用） | Covered |
| D-11 README/PHASES/PROJECT/REQUIREMENTS 终稿 | 07-03 T2 | Covered |
| D-12 本 Phase 不打 tag | 全 plan 明示；03 T1/T2/T3 | Covered |
| D-13 四波顺序先红→案例→文档→终绿 | waves 0→1→2→3；depends_on 链 | Covered |

Deferred（git tag/Release、新生产项目、改案例口径）— 计划显式排除，无 scope creep。

---

## Dimension Results

| Dim | Result | Notes |
|-----|--------|-------|
| 1 Requirement Coverage | ✅ PASS | QA-01/02、ENG-01…04 均在 frontmatter + 有具体任务 |
| 2 Task Completeness | ⚠️ WARN | `verify.plan-structure` 四 plan valid；read_first/acceptance_criteria/done 齐全；07-03 T1 verify 偏弱（见 issues） |
| 3 Dependency Correctness | ✅ PASS | `[] → 07-00 → 07-01 → 07-02 → 07-03`；无环；与 D-13 一致（frontmatter wave 0/1/2/3） |
| 4 Key Links Planned | ✅ PASS | qa_check↔eng_audit；pom↔modules；docs↔examples；PHASES↔实测 |
| 5 Scope Sanity | ⚠️ WARN | tasks/plan ≤3；07-01 单 plan 约 11 新模块（~30+ 文件）超 15 文件阈值——建议拆或强制分批提交（action 已写「逐模块 compile 再提交」） |
| 6 Verification Derivation | ✅ PASS | must_haves 用户可观察（门禁红/绿、mains、行数、双绿）；artifacts/key_links 齐全 |
| 7 Context Compliance | ✅ PASS | D-01…D-13 均有任务；无 deferred 偷渡；无决策矛盾 |
| 7b Scope Reduction | ✅ PASS | Wave0 ENG-04 soft / e12-10 可选跳过均为 Discretion 或 RESEARCH 切片；无静默缩水锁定决策 |
| 7c Architectural Tier | ✅ PASS | 对照 RESEARCH Responsibility Map：脚本=CLI；Demo=Flink；文档=静态；OrbStack=本机 |
| 8 Nyquist Compliance | ⚠️ WARN | VALIDATION 存在且任务均有 `<automated>`；见下方 Detail（8b 延迟；Open Questions 另见 Dim 11） |
| 9 Cross-Plan Data Contracts | ✅ PASS | 阈值 00 设定 → 01/02 抬内容 → 03 收绿；无冲突变换 |
| 10 .cursor/rules/ | SKIPPED | 无 `.cursor/rules/`；CLAUDE.md SSOT/OrbStack/禁伪绿已体现在计划 |
| 11 Research Resolution | ❌ FAIL | `## Open Questions` **无** `(RESOLVED)` 后缀；Q1–Q3 仅有 Recommendation、无 inline `RESOLVED` |
| 12 Pattern Compliance | ✅ PASS | 各 plan context/read_first 引用 `07-PATTERNS.md`；e12-01 / count_interview / P5 终稿样板对齐 |

Threat models: 四份 PLAN 均含 Trust Boundaries + STRIDE register → ✅

### Dimension 8 Detail

| Check | Status |
|-------|--------|
| 8e VALIDATION.md exists | ✅ `07-VALIDATION.md` present |
| 8a Automated verify | ✅ 全部 auto task 含 `<automated>`；03-T3 另有 human-check |
| 8b Feedback latency | ⚠️ 含 `mvn compile` / 全量 `qa_check` 可 60–180s（>30s）——可接受；无 `--watch` |
| 8c Sampling continuity | ✅ 各 wave 无 3 连续无 automated |
| 8d Wave 0 completeness | ✅ qa_check/eng_audit/count_docs 与 VALIDATION Wave 0 Requirements 对齐 |

| Task | Plan | Wave | Automated Command | Status |
|------|------|------|-------------------|--------|
| T1 | 07-00 | 0 | `bash -n qa_check` + 省略清净 + qa_check exit≠0 | ✅ |
| T2 | 07-00 | 0 | eng_audit + count_docs + README 索引 | ✅ |
| T1 | 07-01 | 1 | 11 模块 `mvn -pl … compile` + mains wc | ✅ |
| T2 | 07-01 | 1 | mains≥100 + 全 examples compile | ✅ |
| T3 | 07-01 | 1 | qa_check 解读（案例/compile 绿、文档可红） | ✅ |
| T1 | 07-02 | 2 | count_docs + docs 行数中间门槛 | ✅ |
| T2 | 07-02 | 2 | count_interview + interview 行数 | ✅ |
| T3 | 07-02 | 2 | 全仓 md≥30000 + qa_check ⑤ | ✅ |
| T1 | 07-03 | 3 | qa_check（②③ 清净；ENG-04 硬化后可能短暂红） | ⚠️ 见 blocker/warning |
| T2 | 07-03 | 3 | PHASES/CHANGELOG/REQUIREMENTS grep | ✅ |
| T3 | 07-03 | 3 | OrbStack 双绿 + mains/行数 + human | ✅ |

Sampling: Wave 0–3 均 ✅  
Wave 0: 门禁脚本清单 → ✅ present in VALIDATION + 07-00  
Overall Nyquist structure: ✅（latency WARN）；Research Resolution 单独 FAIL

---

## Structured Issues

```yaml
issues:
  - plan: null
    dimension: research_resolution
    severity: blocker
    description: >
      07-RESEARCH.md「## Open Questions」未标 (RESOLVED)；Q1（eng_audit 独立）、
      Q2（QA-REPORT 可选）、Q3（e12-03/04 Java）仅有 Recommendation，无 inline RESOLVED。
      计划已按 Recommendation 落地，但 Dim 11 要求显式关闭后方可执行。
    fix_hint: >
      将标题改为「## Open Questions (RESOLVED)」；每条加「RESOLVED: …」
      （Q1=独立 eng_audit+qa_check 调用；Q2=可选 docs/QA-REPORT；
      Q3=缺口批满则不强制 e12-03/04 Java）。

  - plan: "07-03"
    dimension: task_completeness
    severity: warning
    description: >
      Task 1 升级 eng_audit ENG-04 严格 PHASES 断言后立刻跑全量 qa_check，
      但 PHASES P6 可验证完成态要到 Task 2 才写入——中间态必然 ENG-04 红。
      当前 <automated> 不断言 exit 0 / 不单独断言 ②③ ok，验收依赖「本 plan 内消掉」，
      易造成执行器误判或跳过真实失败。
    task: 1
    fix_hint: >
      将 ENG-04 严格断言挪到 Task 2（PHASES 写入之后）再跑 eng_audit；
      或 Task 1 仅清 ②③、verify 用分段断言（grep ok 违禁词/断链），
      不在硬化 ENG-04 后要求全绿。

  - plan: "07-01"
    dimension: scope_sanity
    severity: warning
    description: >
      单 plan 新建约 11 个 e12 模块（每模块 pom+Job+README）+ Task 2 回填，
      文件面远超 15 阈值；虽 action 要求逐模块 compile 再提交，仍有上下文质量风险。
    metrics:
      tasks: 3
      estimated_new_modules: 11
      estimated_files: "30+"
    fix_hint: >
      可选拆成 07-01a（e12 优先批）与 07-01b（e0x 回填+门禁回归，depends_on 01a）；
      或保持单 plan 但在 SUMMARY/执行纪律中强制分批 commit（已写在 action）。

  - plan: "07-02"
    dimension: scope_sanity
    severity: warning
    description: >
      文档轴单 plan 覆盖 docs/ + interview L1–L8 + ai/chapters + best-practice，
      行数目标 +20k，文件面大；tasks=3 在阈值内，属内容扩写固有风险。
    fix_hint: 保持现切片即可；执行时按 Task 边界提交，避免单 commit 吞全仓 md。

  - plan: null
    dimension: nyquist_compliance
    severity: warning
    description: >
      含 mvn / 全量 qa_check 的 <automated> 反馈延迟可达 60–180s（VALIDATION 已声明）。
      无 watch-mode；可接受但不满足「<30s」理想采样。
    fix_hint: 保留快检（mains wc / count_docs）作任务内前置；全量 qa_check 放 wave 末。
```

---

## Scope Creep Check

| Forbidden / Deferred | In plans? | Verdict |
|----------------------|-----------|---------|
| git tag / GitHub Release | 明确禁止（D-12） | ✅ 无 creep |
| 新生产项目 / 新可观测栈 | 未出现 | ✅ 无 creep |
| 案例口径改 projects / C 标签 | 未出现；D-01 锁定 | ✅ 无 creep |

---

## Concrete Revision Instructions（BLOCK 必须修）

1. **编辑 `07-RESEARCH.md`**
   - 标题改为 `## Open Questions (RESOLVED)`
   - Q1–Q3 各加一行 `RESOLVED: <与 07-00/01/03 一致的结论>`

2. **（强烈建议，非 blocker）编辑 `07-03-PLAN.md` Task 1/2**
   - ENG-04 严格 PHASES 断言与 PHASES 写入同任务或之后；Task 1 verify 勿在硬化后期待全绿

修完 Dim 11 后可再跑 plan-check；warnings 可不挡 `/gsd-execute-phase`。

---

## Recommendation

**1 blocker**（Research Open Questions 未关闭）须修订后再执行。  
需求覆盖、D-01…D-13、威胁模型、D-13 波次、无 scope creep、must_haves 均可接受。

## REVISION NEEDED

- [ ] Mark `07-RESEARCH.md` Open Questions as `(RESOLVED)` with inline RESOLVED on Q1–Q3
- [ ] (Recommended) Fix `07-03` Task 1 ENG-04 vs PHASES ordering / verify strength
