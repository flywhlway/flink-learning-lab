---
phase: 07-p6-qa
plan: 01
subsystem: testing
tags: [examples, mains, e12, flink-2.2.1, datagen, qa-02]

requires:
  - phase: 07-p6-qa
    provides: "六硬门 qa_check（案例阈值 100）与 eng_audit 骨架"
provides:
  - "examples/ public static void main ≥100（实测 100）"
  - "e12 缺口零依赖模块 05/09/11/12/13/14/16/19/20/21/23 进主 pom"
  - "e0x/e11 回填 + e12 第二 Job；全 examples mvn compile 绿"
affects: [07-02, 07-03, QA-02]

tech-stack:
  added: []
  patterns:
    - "e12 零依赖骨架：common + streaming + clients + datagen + connector-base"
    - "同模块多 main 计入 qa_check ④"
    - "CDC Maven 坐标用 3.6.0-2.2；JDBC 用 Central 现网 3.3.0-1.20"

key-files:
  created:
    - examples/e12-05-streaming-rag-lite/
    - examples/e12-09-streaming-tool-call/
    - examples/e12-11-streaming-workflow/
    - examples/e12-12-multi-agent-topology/
    - examples/e12-13-langgraph-mock/
    - examples/e12-14-knowledge-graph-events/
    - examples/e12-16-trace-propagation/
    - examples/e12-19-ai-gateway-route/
    - examples/e12-20-embedding-cache/
    - examples/e12-21-streaming-evaluation/
    - examples/e12-23-online-learning-sample/
  modified:
    - examples/pom.xml
    - examples/README.md
    - ai/README.md
    - README.md
    - examples/e07-connectors/src/main/java/com/flywhl/flinklab/e07/C1KafkaDeliveryMatrixJob.java
    - examples/e07-connectors/src/main/java/com/flywhl/flinklab/e07/C4FileSinkRollingPolicyJob.java
    - examples/e07-connectors/src/main/java/com/flywhl/flinklab/e07/C6ClickHouseHttpSinkJob.java

key-decisions:
  - "跳过 e12-10 MCP（A1），用 e0x/e12 第二 Job 补足 1 个 main"
  - "e08 新 Job 仅 Labs/状态语义，不扩散真 Postgres CDC 依赖"
  - "修正 flink.cdc.version=3.6.0-2.2、flink.jdbc.connector.version=3.3.0-1.20 以恢复可解析坐标"

patterns-established:
  - "新 e12 模块：pom 克隆 e12-01 + Job uid/execute + README 八段要点 + 父 pom/examples/ai 三处登记"
  - "Aliyun 缺件时可用华为云镜像预热 ~/.m2，再回默认 settings 全量 compile"

requirements-completed: []
# QA-02 仅案例轴达成；文档≥30k 与 README/PHASES 终稿留给 07-02/07-03

duration: 16min
completed: 2026-07-19
---

# Phase 7 Plan 01: Wave 1 案例扩容 Summary

**真实零依赖 Flink Demo 将 examples mains 从 67 提升到 100，并恢复全模块 `mvn -DskipTests compile` 绿。**

## Performance

- **Duration:** 16 min
- **Started:** 2026-07-18T16:29:23Z
- **Completed:** 2026-07-18T16:45:31Z
- **Tasks:** 3/3
- **Files modified:** ~70（含 11 新模块 + 22 回填 Job + 登记/SSOT/e07 修复）

## Accomplishments

- Task 1：11 个 e12 缺口模块（05/09/11–14/16/19–21/23）进主构建，各 ≥1 真实 main + README；登记 `examples/pom.xml`、`examples/README.md`、`ai/README.md`
- Task 2：e08+3、e11+3、e01/e02/e04/e10 各+2、热门 e12 第二 Job×8 → **mains=100**；修正 CDC/JDBC 坐标并修复 e07 Flink 2.2 API 漂移
- Task 3：`qa_check` ④案例≥100、⑥ compile 绿；⑤文档行数仍红（10688&lt;30000，留给 07-02）

## Task Commits

1. **Task 1: 补全 e12 编号缺口零依赖模块** - `1744f12` (feat)
2. **Task 2: e08/e0x/e11 配额回填至 mains≥100** - `65abdfd` (feat)
3. **Task 3: 案例轴门禁回归** - （无代码变更，仅验证）

**Plan metadata:** （本 SUMMARY / STATE / ROADMAP 提交）

## Files Created/Modified

- 11× `examples/e12-*-*/` — 新零依赖 Demo（pom + Job + README）
- 22× 追加 `*Job.java` — e08/e11/e01/e02/e04/e10 + e12 第二 Job
- `examples/pom.xml` — 新模块 + CDC/JDBC 版本 SSOT
- `examples/README.md` / `ai/README.md` / 各模块 README — 登记
- `README.md` — CDC 矩阵注明 Maven `3.6.0-2.2`
- `examples/e07-connectors/.../C1|C4|C6*.java` — Flink 2.2 API 对齐

## Decisions Made

- 跳过 e12-10（章内论证可无独立 Demo）；差额用 e0x/第二 Job 补足
- Preview Agents / Milvus 不进主 pom；e12-03/04 仍仅 SQL、不包装假 main
- SSOT：CDC 连接器发布坐标为 `3.6.0-2.2`；JDBC 现网可用 `3.3.0-1.20`（原 `4.0.0-2.0` 未发布）

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] e12-16 类型链编译失败**
- **Found during:** Task 1
- **Issue:** `EnrichHop` 输出 `String` 后无法再 `process(EnrichHop)`
- **Fix:** 改为 `TracedEvent` POJO 透传 + 最终 logger
- **Files modified:** `TracePropagationJob.java`
- **Committed in:** `1744f12`

**2. [Rule 3 - Blocking] Aliyun 无法解析 CDC/JDBC 旧坐标；e07 API 漂移**
- **Found during:** Task 2 全量 compile
- **Issue:** `flink-connector-postgres-cdc:3.6.0` / `flink-connector-jdbc:4.0.0-2.0` 不在镜像；解析成功后 e07 C1/C4/C6 对 Flink 2.2 符号不匹配
- **Fix:** pom → `3.6.0-2.2` / `3.3.0-1.20`；C1 用 `KafkaSinkBuilder`；C4 包名 `bucketassigners` + `configuration.MemorySize`；C6 `WriterInitContext`
- **Workaround:** 首次拉件可用华为云镜像预热 `~/.m2`，再以默认 Aliyun settings 编译（本地已验证双路径 BUILD SUCCESS）
- **Committed in:** `65abdfd`

**Total deviations:** 2 auto-fixed (Rule 1 ×1, Rule 3 ×1)
**Impact on plan:** 必要阻塞修复；未扩大案例主题范围；无空壳 main

## Issues Encountered

- 默认 Aliyun 对未发布 JDBC `4.0.0-2.0` 与错误 CDC `3.6.0` 持续 404；已通过 SSOT 修正解决
- ENG-01 抽样仍用子串 `3.6.0`，与属性 `3.6.0-2.2` 兼容

## User Setup Required

None - no external service configuration required.

## qa_check Wave 1 结果

| 门禁 | 结果 | 实测 |
|------|------|------|
| ① compose | ok | — |
| ② 违禁词 | ok | — |
| ③ 断链 | ok | — |
| ④ 案例 ≥100 | **ok** | **mains=100** |
| ⑤ 文档 ≥30000 | **FAIL** | 10688（留给 07-02） |
| ⑥ mvn compile | **ok** | 全模块 BUILD SUCCESS |
| eng_audit | PASS | ENG-04 PHASES soft |

### mains 计数证明

```bash
grep -rl --include='*.java' 'public static void main' examples | wc -l
# → 100
```

## Next Phase Readiness

- 案例硬门禁已满足；07-02 专注文档实质扩写至 ≥30000
- QA-02 整体未勾选完成（缺文档轴与终稿）

## Self-Check: PASSED

- FOUND: 11 新 e12 模块目录与 Job
- FOUND: commits `1744f12`, `65abdfd`
- FOUND: mains=100；`mvn -T1C -DskipTests compile` EXIT 0

---
*Phase: 07-p6-qa*
*Completed: 2026-07-19*
