# Phase 2: p03 模式库与 Broadcast - Context

**Gathered:** 2026-07-18
**Status:** Ready for planning

<domain>
## Phase Boundary

将 Phase 1 的单模式 `HarshThenFault` 告警链路升级为可评审的 CEP **模式库（≥3 条，每条五元组齐全）**，并通过 **Broadcast State 动态选择预编译模式集**（非运行时动态编译 Pattern）。交付后维护者可切换激活模式集并观察到匹配行为变化；无 `within` 的模式无法通过项目自检。

本 Phase 对应 REQUIREMENTS：VEH-03、VEH-04。Grafana 大盘、压测/故障演练、完整 ADR/简历页属 Phase 3（VEH-05/06/07），不在本 Phase 范围。

</domain>

<decisions>
## Implementation Decisions

### 模式库条目选型
- **D-01:** 模式库固定交付 **恰好 3 条**预编译模式（满足 ≥3，避免本 Phase 膨胀），全部复用现有信号白名单 `HARSH_ACCEL | DTC | HEARTBEAT`，不新增 signalType（Parse 层零扩展）。
- **D-02:** 三条模式职责分工（教学覆盖面优先）：
  1. `HARSH_THEN_FAULT` — 保留 Phase 1：`followedBy` + `within(30s)`（基线）
  2. `TRIPLE_HARSH` — `times(3).consecutive()` 急加速突发 + `within(20s)`（量词/紧邻）
  3. `DTC_PAIR` — `DTC followedBy DTC` + `within(15s)` + 显式 `AfterMatchSkipStrategy.skipPastLastEvent()`（重复故障 + skip 语义）
- **D-03:** 每条模式必须在独立文档登记五元组：**业务含义 / within / 连接语义 / skip 策略 / 状态上界**。落点：`projects/p03-vehicle-monitoring/docs/PATTERN-LIBRARY.md`（八段式 README 交叉引用该页，不把五元组只写在散文里）。

### Broadcast 控制面
- **D-04:** 控制面走 **Kafka topic** `vehicle.pattern.control`（由现有 `p03-init` 幂等创建），消息为确定性 JSON：`{"activePatterns":["HARSH_THEN_FAULT"],"version":N}`。禁止依赖本地文件热读或作业参数重启换模式作为验收主路径。
- **D-05:** Broadcast 写入纪律对齐 e03-C7：仅在 `processBroadcastElement` 写 Broadcast State；内容完全来自广播消息本身（禁止随机数/本地时钟）；`processElement` 侧只读。
- **D-06:** 默认激活集为 `["HARSH_THEN_FAULT"]`，保证 Phase 1 造数/verify 路径在未发控制消息时仍可绿。

### CEP 与 Broadcast 接线
- **D-07:** **静态作业图 + 出口门控**（推荐默认）：图中并行挂载 3 条 `CEP.pattern(...)`（各自强制 `within`），Handler 输出带 `patternId`；union 后经 **Broadcast 门控算子**按 `activePatterns` 过滤再双写 Kafka/ClickHouse。禁止引入商业动态 CEP / 运行时编译 Pattern。
- **D-08:** `AlertEvent` 增加 `patternId` 字段；ClickHouse `vehicle_alerts` 增加对应列（幂等 DDL / 兼容迁移由 planner 落地），verify 可按 `pattern_id` 断言切换效果。
- **D-09:** TIMEOUT Side Output 语义保留：超时告警同样带 `patternId`，并受同一激活集门控（未激活模式的 TIMEOUT 不落库）。

### 验收与 within 门禁
- **D-10:** 新增（或扩展）可执行验收：发布控制消息切换激活集 → 造数命中模式 A/B → **ClickHouse 为唯一权威出口**断言 `pattern_id` 与匹配行为变化（延续 Phase 1：Kafka 仅诊断）。
- **D-11:** 项目自检：**无 `within` 不得合入**——以模式注册表 + 单测（或等价静态检查）强制每条工厂方法含 `within`；文档评审清单勾选五元组缺项即失败。
- **D-12:** 造数脚本为每条模式提供可判定 `--scenario`（至少 `match-harsh-fault` / `match-triple-harsh` / `match-dtc-pair` + 控制消息辅助命令），尾心跳推进 watermark 纪律延续 Phase 1。

### Claude's Discretion
- 门控算子具体类名、Broadcast State descriptor key、控制消息 schema 字段命名细节可由 researcher/planner 按 e03-C7 / e12-17 类比选定，只要满足 D-04–D-09。
- `TRIPLE_HARSH` / `DTC_PAIR` 的阈值与 within 秒数可在实现时微调，但必须写进五元组且可被造数稳定触发。

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap / Requirements
- `.planning/ROADMAP.md` — Phase 2 目标与成功标准（VEH-03/VEH-04）
- `.planning/REQUIREMENTS.md` — VEH-03、VEH-04 条文
- `.planning/STATE.md` — Phase 1 已锁定决策（verify CH 权威、独立 pom、WM/idleness 等）
- `.planning/PROJECT.md` — 里程碑不变量与验收纪律

### Phase 1 交付与调研
- `.planning/phases/01-p03/01-RESEARCH.md` — Broadcast/模式库明确划出 Phase 2；CEP within 红线
- `.planning/phases/01-p03/01-02-SUMMARY.md` — VehicleAlertJob / HarshThenFault / Handler 现状
- `.planning/phases/01-p03/01-03-SUMMARY.md` — 造数 + verify e2e 纪律
- `.planning/phases/01-p03/01-PATTERNS.md` — p03 文件级类比映射

### 代码与教材（机制样板）
- `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java` — 现有单模式管线
- `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/HarshThenFaultPattern.java` — 模式库第一条
- `projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/AlertPatternHandler.java` — MATCH/TIMEOUT
- `projects/p03-vehicle-monitoring/scripts/verify.sh` — CH 权威断言样板
- `projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql` — 告警表 DDL
- `examples/e03-state/src/main/java/com/flywhl/flinklab/e03/C7BroadcastRuleJob.java` — Broadcast 动态规则骨架
- `examples/e10-cep/README.md` — 五元组模板 + 「Broadcast 选预编译模式集」路线
- `examples/e10-cep/src/main/java/com/flywhl/flinklab/e10/C5VehicleDtcPatternJob.java` — 车联网雏形
- `docs/10-cep/README.md` — within / skip / 无 within 禁令 / Broadcast 选型说明
- `docs/03-state/README.md` §03-02 — Broadcast State 纪律与规模红线
- `.planning/research/STACK.md` — flink-cep / 拒绝商业动态 CEP
- `.planning/research/ARCHITECTURE.md` — p03 建议构建顺序第 2 步

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `HarshThenFaultPattern` / `AlertPatternHandler` / `VehicleAlertJob`：直接演进为模式注册表 + 多 CEP 分支 + 门控
- `ParseVehicleJson` 白名单：本 Phase 不扩展 signalType，三条新模式均落在现有类型上
- `scripts/gen_vehicle_events.py` + `verify.sh`：扩展 scenario / pattern_id 断言
- `e03-C7` / `e12-17` / `e12-22`：Broadcast 控制面与确定性写入样板
- `p03-init` compose profile：追加 `vehicle.pattern.control` topic

### Established Patterns
- CEP：`followedBy` + `within` + `TimedOutPartialMatchHandler` Side Output
- 验收：ClickHouse count 为唯一 exit 0；Kafka 诊断
- 算子均 `.uid(...)`；独立 pom 不挂 examples 父工程
- 无 within 禁止合入（docs/10-cep 红线）

### Integration Points
- 改造点集中在 `projects/p03-vehicle-monitoring`（作业图、模型、DDL、造数、verify、PATTERN-LIBRARY 文档）
- `docker/` 仅追加 control topic 初始化（保持 profile 隔离，不污染 default `make up`）
- Flink UI / CH / Kafka 端口与凭据沿用 Phase 1

</code_context>

<specifics>
## Specific Ideas

- 学习工程叙事对齐 e10 README：「开源 CEP Pattern 编译期固定；运行期 = Broadcast 选择预编译模式集」。
- 五元组「状态上界」必须写清论证（例如：单 vin 进行中部分匹配 ≤ N，由 within + skip 保证），禁止空话。
- `--auto` 会话锁定：优先可演示、可断言，不追求模式库条目数量膨胀。

</specifics>

<deferred>
## Deferred Ideas

- Grafana 窗口聚合大盘与异常检测面板 → Phase 3（VEH-05）
- 压测脚本 / watermark 停滞故障演练 / baseline 数字 → Phase 3（VEH-06）
- 完整 ADR + 简历陈述页终稿 → Phase 3（VEH-07）
- 商业动态 CEP / 运行时编译 Pattern → 明确拒绝（STACK / FUT）
- 新增 signalType（如 SPEED）以支撑更多模式 → 若需要，另开 backlog；本 Phase 刻意不扩展

None — discussion stayed within phase scope（todos 无匹配项）

</deferred>

---

*Phase: 2-p03 模式库与 Broadcast*
*Context gathered: 2026-07-18*
*Mode: --auto (recommended defaults selected)*
