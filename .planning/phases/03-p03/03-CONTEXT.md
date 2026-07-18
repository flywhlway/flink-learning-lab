# Phase 3: p03 大盘与演练收官 - Context

**Gathered:** 2026-07-18
**Status:** Ready for planning

<domain>
## Phase Boundary

在 Phase 1–2 已交付的告警链路 + 三模式库 + Broadcast 门控之上，完成 p03 **监控大盘、压测/故障演练与文档包收官**，使 `projects/p03-vehicle-monitoring` 达到 PHASES P4 单项目完成态（可简历陈述、可一键复现、可压测演练）。

本 Phase 对应 REQUIREMENTS：**VEH-05**（Grafana 窗口聚合指标 + 异常检测相关面板）、**VEH-06**（压测脚本 + 含 watermark 停滞的故障演练 + baseline 数字）、**VEH-07**（架构文档、≥1 ADR、验证脚本、简历陈述页）。

不在本 Phase：p01/p02 完整工程、P5 全矩阵 benchmark / Operator Blue-Green / 仓库级 monitoring 三块看板终稿、商业动态 CEP。

</domain>

<decisions>
## Implementation Decisions

### Grafana 大盘数据源与面板（VEH-05）
- **D-01:** 大盘采用 **双数据源**：业务窗口指标落 **ClickHouse 指标表**（Grafana ClickHouse 或 Prometheus 旁路均可，以可导入 JSON + 本机可打开为准）；平台健康沿用已接通的 **Prometheus → Flink reporter**（`monitoring/` 值班五指标）。禁止「只有截图、无可导入 dashboard JSON」。
- **D-02:** 窗口聚合与 CEP 告警作业 **解耦**：在同一 `vehicle.events` 上新增轻量 **窗口聚合作业**（DataStream 或 Table/SQL，由 researcher 按现有 SSOT 选型），输出按 vin/窗口的计数类指标写入 CH；**不**把大盘聚合逻辑塞进 `VehicleAlertJob` 主图。
- **D-03:** Grafana dashboard JSON **provisioning 落仓**（`docker/config/grafana/provisioning/dashboards/` + p03 专用 JSON，或 `projects/p03-vehicle-monitoring/monitoring/` 再挂载——保持 default `make up` 不被 p03 业务面板硬绑定；推荐 p03 profile / 文档一键导入路径二选一，planner 须保证 OrbStack 上可打开）。面板最少覆盖：窗口吞吐/事件计数、按 `pattern_id` 的告警速率、异常阈值面板、Flink 健康（反压/checkpoint/重启/event-time lag 子集）。

### 异常检测相关面板（VEH-05）
- **D-04:** 「异常检测规则」落地为 **Grafana 阈值/告警面板**（例如单位时间 MATCH 激增、某 vin 急加速计数越界），规则条文写进 p03 文档（可读、可复现阈值）；**不**新增独立 ML 异常作业，**不**再扩 CEP 模式库条目来「假装异常检测」。
- **D-05:** 阈值数字必须来自本机演练可观察量或文档明示的演示默认值；禁止未实测却写成生产 SLA。

### 压测范围与 baseline（VEH-06）
- **D-06:** 本 Phase 压测为 **p03 项目级**（固定 eps/时长/并行度快照下的吞吐、lag、checkpoint 摘要），产出 `projects/p03-vehicle-monitoring/docs/baseline.md`；**不**实现 `benchmark/` 全矩阵（PROD-01 / Phase 6）。方法论可交叉引用 `benchmark/README.md`，数字只写 OrbStack arm64 实测。
- **D-07:** 压测驱动优先 **扩展现有** `scripts/gen_vehicle_events.py`（`--rate` / `--duration` 或等价）+ `make`/`scripts/` 包装拉取 Prometheus/CH 摘要；**不**为 p03 引入未登记的 k6/JMeter 镜像（P5 再评估）。
- **D-08:** baseline 报告模板对齐 `benchmark/README.md` 口径子集：环境快照 → 负载定义 → 指标表 → 结论；热身可缩短但须写明（不必强制 3 分钟，学习工程可 30–60s 热身，须在 baseline 中声明）。

### 故障演练剧本（VEH-06）
- **D-09:** 可执行演练 **恰好 2 条**（MVP）：(1) **watermark 停滞**（必做）；(2) **负载压测跑 baseline**（与 D-06 同一路径）。额外 chaos（杀 TM、断 Kafka）可写「可选附录」，不挡 Phase 完成。
- **D-10:** watermark 停滞剧本：通过造数/分区空闲制造水位停摆（对齐 docs/02-time-window 与作业已有 `withIdleness(30s)`），在 Flink UI Watermarks 列留下可观察证据，再恢复生产/尾心跳推进水位；脚本或 checklist 须断言「停滞可观察 → 恢复后 MATCH/窗口可继续」。禁止只写散文无命令。
- **D-11:** 演练入口放在 `projects/p03-vehicle-monitoring/scripts/`（如 `drill_watermark_stall.sh` + `loadtest.sh`）并由 README「验证/演练」节一键可跟；失败非 0。

### 文档包收官（VEH-07）
- **D-12:** 至少 **1 篇 ADR**：主题锁定 **「开源 CEP：编译期 Pattern + Broadcast 选择预编译激活集」vs 商业动态 CEP / 运行时编译**（承接 Phase 2 D-07 与 STACK 拒绝项）。路径：`projects/p03-vehicle-monitoring/docs/adr/0001-cep-broadcast-precompiled.md`（编号可由 planner 微调，须可按路径打开）。
- **D-13:** **简历陈述页** `projects/p03-vehicle-monitoring/docs/RESUME.md`：用可验证动词 + 指向 verify/baseline/大盘路径；禁止空泛形容词；数字只引用 baseline/verify 实测。
- **D-14:** **架构文档**：在 p03 README 架构节补全「告警 + 窗口大盘 + 演练」总图，或独立 `docs/ARCHITECTURE.md`（二选一，推荐独立短文 + README 链接）；回填 `docs/README.md` 模块 15-03 状态为完成态表述；交叉引用 docs/10-cep、docs/02-time-window、monitoring/。
- **D-15:** 验证脚本保持 **ClickHouse 为 CEP 告警权威出口**（Phase 1–2 纪律）；大盘/压测另有断言（dashboard 文件存在 + Grafana HTTP 或 provisioning 校验、baseline 文件非空且含实测表）——具体门禁由 planner 写入 verify/qa 扩展，须可脚本化。

### Claude's Discretion
- 窗口聚合作业类名、CH 指标表 DDL 列设计、Grafana JSON 面板布局细节、ClickHouse vs Prometheus 作为业务面板主查询引擎的最终选型（只要满足 D-01–D-03 可导入可打开）。
- 压测默认 eps/时长在 OrbStack 可稳定跑通前提下由 executor 实测填写；CONTEXT 不锁死具体数字。

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap / Requirements
- `.planning/ROADMAP.md` — Phase 3 目标与成功标准（VEH-05/06/07）
- `.planning/REQUIREMENTS.md` — VEH-05、VEH-06、VEH-07 条文
- `.planning/PROJECT.md` — 里程碑不变量（OrbStack 实测、禁伪跑通）
- `.planning/STATE.md` — Phase 1–2 已锁定决策（CH 权威、独立 pom、WM/idleness）
- `.planning/phases/02-p03-broadcast/02-CONTEXT.md` — Broadcast/模式库决策；明确将 Grafana/压测/ADR 延至本 Phase

### Architecture / Stack
- `.planning/research/ARCHITECTURE.md` — p03 监控大盘会话 2 示意（窗口聚合 → CH/Metrics → Grafana）
- `.planning/research/STACK.md` — Grafana/Prometheus/ClickHouse 版本；拒绝商业动态 CEP
- `PHASES.md` — P4 单项目完成态验收口径

### 已有可观测与压测方法论
- `monitoring/README.md` — Flink Prometheus 值班五指标；P5 仓库级看板规划（本 Phase 不吞并）
- `benchmark/README.md` — 压测方法论与口径（全矩阵属 P5；本 Phase 只取子集）
- `docker/config/grafana/provisioning/datasources/datasources.yml` — Grafana 已预配 Prometheus
- `docker/docker-compose.yml` — grafana/prometheus 服务与 provisioning 挂载

### p03 交付现状（演进点）
- `projects/p03-vehicle-monitoring/README.md` — 八段式骨架；标注 ADR/Grafana/压测待本 Phase
- `projects/p03-vehicle-monitoring/docs/PATTERN-LIBRARY.md` — 三模式五元组（勿回退）
- `projects/p03-vehicle-monitoring/scripts/gen_vehicle_events.py` — 造数 + 尾心跳 WM；压测扩展基点
- `projects/p03-vehicle-monitoring/scripts/verify.sh` — CH `pattern_id` 权威断言样板
- `docs/README.md` — 模块 15-03 编号登记（须回填完成态）
- `docs/10-cep/README.md` — CEP/Broadcast 叙事
- `docs/02-time-window/README.md` — watermark 停滞/并行度/idleness 教材（演练必引）
- `examples/e02-time-window/README.md` — watermark 实验对照
- `README.md` — ADR-001 版本锁定与端口速查（Grafana :3000）

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `VehicleAlertJob` + 三 CEP + `PatternActivationGate`：告警链路保持；大盘用旁路窗口作业，避免改乱门控语义
- `gen_vehicle_events.py` / `verify.sh` / `make verify-switch`：演练与压测包装的基线
- Grafana provisioning 目录已存在（仅 datasource）；可追加 dashboards provider
- `monitoring/README.md` 五指标：大盘平台面板直接复用查询思路
- `benchmark/README.md`：baseline 报告结构模板

### Established Patterns
- ClickHouse = CEP 验收权威出口；Kafka 仅诊断
- p03 compose profile 隔离；default `make up` 不加 `--profile p03`
- 独立 pom；算子 `.uid(...)`；OrbStack arm64 实测才合入
- 文档八段式 + docs/README 编号登记

### Integration Points
- 新增：窗口聚合作业 + CH 指标 DDL（`p03-init` 幂等）
- 新增：Grafana dashboard JSON + provisioning（或文档化导入）
- 新增：`scripts/loadtest.sh`、`scripts/drill_watermark_stall.sh`、`docs/baseline.md`、`docs/adr/*`、`docs/RESUME.md`
- 回填：p03 README、docs/README 15-03、PHASES/CHANGELOG（执行期）

</code_context>

<specifics>
## Specific Ideas

- 学习工程叙事：大盘证明「CEP 告警可运营观察」，演练证明「watermark 不是课本概念」；简历页只写能 `make` 复现的句子。
- `--auto` 会话：优先可演示、可断言、可导入看板；不追求 P5 级压测矩阵或 Loki/OTel。
- ARCHITECTURE 示意「异常阈值面板」= Grafana 阈值，不是第二套 CEP。

</specifics>

<deferred>
## Deferred Ideas

- `benchmark/` 全矩阵 + 仓库级 `baseline.md` → Phase 6（PROD-01）
- monitoring 三块看板终稿（平台总览 / 作业深潜 / AI 专项）→ Phase 6（PROD-04）
- Loki / OTel Tracing → P5/P6 规划，本 Phase 不做
- k6 / 商业压测工具镜像 → 未进 SSOT 前不做
- 杀 TM / 断网等额外 chaos → 可选附录，不挡 VEH-06
- p01/p02 复制本 Phase 纪律 → Phase 4 / 5

None — discussion stayed within phase scope

</deferred>

---

*Phase: 3-p03 大盘与演练收官*
*Context gathered: 2026-07-18*
*Mode: --auto (recommended defaults selected)*
