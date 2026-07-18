# Phase 6: P5 生产化 - Context

**Gathered:** 2026-07-18
**Status:** Ready for planning

<domain>
## Phase Boundary

落地仓库级 **P5 生产化**能力：`benchmark/` 全矩阵可运行并产出 `baseline.md`、OrbStack K8s 上 **Flink Kubernetes Operator 1.15** 可观察的 Blue/Green 演练、**单一** CI/CD + GitOps 路径可按文档复现，以及 `best-practice/` 完整规范、`interview/` ≥150 题、`monitoring/` 看板 JSON 可导入——且均在 OrbStack arm64 可观察（禁止纸面合入）。

本 Phase 对应 REQUIREMENTS：**PROD-01**（benchmark 全矩阵 + baseline.md）、**PROD-02**（Operator 1.15 + Blue/Green 可观察）、**PROD-03**（CI/CD + 单一 GitOps 可复现）、**PROD-04**（best-practice 完整、interview ≥150、monitoring 看板 JSON 可导入）。

不在本 Phase：P6 总装 QA（`qa_check` 终检、案例/行数计量、README 终稿）、双 GitOps 工具并行、云厂商托管 Flink 作为主演示路径、完整 Loki/OTel 栈硬验收、商业多租户控制面、升主线 Flink 2.3。

</domain>

<decisions>
## Implementation Decisions

### Benchmark 矩阵与驱动（PROD-01）
- **D-01:** 矩阵采用 **学习工程可跑通的裁剪全矩阵**（保留 README 轴语义，禁止未实测数字）：作业轴 = **e01-J2（窗口聚合）+ e10 CEP + p03 VehicleAlertJob**；负载轴 = **1k / 5k eps** 为必跑，**20k eps** 为 stretch（OrbStack 不稳定则在 baseline 标注 SKIPPED + 原因，不得虚构吞吐）；State Backend = **HashMap + RocksDB 增量**（ForSt 仅可选附录一行，不挡 Phase）；Checkpoint 主路径 = **对齐 + 30s**，另用 **1–2 行对照**覆盖非对齐或 10s（禁止全笛卡尔积爆炸）。
- **D-02:** 压测驱动锁定 **既有 Python 造数 / 项目 gen 脚本 + `benchmark/` 包装脚本 + Makefile 目标**；**不**引入未登记的 k6/JMeter 镜像。方法论继续服从 `benchmark/README.md` 单变量原则与报告模板；热身可缩短为学习工程级 **30–60s**，但必须在 `benchmark/baseline.md` 声明相对「理想 3 分钟」的偏差。
- **D-03:** 权威产物路径锁定 **`benchmark/baseline.md`**（仓库级）；数字仅 OrbStack arm64 实测。项目级 `projects/*/docs/baseline.md` 可交叉引用，**不**替代仓库级矩阵报告。

### Operator Blue/Green（PROD-02）
- **D-04:** Operator 版本锁定 **Flink Kubernetes Operator 1.15.0**（根 README 版本矩阵已登记）；部署场锁定 **OrbStack 内置 Kubernetes**；安装路径优先 **Helm**（与 STACK / `production/README.md` 一致）。须先登记任何新增镜像/chart 坐标到版本矩阵。
- **D-05:** Blue/Green 演练主对象锁定 **p03 `VehicleAlertJob` 的 FlinkDeployment**（P4 最成熟、JAR/verify 纪律齐全）。Session 模式与 Autoscaler 可写「可选附录实验」，**不**作为 PROD-02 硬门禁——硬门禁是可观察的 Blue/Green 发布时间线。
- **D-06:** 可观察证据必须 **脚本化时间线**（推荐落 `production/docs/bluegreen-timeline.md` 或等价路径）：至少包含 `kubectl`/Operator 事件、`FlinkDeployment` 状态迁移、作业/savepoint 相关日志片段；禁止「仅截图散文、无命令可复现」。演练入口放在 `production/scripts/`（或 Makefile 目标）并由文档一键可跟，失败非 0。

### CI/CD 与单一 GitOps（PROD-03）
- **D-07:** GitOps 工具锁定 **Argo CD**（`production/README.md` 已写 ArgoCD；STACK 拒绝双栈）。**禁止**并行深讲 Flux。路径形态：**Git 仓库 manifests（Helm/Kustomize 二选一，推荐 Helm chart + values）→ Argo CD sync → OrbStack K8s**。
- **D-08:** CI/CD MVP：**GitHub Actions** 跑编译/`scripts/qa_check.sh`（或子集门禁）+ 文档化的镜像/chart 发布步骤；本机演示以 arm64/OrbStack 为准。多架构 `buildx` 可写完整 SOP，但 **不以多架构镜像推送为 PROD-03 硬门禁**（避免学习工程被 registry 账号挡住）。
- **D-09:** `production/` 交付可复现清单：Operator 安装、示例 FlinkDeployment、Blue/Green SOP、Argo 应用清单、回滚步骤——全部可按文档在本机跟完，无「请参考官网」占位。

### 规范 / 题库 / 看板（PROD-04）
- **D-10:** `monitoring/` 交付 **恰好 3 块可导入 Grafana dashboard JSON**（对齐既有规划）：**平台总览 / 作业深潜 / AI 专项（含 token/成本类面板或等价业务指标）**。须可在现有 Grafana provisioning 或文档一键导入路径下打开；**Loki / OTel Tracing 不作为 PROD-04 硬验收**（可在 docs/14 提及为可选增强，禁止为此新拉未登记可观测栈挡门禁）。
- **D-11:** `interview/` 扩至 **≥150 题**，保持 Level 分层（L1–L8+）；P5 起每题附 **完整参考答案或等价完整考点推导**（可链回教材章节，禁止只留空题干）。首批 30 题可保留并升级答案完整度。
- **D-12:** `best-practice/` 从首批 12 条扩展为 **完整规范体系**（至少覆盖：架构/目录命名、uid/savepoint、checkpoint/Kafka 事务、状态与 TTL、反压基线、日志/异常、CI-CD/GitOps 检查清单、AI 降级），与 `production/` **互链**——规范正文在 `best-practice/`，落地清单在 `production/`。
- **D-13:** `docs/README.md` 模块 **13（性能与压测）** 与 **14（生产化）** 必须登记并回填完成态表述；正文可落 `docs/` 对应目录或明确指向 `benchmark/`/`production/`/`monitoring/` 权威路径，禁止编号空洞。

### 交付切片建议（供 planner 切 wave）
- **D-14:** 推荐执行顺序（可微调，但依赖方向锁定）：**(1)** SSOT/编号登记 + benchmark 矩阵脚本与 `baseline.md` → **(2)** Operator 安装 + p03 Blue/Green 演练证据 → **(3)** Argo CD + CI 路径文档化 → **(4)** interview ≥150 + best-practice 完整 + 三块 Grafana JSON。Benchmark 与文档扩容可与 Operator 并行，但 **Blue/Green 不得先于可用作业 JAR**。

### Claude's Discretion
- Helm chart 目录布局、Argo Application CR 命名、GitHub Actions workflow 文件名、dashboard 面板具体 PromQL/CH 查询、interview 题目在各级的精确配比、best-practice 章节拆分粒度、20k eps / ForSt / Autoscaler 附录是否实跑。
- 压测默认并行度/TM 数在 OrbStack 可稳定跑通前提下由 executor 实测填写；CONTEXT 不锁死具体吞吐数字。

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap / Requirements
- `.planning/ROADMAP.md` — Phase 6 目标与成功标准（PROD-01–04）
- `.planning/REQUIREMENTS.md` — PROD-01 … PROD-04 条文；Out of Scope「双 GitOps」
- `.planning/PROJECT.md` — 里程碑不变量（OrbStack 实测、禁伪跑通、SSOT）
- `.planning/STATE.md` — P4 三项目完成态与工程纪律累积决策
- `PHASES.md` — P5 生产化验收口径（压测可复现 + OrbStack K8s Blue/Green）

### Research / Stack
- `.planning/research/STACK.md` — Operator 1.15、Helm、Argo（或 Flux 二选一）、拒绝双 GitOps / 云托管主路径
- `.planning/research/ARCHITECTURE.md` — P5 层（production/ + benchmark/ + Operator）与构建顺序
- `.planning/research/FEATURES.md` — P5 功能表与不同点（真实 Blue/Green）
- `.planning/research/PITFALLS.md` — 「OrbStack K8s Blue/Green 纸上谈兵」陷阱与防治
- `.planning/research/SUMMARY.md` — P4 作业可跑 → P5 Operator 顺序

### 仓库级交付目录（演进点）
- `benchmark/README.md` — 方法论、计划矩阵轴、报告模板（本 Phase 落地 baseline）
- `production/README.md` — Operator/升级模式/Blue-Green/GitOps 蓝图（本 Phase 落地）
- `monitoring/README.md` — 值班五指标 + P5 三块看板规划
- `best-practice/README.md` — 首批 12 条军规（本 Phase 扩展为完整规范）
- `interview/README.md` — 首批 30 题 Level 分层（本 Phase 扩至 150+）
- `README.md` — 版本矩阵 SSOT（Operator 1.15.0）、目录状态列
- `docs/README.md` — 模块 13/14 编号登记落点

### 可复用作业与压测样板
- `projects/p03-vehicle-monitoring/` — Blue/Green 主对象；`docs/baseline.md` / `scripts/loadtest.sh` / JAR 构建路径
- `projects/p01-log-ai-platform/docs/baseline.md` — 项目级 baseline 口径对照
- `projects/p02-realtime-reco/docs/baseline.md` — 项目级 baseline 口径对照
- `examples/e01-datastream/` — e01-J2 窗口作业（矩阵作业轴）
- `examples/e10-cep/` — CEP 作业（矩阵作业轴）
- `docker/docker-compose.yml` — Prometheus/Grafana 基座与 provisioning
- `docker/config/grafana/provisioning/` — datasource/dashboard 挂载点
- `scripts/qa_check.sh` — Phase 结束门禁（CI 应复用）

### 先前 Phase 纪律样板
- `.planning/phases/05-p02/05-CONTEXT.md` — 最近完成的项目级压测/演练纪律
- `.planning/phases/03-p03/03-CONTEXT.md` — Grafana JSON 可导入 + baseline 模板决策

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `benchmark/` / `production/` / `monitoring/` / `best-practice/` / `interview/`：骨架 README 已在；本 Phase 填工程与内容，不必改名
- p03/p01/p02：各有 `scripts/loadtest.sh` + `docs/baseline.md`，可抽口径进仓库级矩阵 harness
- Grafana：datasource 已 provisioning；p03 已有项目级 dashboard JSON 可作仓库级三块看板的版式参考
- docker 基座 Prometheus `:9090` / Grafana `:3000` / Flink reporter `:9249` 已接通

### Established Patterns
- OrbStack arm64 实测才合入；禁止沙箱「假装跑通」
- 版本 SSOT：根 README 矩阵 + pom 属性；新增组件先登记
- 单一工具深讲（GitOps 只选 Argo）；文档禁 TODO/省略/请参考官网
- 项目 compose profile 隔离纪律保持——P5 K8s 路径是并行演示面，不破坏 `make up`

### Integration Points
- 新建/扩展：`benchmark/` 脚本与 `baseline.md`；`production/` Helm/manifests/scripts/docs；`monitoring/*.json`；`interview/` 扩题；`best-practice/` 规范正文；`docs/` 模块 13/14；可选 `.github/workflows/`
- 回填：根 README 目录状态列、PHASES.md P5 状态、CHANGELOG（执行期）

</code_context>

<specifics>
## Specific Ideas

- `--auto` 讨论：优先对齐已有蓝图文案（`production/README` 写 ArgoCD；`monitoring/README` 写三块看板；`benchmark/README` 写矩阵轴），在可复现前提下裁剪笛卡尔积与可选栈（Loki/OTel/k6/Flux）。
- Blue/Green 证据强调「事件/状态时间线」而非截图——承接 PITFALLS「纸上谈兵」防治。

</specifics>

<deferred>
## Deferred Ideas

- 完整 Loki 日志 + OTel Tracing 接入 —— 可作为 docs/14 可选增强或后续里程碑，不挡 PROD-04
- Flux 作为第二 GitOps 路径 —— 明确 Out of Scope
- 云多区域灾备 / 多租户计费中台 —— FUT / Out of Scope
- ForSt 全负载轴、20k eps 必跑、Autoscaler 生产级调参 —— 可选附录，非硬门禁
- P6 `qa_check` 终检与案例/行数计量 —— Phase 7

None — discussion stayed within phase scope（无额外 todo 折叠）

</deferred>

---

*Phase: 6-P5 生产化*
*Context gathered: 2026-07-18*
