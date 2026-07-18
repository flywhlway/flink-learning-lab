# ADR-0001：AI 路径可降级——主构建零硬依赖 Preview / 外部模型

- **Status:** Accepted
- **Date:** 2026-07-18
- **Deciders:** flink-learning-lab P4 / p01（LOG-02 / LOG-03 / LOG-05 / D-15）
- **Tags:** degradable-ai, async-io, ollama, flink-agents-preview, flink-2.2

## Context

日志 AI 平台需要「至少一条可观察的 AI 路径」，同时必须满足学习工程硬纪律：

1. **无外部模型时仍可演示完整故事**（LOG-02）：解析 → 富化 → 规则 → ClickHouse。
2. **主构建不可被 Preview 坐标或未起的模型拖垮**（D-02 / 项目不变量）。
3. 里程碑条文枚举含 `ML_PREDICT` / Flink Agents / Milvus；本项目必须在可复现边界内收窄选型。

可选路径对比：

| 路径 | 能力 | 代价 / 风险 |
|------|------|-------------|
| A. 主 pom 硬依赖 Flink Agents 0.3.0 Preview | Agents 叙事完整 | Preview 坐标漂移会阻断 `mvn clean package`；与 e12-07「standalone 隔离」纪律冲突 |
| B. 默认作业硬依赖宿主机 Ollama / Milvus | AI 路径总在线 | 无模型则作业失败；违反「可降级」与 default `verify` 门禁 |
| C. SQL `ML_PREDICT` 作为验收主路径 | 与 e12-03 教材对齐 | 本项目工程形态是 DataStream（对齐 p03）；双主路径冲淡叙事 |
| D. **默认 AI off + 可选 Async Ollama 旁路** | 规则路径零模型；有模型时多一条可观察旁路 | AI 能力有意收窄为风险分级，不做多 Agent / 完整 RAG 平台 |

教材已给出降级红线：[ai/chapters/03-streaming-inference.md](../../../../ai/chapters/03-streaming-inference.md)、[examples/e11-async-io](../../../../examples/e11-async-io/)、[examples/e12-07-agent-quickstart](../../../../examples/e12-07-agent-quickstart/)（Agents 隔离样板）。

## Decision

采用路径 **D**：

1. **默认 `--ai.enabled=false`**：作业只跑 Parse → Enrich → Rule → Guardrail → ClickHouse；`ai_source=DISABLED`；**零** Ollama / Milvus 调用。`JobConfig` 默认值与 `make submit` 显式参数双重保险。
2. **可选 AI 旁路**：仅当 `--ai.enabled=true` 时挂接 **BudgetGate → Async I/O → 宿主机 Ollama `/api/chat` 风险分级** → Guardrail → CH；验收走独立 `make verify-ai`（非默认门禁）。
3. **主 pom 禁止**：Flink Agents Preview 坐标、向量库 SDK、未登记 AI Gateway。Agents / Milvus 若展示，必须独立模块或既有 `--profile ai`，且不得阻塞主构建与 `make verify`。
4. **明确不做**：把 Agents Preview 或外部模型写成默认硬依赖；把 SQL `ML_PREDICT` 写成第二主验收路径冲淡 Async 选型。

降级勾选与命令见 [DEGRADE-CHECKLIST.md](../DEGRADE-CHECKLIST.md)。架构总图见 [ARCHITECTURE.md](../ARCHITECTURE.md)。

## Consequences

### 正向

- OrbStack 上可一键复现：`make drill-degrade`（AI off + 不可达 endpoint）后 `verify` 仍绿；权威出口仍是 ClickHouse `log_results`。
- 主构建 `mvn clean package` 不依赖 Preview 坐标或本机模型是否已 pull。
- 简历陈述可指向脚本路径（见 [RESUME.md](../RESUME.md)），而非空泛「接入了大模型」。

### 负向 / 约束

- AI 能力刻意收窄为 **风险分级旁路**；不做多 Agent 分诊、不做 Streaming RAG 主验收。
- `verify-ai` 依赖宿主机 `ollama serve` + 已 pull 模型；失败不得拖垮默认 `verify`。
- BudgetGate 必须接在 Async **之前**（短路外呼）；Guardrail 必须接在 Sink **之前**（输出侧 BLOCK）。顺序写死在作业图，禁止事后仅打标。

### 可验证锚点

| 符号 / 产物 | 路径 |
|-------------|------|
| `JobConfig.aiEnabled` 默认 false | `src/main/java/.../JobConfig.java` |
| `LogAiJob` 条件挂接 Async | `src/main/java/.../LogAiJob.java` |
| `OllamaRiskAsyncFunction` | `src/main/java/.../ai/OllamaRiskAsyncFunction.java` |
| 降级勾选表 | [docs/DEGRADE-CHECKLIST.md](../DEGRADE-CHECKLIST.md) |
| AI off 硬演练 | `make drill-degrade` → `scripts/drill_ai_degrade.sh` |
| 默认验收 | `make verify` → `scripts/verify.sh`（CH 权威） |
| 可选 AI 验收 | `make verify-ai` → `scripts/verify_ai.sh` |
