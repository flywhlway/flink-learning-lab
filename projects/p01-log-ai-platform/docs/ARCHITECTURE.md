# p01 · 架构短文（规则路径 + 可降级 Async AI）

> 对应教材：[ai/chapters/03-streaming-inference.md](../../../ai/chapters/03-streaming-inference.md) · [ai/chapters/17-streaming-guardrail.md](../../../ai/chapters/17-streaming-guardrail.md) · [ai/chapters/18-streaming-cost-control.md](../../../ai/chapters/18-streaming-cost-control.md) · Async 样板 [examples/e11-async-io](../../../examples/e11-async-io/)
> 决策记录：[ADR-0001](adr/0001-ai-path-degradable.md) · 降级勾选：[DEGRADE-CHECKLIST.md](DEGRADE-CHECKLIST.md)

## 1. 边界

本页描述 **结构化日志 AI 平台样板** 在 OrbStack arm64 上可复现的运行拓扑：默认规则路径、可选 Async Ollama 风险分级旁路、BudgetGate / Guardrail 顺序、以及两条演练入口。不覆盖 p02/p03 主图、P5 Operator Blue/Green、仓库级 `benchmark/` 全矩阵、Flink Agents 多专家网关、Streaming RAG 主验收。

## 2. 总图

算子顺序锁定：**Parse → Enrich → Rule → BudgetGate（短路）→ 可选 Async AI → Guardrail → ClickHouse**。BudgetGate 不得画在 AI 之后；Guardrail 不得挪到 Sink 之后。

```text
                    ┌─────────────────────────────┐
                    │ gen_log_events.py           │
                    │  (--scenario | --rate/--dur)│
                    └──────────────┬──────────────┘
                                   │ produce
                                   ▼
                          Kafka logs.events
                          （宿主机 localhost:9094 /
                           作业内 kafka:9092）
                                   │
                                   ▼
                          LogAiJob（单作业）
                          groupId=p01-log-ai*
                          WM: ooo=5s
                                   │
                    ParseLogJson → FeatureEnricher → RuleTagger
                                   │
                    ┌──────────────┴──────────────┐
                    │ ai.enabled=false            │ ai.enabled=true
                    │ 透传 + ai_source=DISABLED   │
                    │                             ▼
                    │                    BudgetGate（调用上限）
                    │                    allow ──► Async Ollama
                    │                    trip ───► Side Output DEGRADED
                    │                             │
                    └──────────────┬──────────────┘
                                   ▼
                          Guardrail（关键词 BLOCK）
                                   │
                                   ▼
                          CH flinklab.log_results
                   （rule_label / ai_source / ai_risk / feature_json）
                                   │
              ┌────────────────────┼────────────────────┐
              ▼                    ▼                    ▼
     verify.sh（默认）      verify_ai.sh（可选）   Prom :9090 / TM :9249
     CH 权威 AUTH_FAIL      ai_source=AI           ai_calls / budget_trips /
     ai_source=DISABLED     （需宿主机 Ollama）     guardrail_blocks …
              │
              ├── loadtest.sh → docs/baseline.md
              └── drill_ai_degrade.sh（AI off + 不可达 endpoint → verify 仍绿）
```

## 3. 单作业与验收双轨

| 轨 | 入口 | 前置 | 权威断言 |
|----|------|------|----------|
| 默认规则 | `make submit` → `make verify` / `make match` | 无 Ollama | CH `rule_label`（默认 `AUTH_FAIL`）；Kafka 仅诊断 |
| 可选 AI | `make submit-ai` → `make verify-ai` | 宿主机 `ollama serve` + 模型 | CH `ai_source=AI` 且 `ai_risk∈{HIGH,MEDIUM,LOW}` |
| AI 降级演练 | `make drill-degrade` | 故意 `--ai.enabled=false` + 不可达 endpoint | 仍走默认 `verify`，exit 0 |
| 压测 baseline | `make loadtest` | p01 作业 RUNNING | [baseline.md](baseline.md)（OrbStack 实测） |

Compose 隔离：`profiles: ["p01"]` 的 `p01-init` 创建 `logs.events` 与 `flinklab.log_results` DDL；default `make up` 不要求 `--profile p01`。Milvus 继续走既有 `--profile ai`，**不**绑进 p01 默认验收。

AI 路径选型锁定为 **DataStream Async I/O → 宿主机 Ollama**，见 [ADR-0001](adr/0001-ai-path-degradable.md)；Agents Preview / 默认硬依赖外部模型被明确拒绝。

## 4. 成本与护栏（LOG-04）

| 能力 | 算子位置 | 指标名片段 | 行为 |
|------|----------|------------|------|
| 预算熔断 | **Async 之前** `BudgetGateFunction` | `budget_trips` | 超 `--budget.max-ai-calls` Side Output `DEGRADED`，短路外呼 |
| AI 调用 | `OllamaRiskAsyncFunction` | `ai_calls` / `ai_timeouts` / `ai_degrades` | timeout → `DEGRADED`，作业不重启 |
| 输出护栏 | **Sink 之前** `GuardrailFunction` | `guardrail_blocks` | 关键词命中 → `ai_source=BLOCKED`，脱敏落库 |

指标经 Flink Prometheus reporter（TM `:9249`）→ Prometheus `:9090`；观察命令见项目 README §5.4。不引入未登记的 AI Gateway / Loki / OTel 栈。

## 5. 演练与文档包

| 入口 | 脚本 | 断言重心 |
|------|------|----------|
| 规则回归 | `scripts/verify.sh` / `make verify` | ClickHouse `log_results.rule_label`；**Kafka 仅诊断** |
| AI 可选轨 | `scripts/verify_ai.sh` / `make verify-ai` | Ollama 前置 + CH `ai_source=AI` |
| 压测 baseline | `make loadtest` → [baseline.md](baseline.md) | Prometheus + CH 摘要（OrbStack 实测） |
| AI 降级 | `make drill-degrade` | AI off / endpoint 不可达后 `verify` 仍绿 |

恰好 **2** 条可执行演练（D-14）：`loadtest` + `drill-degrade`。杀 TM 等 chaos 仅可写 README 可选附录，不挡完成。

## 6. 交叉引用

- 八段式启动与验收：项目根 [README.md](../README.md)
- 简历可复现陈述：[RESUME.md](RESUME.md)
- 降级四格勾选：[DEGRADE-CHECKLIST.md](DEGRADE-CHECKLIST.md)
- 仓库压测方法论子集：[benchmark/README.md](../../../benchmark/README.md)（全矩阵属 P5）
- 推理 / 护栏 / 成本教材：`ai/chapters/03`、`17`、`18`；机制样板 `examples/e11-async-io`、`e12-17`、`e12-18`
