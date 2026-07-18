# 08 · AI 降级

## 规则

1. **默认关闭外呼：** 无模型时规则路径必须绿（p01 `--ai.enabled=false`，`ai_source=DISABLED`）（总则 #12）。
2. **BudgetGate 在 Async 之前：** 超预算 Side Output 短路，打点 `budget_trips`，禁止先调用再熔断。
3. **超时 → DEGRADED：** 作业不因模型慢而重启；保留规则标签。
4. **Guardrail 在 Sink 前：** 阻断敏感输出并脱敏落库，`guardrail_blocks++`。
5. **可观测：** 只用真实序列 `flink_taskmanager_job_task_operator_p01_*`；看板 [`monitoring/ai-cost.json`](../monitoring/ai-cost.json)。
6. **演练清单必跑：** [`projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md`](../projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md)。

## 理由

模型可用性抖动是常态；主数据链路必须以规则/特征为底线，AI 为增强。

## 反例

- 同步 HTTP 调 Ollama 堵死 Task 线程。
- 无预算把本机 GPU/CPU 打满导致 TM kill（`numRestarts` 飙升）。
- 文档只给空洞外链、不写本仓可执行降级步骤（违禁）。

## 落地互链

- p01 ADR：[`projects/p01-log-ai-platform/docs/adr/0001-ai-path-degradable.md`](../projects/p01-log-ai-platform/docs/adr/0001-ai-path-degradable.md)
- AI 专书第 III 部：[`ai/`](../ai/)
- 生产化模块：[`docs/14-production/README.md`](../docs/14-production/README.md)
