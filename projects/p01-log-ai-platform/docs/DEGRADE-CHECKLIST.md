# p01 降级核对清单（LOG-03 / D-05）

本清单把「没有外部模型也能讲完故事」落成可勾选、可执行的命令与可观察输出。  
**主门禁**仍是 `make verify`（规则路径，零 Ollama）。  
**可选 AI 轨**是 `make verify-ai`（要求宿主机 Ollama + `--ai.enabled=true`）。

## REQ 原文 → 本项目实现选型（A5）

| REQUIREMENTS LOG-03 原文枚举 | 本项目落地 | 是否硬依赖 |
|---|---|---|
| SQL `ML_PREDICT` | **不采用**为验收主路径（已有 e12-03 教材） | 否 |
| Flink Agents Preview | **不交付**；禁止进 p01 主 `pom.xml`（D-02） | 否 |
| Milvus / VECTOR_SEARCH / Streaming RAG | **可选增强**；走 docker `--profile ai`，**不**绑进 p01 profile；本 Phase **无** `verify-rag` | 否 |
| （CONTEXT 收窄）Async I/O → 宿主机 Ollama `/api/chat` 风险分级 | **已交付**：`OllamaRiskAsyncFunction` + `verify-ai` | 仅可选轨 |

## 四格勾选表

复制下表到笔记或 PR 描述；每行执行「命令」列，对照「预期可观察输出」打勾。

| # | 场景 | 命令（在 `projects/p01-log-ai-platform`） | 预期可观察输出 | 勾选 |
|---|---|---|---|---|
| 1 | **无 Ollama**（AI 轨前置失败，默认门禁仍绿） | `curl -sf http://127.0.0.1:11434/api/tags`（应失败）；另开：`make submit && make truncate-results && make gen && make verify` | `curl` 非 0；`make verify` 仍 `ok … rule_label=AUTH_FAIL`；CH：`SELECT count() FROM flinklab.log_results WHERE ai_source='DISABLED' AND rule_label='AUTH_FAIL'` ≥ 1 | ☐ |
| 2 | **无 Milvus**（p01 默认不启向量库） | `docker compose -f ../../docker/docker-compose.yml ps`（确认无 milvus 容器亦可）；`make verify` | 无 milvus 时 `verify` 仍 exit 0；**不**要求 `verify-rag`；向量库仅文档指向 `docker compose --profile ai` | ☐ |
| 3 | **AI 关闭**（`--ai.enabled=false`） | `make cancel-p01 \|\| true; make submit; make truncate-results; make gen; make verify`；核对：`docker compose -f ../../docker/docker-compose.yml exec -T clickhouse clickhouse-client --user flinklab --password flinklab123 --query "SELECT ai_source, count() FROM flinklab.log_results GROUP BY ai_source"` | 作业参数含 `--ai.enabled=false`；CH 以 `ai_source=DISABLED` 为主；`ai_risk` 为 `NONE`（规则路径）；**零**对 `host.docker.internal:11434` 的业务调用 | ☐ |
| 4 | **AI 超时降级到规则**（有 Ollama 但强制短超时） | 先 `ollama list` 确认模型；`make cancel-p01 \|\| true`；提交：`… flink run … --ai.enabled=true --ai.model=<本机模型> --ai.timeout-ms=1 --group-id=p01-log-ai-degrade`；`make truncate-results; make gen-ai`；等 30s 后：`… --query "SELECT ai_source, ai_risk, count() FROM flinklab.log_results GROUP BY ai_source, ai_risk"` | 作业 **RUNNING 不重启**；CH 出现 `ai_source=DEGRADED` 且 `ai_risk=UNKNOWN`（timeout 完成降级）；`rule_label` 仍可保留规则标签；作业不因超时失败 | ☐ |

### 可选正例（有 Ollama 时勾选）

| # | 场景 | 命令 | 预期 | 勾选 |
|---|---|---|---|---|
| A | **AI 旁路成功** | `make verify-ai AI_MODEL=<本机模型>`（无 `qwen3:8b` 时常用 `qwen3.5:9b-mlx`） | `ok ai_source=AI ai_risk_match=N …`；CH：`ai_source='AI' AND ai_risk IN ('HIGH','MEDIUM','LOW')` ≥ 1 | ☐ |

## 快速对照命令

```bash
# 默认门禁（无 Ollama）
cd projects/p01-log-ai-platform
make submit
make match          # truncate → gen rule-auth-fail → 轮询 verify

# 可选 AI 轨（需 ollama serve + 已 pull 模型）
make verify-ai AI_MODEL=qwen3.5:9b-mlx

# CH 抽样
docker compose -f ../../docker/docker-compose.yml exec -T clickhouse \
  clickhouse-client --user flinklab --password flinklab123 \
  --query "SELECT ai_source, ai_risk, rule_label, count() FROM flinklab.log_results GROUP BY ai_source, ai_risk, rule_label"
```

## 非交付说明（D-02 / D-03）

- **Agents**：仅附录概念；本仓库 p01 **不**引入 Agents Maven 坐标，也不作为 LOG-03 验收条件。
- **Milvus / RAG**：启用步骤见根 docker `--profile ai` 与 e12-04；p01 **不**把 milvus 绑进 `--profile p01`，**不**提供 `verify-rag` 以免拖垮默认门禁。
