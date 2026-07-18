# p01 · 日志 AI 平台（规则路径 + 可选 Async Ollama）

> 对应企业实战模块 15-01 · GSD Phase 4（LOG-01–LOG-05）
> 目录锁定：`projects/p01-log-ai-platform`。降级勾选表见 [`docs/DEGRADE-CHECKLIST.md`](docs/DEGRADE-CHECKLIST.md)。

## 1. 背景

结构化日志要从 Kafka 流式解析、富化特征、打规则标签，并落到可断言的权威表。生产叙事要求：**没有外部模型也能讲完故事**——默认关闭 AI，只跑解析 → Keyed 特征 → 规则标签 → ClickHouse；有 Ollama 时再开 Async 风险分级旁路（`verify-ai`）。

硬验收：

- 独立 `make up-p01` 不影响 default `make up`
- 默认 `--ai.enabled=false`：造数 `rule-auth-fail` 后 CH `flinklab.log_results` 出现 `rule_label=AUTH_FAIL`，且 `ai_source=DISABLED`
- `verify.sh` **以 ClickHouse 为唯一权威出口**；Kafka 仅诊断；失败非 0
- 可选 `verify-ai`：宿主机 Ollama 可用时断言 `ai_source=AI` 且 `ai_risk∈{HIGH,MEDIUM,LOW}`（**非**默认门禁）
- 主 pom 禁止 Agents Preview 坐标 / 向量库 SDK / CEP

**REQ LOG-03 选型说明：** 里程碑条文枚举含 ML_PREDICT / Agents / Milvus；本项目实现收窄为 **Async I/O → 宿主机 Ollama `/api/chat` 风险分级**（见降级清单「REQ 原文 → 实现选型」）。

## 2. 架构

```text
[gen_log_events.py] --produce--> Kafka logs.events (宿主机 localhost:9094)
                                            |
                               Flink JobManager / TaskManager
                                            |
                               LogAiJob
                 ParseLogJson → keyBy(service) → FeatureEnricher → RuleTagger
                              |                                      |
                   ai.enabled=false                          ai.enabled=true
                   ai_source=DISABLED                        AsyncDataStream
                                                             unorderedWaitWithRetry
                                                             → OllamaRiskAsyncFunction
                                                             POST host.docker.internal:11434/api/chat
                                                             timeout → DEGRADED/UNKNOWN
                                            |
                                            ▼
                              CH flinklab.log_results
                         （rule_label / ai_source / ai_risk / feature_json）
                                            |
                    verify.sh（规则）              verify_ai.sh（可选 AI）
```

Compose 隔离：`profiles: ["p01"]` 的 `p01-init` 幂等创建 `logs.events` + `flinklab.log_results` DDL。Milvus 仍走既有 `--profile ai`，不绑进 p01 默认验收；本项目无 `verify-rag`。

## 3. 代码

| 路径 | 职责 |
|---|---|
| `LogAiJob` | Kafka → Parse → Enrich → Rule →（可选 Async Ollama）→ CH；`uid` 前缀 `p01-` |
| `JobConfig` | 手写 `--key` / `--key=value`；`--ai.enabled` 默认 `false`；`--ai.capacity`≤16 |
| `ai/OllamaRiskAsyncFunction` | JDK `HttpClient.sendAsync` → `/api/chat`；`stream/think=false`；timeout→DEGRADED |
| `ParseLogJson` | JSON → `LogEvent`；脏数据丢弃；拒引号/反斜杠 |
| `enrich/FeatureEnricher` | 按 service Keyed State 累计 ERROR 计数 → `featureJson` |
| `rule/RuleTagger` | `AUTH_FAIL` / `ERROR_BURST` / `NONE`；可单测纯逻辑 |
| `sink/ClickHouseLogSink` | HTTP SinkV2 → `flinklab.log_results`；拒引号/反斜杠 |
| `scripts/gen_log_events.py` | `rule-auth-fail` / `rule-error-burst` / `ai-risk-high` + `--rate/--duration` |
| `scripts/verify.sh` | `RULE_LABEL` 白名单 + CH COUNT（默认门禁） |
| `scripts/verify_ai.sh` | Ollama `/api/tags` 前置 + CH `ai_source=AI`（可选轨） |
| `docs/DEGRADE-CHECKLIST.md` | 无 Ollama / 无 Milvus / AI 关闭 / 超时降级 四格勾选 |

Maven 独立模块（不挂 `examples/` 父工程），版本对齐仓库 SSOT（Flink 2.2.1 / Kafka connector 5.0.0-2.2）。

## 4. 启动

前置：OrbStack arm64，本机已安装 JDK 21、Maven、uv、Docker。

```bash
# 1) 基座（不含 p01 profile）
cd docker
make up
make init

# 2) p01 topic + ClickHouse DDL
make up-p01

# 3) 打包并提交作业（默认 AI 关闭）
cd ../projects/p01-log-ai-platform
make package
make submit
# submit 显式传 --ai.enabled=false；与 JobConfig 默认双重保险

# 4) 可判定造数（AUTH_FAIL）
make gen
```

可选 AI 作业（需宿主机 `ollama serve` + 已 pull 模型）：

```bash
# 模型名以本机 ollama list 为准；无 qwen3:8b 时覆盖，例如 qwen3.5:9b-mlx
make submit-ai AI_MODEL=qwen3.5:9b-mlx
make gen-ai
```

端口速查：Flink UI `http://localhost:8081`；Kafka 宿主机 `localhost:9094`（容器内作业用 `kafka:9092`，勿混用）；ClickHouse `localhost:8123`（`flinklab` / `flinklab123`）；Ollama 宿主机 `localhost:11434`（容器内 `host.docker.internal:11434`）。

## 5. 验证

### 5.1 主门禁（不依赖 Ollama）

权威出口是 ClickHouse，不是 Kafka。

```bash
cd projects/p01-log-ai-platform

make verify
# 期望：ok log_results_match=N rule_label=AUTH_FAIL ...（exit 0）

make match          # 清空 → 造数 → 轮询 CH

# 负例：清空后必须非 0
make truncate-results
bash scripts/verify.sh   # 期望 FAIL … count >= 1, got match=0
```

### 5.2 可选 AI 轨（依赖 Ollama）

前置：`curl -sf http://127.0.0.1:11434/api/tags` 成功，且 `ollama list` 有可用模型。

```bash
make verify-ai AI_MODEL=qwen3.5:9b-mlx
# 期望：ok ai_source=AI ai_risk_match=N ...
```

`verify-ai` 失败**不**代表默认 `verify` 失败；主 CI/Phase 门禁以 `make verify` 为准（D-06）。

### 5.3 降级勾选

显式四格（无 Ollama / 无 Milvus / AI 关闭 / AI 超时降级）与可执行命令见 **[`docs/DEGRADE-CHECKLIST.md`](docs/DEGRADE-CHECKLIST.md)**。

## 6. 踩坑

| 现象 | 原因 | 处理 |
|---|---|---|
| `verify` 一直 match=0 | 作业未 RUNNING / 未造数 / watermark 未推进 | Flink UI 看 Job；`make gen` 含尾 INFO 推进水位；等 10–30s 再 `make verify` |
| `verify-ai` 全 DEGRADED | 模型名不匹配 / 超时过短 / think 未关 | `ollama list` 后 `AI_MODEL=…`；`AI_TIMEOUT_MS=30000`；请求体强制 `think=false` |
| Sink 抛「含引号或反斜杠」 | `featureJson`/字段含 `'` `"` `\` | 特征用无引号紧凑格式；Parse 已拒注入字符 |
| `p01-init` DDL 失败 | CH HTTP 禁多语句 | `sql/clickhouse_log_results.sql` 保持单条 CREATE |
| 宿主机连不上 Kafka | 用了容器内地址 | 造数 `--bootstrap localhost:9094`；作业内 `kafka:9092` |
| `make up` 被拖慢 | 误把 p01 绑进 default | 只用 `make up-p01`；default `up` 不含 `--profile p01` |

## 7. 最佳实践

- **验收权威在 CH**：任何「Kafka 有消息」只能当诊断，不能 `exit 0`
- **AI 默认关闭**：`JobConfig` 默认 + `submit` 显式 `--ai.enabled=false` 双重保险
- **双轨分离**：`verify` 零模型；`verify-ai` 本机可选硬验收
- **Async 纪律**：LLM HTTP 只走 `unorderedWaitWithRetry`；禁止 `asyncInvoke` 内 `Future.get()`
- **算子稳定 uid**：`p01-*` 前缀，便于后续 savepoint / 扩缩
- **白名单拼 SQL**：`RULE_LABEL` / `ai_risk` / Sink 枚举校验后再拼查询（T-04-01）
- **profile 隔离**：项目 DDL/topic 走 `p01-init`；向量库仅 `--profile ai`

## 8. 面试题与参考

**Q1：为什么规则路径不用 Redis 做特征？**  
A：LOG-02 要求无外部模型/缓存时仍可演示；Keyed State 绑定 service 即可累计 ERROR 计数，减少「Redis 未起导致验收红」的失败面。

**Q2：`ai.enabled=false` 时作业图里还有 AI 算子吗？**  
A：没有。`LogAiJob` 仅在 `cfg.aiEnabled==true` 时挂接 `AsyncDataStream.unorderedWaitWithRetry`；关闭时透传规则结果且 `ai_source=DISABLED`，零 Ollama 调用。

**Q3：verify / verify-ai 如何防 SQL 注入式环境变量？**  
A：`RULE_LABEL`、`AI_RISK` 先走白名单 `case`，非法立即非 0；只有枚举常量拼进 `WHERE`。

**Q4：为什么不用 Flink Agents / Milvus 做 LOG-03？**  
A：Agents 为 Preview 坐标，进主 pom 会破坏主构建（D-02）；Milvus 作可选增强且不绑 p01 profile（D-03）。里程碑「至少一条可观察 AI 路径」由 Async Ollama 风险分级满足，并配显式降级清单。

参考（仓库内）：

- 降级勾选表：[docs/DEGRADE-CHECKLIST.md](docs/DEGRADE-CHECKLIST.md)
- 基座版本矩阵与端口：[根 README](../../README.md)
- Compose profile / p01-init：`docker/docker-compose.yml`、`docker/Makefile` 的 `up-p01`
- Async I/O 样板：`examples/e11-async-io/`（C2 timeout/retry/降级）
- 推理降级论述：`ai/chapters/03-streaming-inference.md`
