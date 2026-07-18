# p01 · 日志 AI 平台（规则路径先行）

> 对应企业实战模块 15-01 · GSD Phase 4（LOG-01–LOG-05）
> 目录锁定：`projects/p01-log-ai-platform`。架构/ADR/降级清单/简历页随后续切片补齐（见 `docs/DEGRADE-CHECKLIST.md`，随 04-03 交付）。

## 1. 背景

结构化日志要从 Kafka 流式解析、富化特征、打规则标签，并落到可断言的权威表。生产叙事要求：**没有外部模型也能讲完故事**——默认关闭 AI，只跑解析 → Keyed 特征 → 规则标签 → ClickHouse；有 Ollama 时再开 Async 风险分级旁路（`verify-ai`，04-03）。本切片交付 V2 规则路径：独立 compose profile `p01`、零 Ollama/Milvus 依赖的端到端可复现验收。

硬验收（本切片）：

- 独立 `make up-p01` 不影响 default `make up`
- 默认 `--ai.enabled=false`：造数 `rule-auth-fail` 后 CH `flinklab.log_results` 出现 `rule_label=AUTH_FAIL`，且 `ai_source=DISABLED`
- `verify.sh` **以 ClickHouse 为唯一权威出口**；Kafka 仅诊断；失败非 0
- `RuleTaggerTest` / `ParseLogJsonTest` 单测绿；主 pom 禁止 Agents / Milvus / CEP

## 2. 架构

```text
[gen_log_events.py] --produce--> Kafka logs.events (宿主机 localhost:9094)
                                            |
                               Flink JobManager / TaskManager
                                            |
                               LogAiJob（默认 ai.enabled=false）
                 ParseLogJson → keyBy(service) → FeatureEnricher
                              → RuleTagger → ClickHouseLogSink
                                            |
                                            ▼
                              CH flinklab.log_results
                         （rule_label / ai_source / feature_json）
                                            |
                                            ▼
                         verify.sh（RULE_LABEL 白名单 + COUNT）
```

规则路径不调用 Ollama、不读 Redis、不写 Kafka 作为验收出口。AI 旁路（Async → 宿主机 Ollama `/api/chat`）与 `verify-ai` / 护栏预算在 04-03+ 接线；本作业在 `ai.enabled=false` 时整图零外呼。

Compose 隔离：`profiles: ["p01"]` 的 `p01-init` 幂等创建 `logs.events` + `flinklab.log_results` DDL；不写入 default `init.sh`。Milvus 仍走既有 `--profile ai`，不绑进 p01 默认验收。

## 3. 代码

| 路径 | 职责 |
|---|---|
| `LogAiJob` | Kafka → Parse → Enrich → Rule → CH；算子 `uid` 前缀 `p01-` |
| `JobConfig` | 手写 `--key` / `--key=value`；`--ai.enabled` 默认 `false` |
| `ParseLogJson` | JSON → `LogEvent`；脏数据丢弃；拒引号/反斜杠 |
| `enrich/FeatureEnricher` | 按 service Keyed State 累计 ERROR 计数 → `featureJson` |
| `rule/RuleTagger` | `AUTH_FAIL` / `ERROR_BURST` / `NONE`；可单测纯逻辑 |
| `sink/ClickHouseLogSink` | HTTP SinkV2 → `flinklab.log_results`；拒引号/反斜杠 |
| `model/LogEvent` / `LogResult` | 事件与落库前中间态 POJO |
| `sql/clickhouse_log_results.sql` | 单语句 CREATE，供 p01-init POST |
| `scripts/gen_log_events.py` | `rule-auth-fail` / `rule-error-burst` + `--rate/--duration` |
| `scripts/verify.sh` | `RULE_LABEL` 白名单 + CH COUNT 权威断言 |

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

端口速查：Flink UI `http://localhost:8081`；Kafka 宿主机 `localhost:9094`（容器内作业用 `kafka:9092`，勿混用）；ClickHouse `localhost:8123`（`flinklab` / `flinklab123`，与 `docker/.env` 一致，仅本地 lab）。

## 5. 验证

权威出口是 ClickHouse，不是 Kafka。默认关闭 AI，**不依赖** 宿主机 Ollama / Milvus。

```bash
cd projects/p01-log-ai-platform

# 正例：规则路径落库 AUTH_FAIL
make verify
# 期望：ok log_results_match=N rule_label=AUTH_FAIL ...（exit 0）

# 一键清空 → 造数 → 轮询 CH（可选）
make match

# 负例：清空后必须非 0（证明不以 Kafka 有消息放行）
make truncate-results
bash scripts/verify.sh   # 期望 FAIL … count >= 1, got match=0

# 非法 RULE_LABEL 拒绝注入
RULE_LABEL=DROP_TABLE bash scripts/verify.sh   # 期望 FAIL: RULE_LABEL 非法
```

可选环境变量：`RULE_LABEL`（白名单 `AUTH_FAIL|ERROR_BURST|NONE`，默认 `AUTH_FAIL`）、`MIN_COUNT`（默认 `1`）。

AI 路径验收（`make verify-ai`、降级勾选表）见后续切片与 `docs/DEGRADE-CHECKLIST.md`（随 04-03 交付）；主门禁以本 README 的 `make verify` 为准。

## 6. 踩坑

| 现象 | 原因 | 处理 |
|---|---|---|
| `verify` 一直 match=0 | 作业未 RUNNING / 未造数 / watermark 未推进 | Flink UI 看 Job；`make gen` 含尾 INFO 推进水位；等 10–30s 再 `make verify` |
| Sink 抛「含引号或反斜杠」 | `featureJson`/字段含 `'` `"` `\` | 特征用无引号紧凑格式；Parse 已拒注入字符 |
| `p01-init` DDL 失败 | CH HTTP 禁多语句 | `sql/clickhouse_log_results.sql` 保持单条 CREATE |
| 宿主机连不上 Kafka | 用了容器内地址 | 造数 `--bootstrap localhost:9094`；作业内 `kafka:9092` |
| `make up` 被拖慢 | 误把 p01 绑进 default | 只用 `make up-p01`；default `up` 不含 `--profile p01` |

## 7. 最佳实践

- **验收权威在 CH**：任何「Kafka 有消息」只能当诊断，不能 `exit 0`
- **AI 默认关闭**：`JobConfig` 默认 + `submit` 显式 `--ai.enabled=false` 双重保险
- **算子稳定 uid**：`p01-*` 前缀，便于后续 savepoint / 扩缩
- **白名单拼 SQL**：`RULE_LABEL` / Sink 枚举校验后再拼查询或 INSERT（T-04-01）
- **profile 隔离**：项目 DDL/topic 走 `p01-init`，不污染 default `init.sh`

## 8. 面试题与参考

**Q1：为什么规则路径不用 Redis 做特征？**  
A：LOG-02 要求无外部模型/缓存时仍可演示；Keyed State 绑定 service 即可累计 ERROR 计数，减少「Redis 未起导致验收红」的失败面。Redis 可作为附录增强，不挡默认 `verify`。

**Q2：`ai.enabled=false` 时作业图里还有 AI 算子吗？**  
A：本切片（V2）整图只有 Parse→Enrich→Rule→CH，不挂 Async Ollama。后续切片在开关为 true 时旁路挂接，关闭时仍走规则落库且 `ai_source=DISABLED`。

**Q3：verify 如何防 SQL 注入式环境变量？**  
A：`RULE_LABEL` 先走 `case` 白名单，非法立即非 0；只有枚举常量拼进 `WHERE rule_label='…'`。

参考（仓库内）：

- 基座版本矩阵与端口：[根 README](../../README.md)
- Compose profile / p01-init：`docker/docker-compose.yml`、`docker/Makefile` 的 `up-p01`
- p03 CH 权威验收样板：`projects/p03-vehicle-monitoring/scripts/verify.sh`
- Async I/O / 降级论述：`examples/e11-async-io/`、`ai/chapters/03-streaming-inference.md`（AI 旁路接线属 04-03）
