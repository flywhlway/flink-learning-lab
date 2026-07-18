# Phase 5: p02 实时推荐 - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-18
**Phase:** 5-p02 实时推荐
**Mode:** `--auto --chain`
**Areas discussed:** 在线特征存储选型, 召回/打分闭环形态, 工程骨架与权威出口, 压测演练与文档包

---

## 在线特征存储选型

| Option | Description | Selected |
|--------|-------------|----------|
| 双通道 Keyed State + Redis | 对齐 e12-06；状态新鲜度 + Redis 点查可观察 | ✓ |
| 仅 Keyed State | 更简单但缺少在线特征库演示 | |
| 仅 Redis | 缺少状态语义教学，且故障边界更脆 | |

**User's choice:** [auto] 双通道 Keyed State + Redis（recommended default）
**Notes:** jedis 与 e07-C7 对齐；特征 key `feature:{userId}:*`；at-least-once + 攒批写明边界。

`[auto] 在线特征存储选型 — Q: "在线特征落哪里？" → Selected: "双通道 Keyed State + Redis" (recommended default)`

---

## 召回/打分闭环形态

| Option | Description | Selected |
|--------|-------------|----------|
| 规则/简单加权 + Top-K | 可解释、零外部模型、可断言 | ✓ |
| ANN/向量召回（Milvus） | 与 p01 重叠，增加硬依赖 | |
| LLM 重排 | STACK 声明非必须；超出 MVP | |

**User's choice:** [auto] 规则/简单加权 + Top-K（recommended default）
**Notes:** 候选目录用合成静态或 PG 维表；输出双写 Kafka + ClickHouse。

`[auto] 召回/打分闭环形态 — Q: "召回与打分怎么做？" → Selected: "规则/简单加权 + Top-K" (recommended default)`

---

## 工程骨架与权威出口

| Option | Description | Selected |
|--------|-------------|----------|
| 独立 profile p02 + CH 权威 | 复制 p03/p01 纪律；目录 `p02-realtime-reco` | ✓ |
| 挂入 examples 父工程 | 违反已锁定样板 | |
| Redis/Kafka 单独放行 verify | 削弱验收权威 | |

**User's choice:** [auto] 独立 `--profile p02` + CH 权威出口（recommended default）
**Notes:** topic 建议 `reco.events` / `reco.results`；行为 JSON 契约锁定最小字段集。

`[auto] 工程骨架与权威出口 — Q: "工程怎么落、验收看哪里？" → Selected: "profile p02 + CH 权威" (recommended default)`

---

## 压测演练与文档包

| Option | Description | Selected |
|--------|-------------|----------|
| Redis 降级演练 + loadtest + ADR 双通道 | 对齐 p01 降级叙事，换 Redis 域 | ✓ |
| watermark 停滞演练 | p03 已做过；本 Phase 差异化不足 | |
| 仅散文 checklist 无脚本 | 违反 RECO-03 / 不变量 | |

**User's choice:** [auto] Redis 降级 + loadtest baseline；ADR 锁定双通道特征（recommended default）
**Notes:** 恰好 2 条演练；文档包 ARCHITECTURE/ADR/RESUME；回填 15-02。

`[auto] 压测演练与文档包 — Q: "演练与文档包如何定？" → Selected: "Redis 降级 + loadtest + ADR 双通道" (recommended default)`

---

## Claude's Discretion

- CH/PG 表名列设计、Top-K、打分权重、eventType 枚举、是否单独 Grafana JSON、Redis 默认连接参数、压测 eps/时长实测填写。

## Deferred Ideas

- ANN/Milvus 第二召回、LLM 重排、真实生产数据、P5 全矩阵与 Operator — 见 CONTEXT.md `<deferred>`
