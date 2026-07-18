# Phase 4: p01 日志 AI 平台 - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-18
**Phase:** 4-p01 日志 AI 平台
**Mode:** `--auto --chain`（无交互；每题选 recommended 默认）
**Areas discussed:** AI 主路径选型, 降级开关与验收双轨, 工程骨架与权威出口, 成本护栏与演练文档

---

## AI 主路径选型

| Option | Description | Selected |
|--------|-------------|----------|
| Async I/O → Ollama 风险分级（推荐） | DataStream 主路径；主构建不依赖 Agents/Milvus | ✓ |
| SQL ML_PREDICT 为主路径 | 对齐 e12-03；与 p03 Java 工程形态不一致 | |
| Flink Agents 为主路径 | Preview；易阻塞主构建（PITFALLS） | |
| Milvus RAG 为唯一硬依赖 | ai-profile 已有，但无 Milvus 时难满足 LOG-02 | |

**User's choice:** [auto] Async I/O → Ollama 风险分级（recommended default）
**Notes:** Agents 隔离可选；Milvus RAG 为可选增强 + 独立 verify-rag

---

## 降级开关与验收双轨

| Option | Description | Selected |
|--------|-------------|----------|
| 默认 AI 关闭 + verify / verify-ai 双轨（推荐） | LOG-02 门禁与 LOG-03 可选硬验收分离 | ✓ |
| 默认 AI 开启，失败则整作业挂 | 违反降级纪律 | |
| 仅文档声明降级、无开关 | 不可脚本断言 | |

**User's choice:** [auto] 默认 `--ai.enabled=false` + verify/verify-ai 双轨
**Notes:** 降级核对清单为 LOG-03 文档硬要求

---

## 工程骨架与权威出口

| Option | Description | Selected |
|--------|-------------|----------|
| 复制 p03：独立 pom + profile p01 + CH 权威（推荐） | 最快对齐已验证纪律 | ✓ |
| 挂入 examples 父工程 | 违背 p03 样板与 Agents 隔离教训 | |
| Kafka 作为唯一验收出口 | 违背 p03 CH 权威纪律 | |

**User's choice:** [auto] `projects/p01-log-ai-platform` + profile p01 + CH 权威
**Notes:** 目录名对齐 docs/README 15-01

---

## 成本护栏与演练文档

| Option | Description | Selected |
|--------|-------------|----------|
| 指标+护栏/成本 MVP + 2 条演练 + ADR 降级主题（推荐） | 覆盖 LOG-04/05，不吞 P5 | ✓ |
| 完整 AI Gateway + OTel | 范围膨胀 | |
| 仅散文无脚本演练 | 违反硬验收 | |

**User's choice:** [auto] Dropwizard/Flink 指标 + e12-17/18 级 MVP + 降级演练 + loadtest baseline
**Notes:** 演练 1=AI 不可用仍绿；演练 2=loadtest

---

## Claude's Discretion

- 表名/列名、Async 超时、Ollama 模型默认、护栏词表、是否单独 Grafana JSON、Agents 附录 demo

## Deferred Ideas

- Agents 多专家协作、SQL ML_PREDICT 第二主路径、P5 看板/全矩阵、p02、Loki/OTel
