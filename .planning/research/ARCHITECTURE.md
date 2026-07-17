# Architecture Research

**Domain:** flink-learning-lab P4–P6
**Researched:** 2026-07-17
**Confidence:** HIGH

## System Overview

在已有 `docker/` 基座与 `examples/` 教学 Demo 之上，新增三个**可独立启停**的生产级项目目录，再叠加 P5 生产化层与 P6 质量门。共享基础设施，隔离项目配置与作业产物。

```text
┌─────────────────────────────────────────────────────────┐
│  docker/ (shared: Kafka Flink CH PG Redis MinIO Prom/Gr) │
│     profiles: default | ai | p03 | p01 | p02             │
└───────────────┬─────────────────────┬───────────────────┘
                │                     │
    ┌───────────▼──────────┐  ┌───────▼──────────┐
    │ projects/p03-vehicle │  │ projects/p01-log │
    │  jobs + docs + drills│  │  + p02-reco      │
    └───────────┬──────────┘  └───────┬──────────┘
                │                     │
                └──────────┬──────────┘
                           ▼
              P5 production/ + benchmark/ + Operator(K8s)
                           ▼
              P6 qa_check.sh + 交叉引用收官
```

## Component Boundaries

| Component | Responsibility | Talks to | Must NOT |
|-----------|----------------|----------|----------|
| `docker/` 基座 | 通用中间件生命周期 | 宿主机端口 | 塞入项目业务 SQL/作业细节 |
| `projects/pNN-*` | 该项目作业、文档、ADR、验证/压测/演练脚本、compose overlay | 基座服务 | 修改其他项目代码 |
| `examples/e10` 等 | 教学 Demo，保持轻量 | 基座 | 膨胀成生产项目 |
| `docs/` / `ai/` | 教材；项目回链 | 相对路径引用 | 复制大段项目专属运维步骤 |
| `production/` | Operator/GitOps/CI 蓝图落地 | OrbStack K8s | 依赖未交付的项目作业才能「空转文档」 |
| `scripts/qa_check.sh` | 仓库级门禁 | 全仓 | 被项目私自绕过 |

## Data Flows

### p03 告警链路（会话 1）

```text
车端事件生成器 → Kafka(vehicle.events)
  → Flink CEP(模式库) → Match 主流 / Timeout·告警 Side Output
  → Kafka(vehicle.alerts) → ClickHouse
  →（可选）Webhook/日志通知
```

### p03 监控大盘（会话 2）

```text
同一事件流 → 窗口聚合作业 → Flink Metrics / CH 指标表
  → Prometheus scrape → Grafana 看板（异常阈值面板）
```

### p01 日志 AI（示意）

```text
日志源 → Kafka → 解析/富化/特征
  → 规则路径（必可跑）
  → AI 路径（Ollama/Milvus/Agents，可降级旁路）
  → 告警或知识库写入 + 可观测指标
```

### p02 实时推荐（示意）

```text
行为事件 → Kafka → 特征（状态/Redis）→ 召回/打分 → Kafka(reco.out)
```

## Compose Profile Isolation

- **default**：现有 `make up` 不变，主干始终可起
- **profile p03/p01/p02**：仅追加 topic 初始化、项目专用连接器依赖挂载、可选旁路服务
- 项目 README 启动命令形如：`docker compose --profile p03 up -d` + 项目内 `make verify`
- 禁止在 default 路径硬依赖 p03 专用镜像

## Suggested Build Order

1. **p03 告警骨架** — compose profile + 造数 + 单模式 CEP 作业 + 验证脚本（证明验收样板）
2. **p03 模式库 + Broadcast** — ≥3 模式 + 五元组文档
3. **p03 大盘 + 演练/压测** — Grafana + 故障剧本
4. **p01** — 复制 p03 工程纪律，换日志 AI 域
5. **p02** — 同纪律，换推荐域
6. **P5** — benchmark 矩阵、Operator Blue/Green、GitOps、规范与题库扩容
7. **P6** — 全仓 QA、计量、README 终稿

## 与教材交叉引用

- p03 ↔ `docs/10-cep`、`examples/e10`（尤其 C5）
- p01 ↔ `ai/` + `examples/e12-*`
- p02 ↔ `docs/03-state`、`docs/07-connectors`、e07 Redis
- P5 ↔ `docs/14-*`（规划）、`production/`、`benchmark/`

## Risks to Architecture

| Risk | Mitigation |
|------|------------|
| 项目把共享 compose 改坏 | overlay + profile；CI 校验 `docker compose config` |
| 教学 Demo 与项目代码分叉失控 | 项目可复制模式后独立模块；e10 保持教学纯度 |
| P5 文档先于可运行作业 | ROADMAP 强制 P4 样板先于 Operator 深讲 |

---
*Research date: 2026-07-17*
