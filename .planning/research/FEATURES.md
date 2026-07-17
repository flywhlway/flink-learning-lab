# Features Research

**Domain:** Flink 生产级 showcase 项目 + 生产化 + 总装 QA
**Researched:** 2026-07-17
**Confidence:** HIGH

## Table Stakes（缺一则不可声称「生产级项目」）

### 三项目共用

| Feature | Why table stakes | Complexity | Phase |
|---------|------------------|------------|-------|
| 独立 compose profile 一键起 | PHASES 验收口径 | M | p03/p01/p02 |
| 端到端数据路径可复现（造数→作业→可观察结果） | Core Value | M | 各项目 |
| 架构文档 + 至少 1 篇 ADR + 验证脚本 | PHASES 验收 | M | 各项目 |
| 压测脚本 + 故障演练剧本（可执行） | 硬指标，不可降级 | L | 各项目 |
| 简历陈述一页（问题/方案/指标/权衡） | 受众=架构师求职 | S | 各项目 |
| 八段式或等价章节结构 | 根 README 约定 | S | 文档 |
| 版本登记进 SSOT 后再用新组件 | 不变量 | S | 全程 |

### p03 车联网监控

| Feature | Why | Complexity | Deps |
|---------|-----|------------|------|
| 告警链路：事件→Kafka→CEP→Side Output→CH/通知 | 用户锁定第一会话 | M | e10 C5 |
| 模式库（≥3 条）+ within/连接语义/skip 五元组登记 | 生产 CEP 纪律 | M | 告警链路 |
| Broadcast 动态选择预编译模式 | e10 已预告路线 | L | 模式库 |
| 监控大盘：窗口聚合指标→Prometheus/Grafana | 用户锁定第二会话 | M | 告警可跑 |
| 异常检测（规则或简单统计阈值） | 「监控」闭环 | M | 大盘 |
| 故障演练：watermark 停滞 / TM 杀进程 / Kafka 延迟 | 硬验收 | M | 端到端 |

### p01 日志 AI 平台

| Feature | Why | Complexity | Deps |
|---------|-----|------------|------|
| 日志接入与结构化（Kafka） | 平台起点 | M | docker |
| 流式特征/富化 | AI 前置 | M | e12-06 |
| 至少一条 AI 路径（ML_PREDICT 或 Agents 或向量检索） | 「日志 AI」名副其实 | L | P3 |
| 明确降级路径（无 LLM/Milvus 仍可演示） | P3 纪律延续 | S | AI 路径 |
| 成本/护栏指标（可观测） | 生产 AI 差异点 | M | e12-17/18 |

### p02 实时推荐

| Feature | Why | Complexity | Deps |
|---------|-----|------------|------|
| 用户行为流接入 | 推荐起点 | S | Kafka |
| 在线特征（Redis 或状态） | 实时性 | M | e07 Redis |
| 候选生成 + 打分（规则/简单模型即可） | 闭环 | M | 特征 |
| 结果写回 Kafka/可查询存储 | 可验证 | S | — |

### P5 生产化

| Feature | Why | Complexity |
|---------|-----|------------|
| benchmark 矩阵 + baseline.md | PHASES | L |
| Operator 部署 + Blue/Green 演练 | PHASES | L |
| CI/CD + GitOps 一条完整路径 | PHASES | L |
| best-practice 完整规范 | PHASES | M |
| interview ≥150 | PHASES | M |
| monitoring 看板 JSON 可导入 | PHASES | M |

### P6 总装 QA

| Feature | Why | Complexity |
|---------|-----|------------|
| qa_check.sh 全绿 | 门禁 | M |
| 案例 ≥100、文档 ≥30k 行 | PHASES | S（计量） |
| 断链 + 违禁词清零 | 不变量 | M |
| README 终稿与版本状态一致 | 收官 | S |

## Differentiators（简历/面试加分，建议纳入 v1）

| Feature | Why differentiate | Phase |
|---------|-------------------|-------|
| p03 模式库评审五元组 + 状态上界论证 | 区别于「跑通 CEP HelloWorld」 | p03 |
| p01 AI 降级路径与成本熔断同演示 | 生产 AI 成熟度信号 | p01 |
| 三项目统一故障演练目录结构 | 可迁移的运维资产 | P4 |
| OrbStack arm64 上真实 Blue/Green | 本地生产化稀缺 | P5 |
| 交叉引用教材↔项目双向锚点 | 学习工程完整性 | P6 |

## Anti-Features（学习工程故意不做）

| Anti-feature | Why exclude |
|--------------|-------------|
| 多租户计费/权限中台 | 非本仓目标 |
| 云厂商锁定一键部署为唯一路径 | 破坏本地复现 |
| 未验证的「完整 LLM Ops 平台」 | 易写成空文档 |
| 同时维护两套 GitOps 工具 | 稀释深度 |
| 真实车企/广告生产数据接入 | 合规与范围；用合成数据 |
| 把 showcase 做成不可拆的巨型 monorepo 作业 | 违反独立 profile |

## Feature Dependencies

```text
e10 C5 ──► p03 告警链路 ──► p03 模式库/Broadcast ──► p03 大盘
                                    │
docker 基座 ─────────────────────────┴──► p01 / p02
P3 ai/e12 ──► p01 AI 路径（可降级）
p03 验收样板 ──► p01/p02 复制工程纪律（compose/ADR/压测/演练）
P4 全部 ──► P5（有真实作业才值得 Operator/压测矩阵）
P5 ──► P6 计量与交叉引用收官
```

## Recommended v1 Scope（供 REQUIREMENTS 直接采用）

**Must (v1):** 上表 Table Stakes 全部  
**Should (v1):** Differentiators 中 p03 五元组、p01 降级+成本、统一演练结构、Blue/Green  
**Defer (v2+):** 商业动态 CEP、完整多 Agent 协作生产网关、云上多区域容灾

---
*Research date: 2026-07-17 · Sources: PROJECT.md, PHASES.md, e10, P3 CHANGELOG notes*
