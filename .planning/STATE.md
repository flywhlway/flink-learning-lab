---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
current_plan: 4
status: ready_to_plan
last_updated: 2026-07-18T04:33:19.562Z
last_activity: 2026-07-18
progress:
  total_phases: 7
  completed_phases: 5
  total_plans: 23
  completed_plans: 23
  percent: 71
stopped_at: Phase 5 complete (4/4) — ready to discuss Phase 6
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-07-17)

**Core value:** 每个生产级项目必须在 OrbStack arm64 上独立 compose profile 一键起、端到端可复现，且压测与故障演练真实跑通——不可验证的内容不合入。
**Current focus:** Phase 6 — p5 生产化

## Current Position

Phase: 6
Plan: 4 of 4
Current Plan: Not started
Total Plans in Phase: 4
Status: Ready to plan
Last activity: 2026-07-18

Progress: [██████████] 100%

## Performance Metrics

**Velocity:**

- Total plans completed: 17
- Average duration: —
- Total execution time: 0 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| 1 | 4 | - | - |
| 2 | 5 | - | - |
| 3 | 4 | - | - |
| 5 | 4 | - | - |

**Recent Trend:**

- Last 5 plans: —
- Trend: —

*Updated after each plan completion*
| Phase 01-p03 P00 | 2min | 2 tasks | 6 files |
| Phase 01-p03 P01 | 2min | 3 tasks | 4 files |
| Phase 01-p03 P02 | 5min | 2 tasks | 10 files |
| Phase 01-p03 P03 | 61min | 3 tasks | 9 files |
| Phase 02-p03 P00 | 2min | 2 tasks | 5 files |
| Phase 02 P01 | 2min | 2 tasks | 6 files |
| Phase 02 P02 | 1min | 1 tasks | 4 files |
| Phase 02 P02b | 2min | 2 tasks | 12 files |
| Phase 02 P03 | 5min | 3 tasks | 9 files |
| Phase 04 P00 | 2min | 3 tasks | 11 files |
| Phase 04 P01 | 3min | 3 tasks | 9 files |
| Phase 04 P02 | 4min | 3 tasks | 10 files |
| Phase 04 P03 | 5min | 3 tasks | 9 files |
| Phase 04 P04 | 3min | 3 tasks | 10 files |
| Phase 04 P05 | 6min | 3 tasks | 12 files |
| Phase 05 P00 | 2min | 3 tasks | 11 files |
| Phase 05 P01 | 3min | 3 tasks | 10 files |
| Phase 05 P02 | 13min | 3 tasks | 16 files |
| Phase 05 P03 | 7min | 3 tasks | 11 files |

## Accumulated Context

### Decisions

- 里程碑覆盖 P4+P5+P6；顺序 p03→p01→p02→P5→P6
- P4 验收三项全硬；p03 先告警后大盘
- GSD 按交付物切细（7 phases）；Vertical MVP
- 跳过 codebase map；调研子代理 API 限额时内联完成
- [Phase 01-p03]: Wave 0 故意省略 Pattern.within(30s)，单测断言失败以建立 RED 反馈环
- [Phase 01-p03]: verify.sh 以 ClickHouse count 为唯一 exit 0 条件；Kafka 仅诊断
- [Phase 01-p03]: p03 独立 pom，不挂入 examples/ 父工程
- [Phase 01-p03]: p03-init 复用 KAFKA_IMAGE，经 wget POST ClickHouse HTTP 执行挂载 DDL（不引入未登记镜像）
- [Phase 01-p03]: up-p03 显式 up p03-init；default up 目标不加 --profile p03
- [Phase 01-p03]: JobConfig 手写 --key 解析（Flink 2.2 无 ParameterTool）
- [Phase 01-p03]: MATCH/TIMEOUT 经 union 后双写同一 Kafka topic 与 vehicle_alerts 表（alert_type 区分）
- [Phase 01-p03]: submit 仅复制 p03-vehicle-monitoring-*.jar（排除 original-），flink run -c 固定主类
- [Phase 01-p03]: verify 唯一放行条件为 ClickHouse MATCH count；Kafka 仅诊断
- [Phase 01-p03]: 造数在 DTC 后追加晚心跳推进 watermark（ooo=5s），避免 MATCH 迟迟不落库
- [Phase 01-p03]: Flink FLINK_PROPERTIES 固定 S3a SimpleAWSCredentialsProvider，消除 INITIALIZING 挂起
- [Phase 02-00]: Gate 单测用 resolveActivePatterns/isNewerVersion/isAllowed 包内辅助，避免 MiniCluster
- [Phase 02-00]: verify PATTERN_ID 仅白名单三常量拼 SQL；默认 HARSH_THEN_FAULT
- [Phase 02]: Registry Entry 用 record(id, pattern) 对齐 Wave 0 entry.id()/entry.pattern() 契约
- [Phase 02]: within 秒数锁定 CONTEXT 默认 30/20/15，未做 Discretion 微调
- [Phase 02]: 排除 PatternActivationGateTest 编译直至 02-02 交付 Gate/ControlMessage
- [Phase 02]: Gate 采用非 keyed BroadcastProcessFunction（RESEARCH A1 / RESOLVED Q1）
- [Phase 02]: AlertEvent 仅增加 patternId 最小字段；Handler/DDL/作业接线留给 02-02b
- [Phase 02]: 原始 activePatterns.size()>3 整条跳过；未知 ID 求交过滤
- [Phase 02]: HarshThenFaultHandler 继承 AlertPatternHandler，三 Handler 共享 TIMEOUT_TAG
- [Phase 02]: ParsePatternControlJson 随作业接线落地；Sink patternId 白名单+拒注入字符
- [Phase 02]: 切换验收主路径为 make verify-switch（CH pattern_id），禁止 Kafka 单独放行
- [Phase 02]: p03-init DDL 拆成 CREATE + ALTER 两次 POST（CH 24.8 HTTP 无 multiquery）
- [Phase 02]: auto 链下 Task 3 human-verify 以 OrbStack CH 断言输出为自动化验收证据
- [Phase 04]: Wave 0 故意引用尚未交付的 ParseLogJson/RuleTagger/BudgetGate，testCompile 失败以建立 RED 反馈环
- [Phase 04]: 四脚本以 --implemented 门闩保持默认非 0；verify 声明 RULE_LABEL 白名单与 CH 权威出口
- [Phase 04]: p01-init 独立 --profile p01；default make up 不加 p01
- [Phase 04]: 主 pom 禁止 flink-agents / Milvus / flink-cep（D-02/D-03/D-07）
- [Phase 04]: LogEvent 用公开字段 + 无参构造（Flink POJO）并保留访问器以兼容 Wave 0 单测
- [Phase 04]: 排除 RuleTaggerTest/BudgetGateTest 编译直至 04-02+ 交付生产类
- [Phase 04]: V1 LogAiJob 以 print 作临时 Sink，注释标明 V2 接 CH
- [Phase 04]: submit 显式 --ai.enabled=false，与 JobConfig 默认双重保险
- [Phase 04]: featureJson 用无引号紧凑格式，避开 Sink 拒双引号与合法 JSON 冲突
- [Phase 04]: RuleTagger.tag(LogEvent) 供单测；tag(LogResult) 叠加 ERROR_BURST 阈值 5
- [Phase 04]: 本切片不接 Async Ollama；LogAiJob 整图 Parse→Enrich→Rule→CH
- [Phase 04]: 默认模型名保留 qwen3:8b；Makefile/submit-ai 默认 AI_MODEL=qwen3.5:9b-mlx 对齐本机 ollama list
- [Phase 04]: submit-ai 使用独立 group-id=p01-log-ai-verify，避免与默认规则作业争抢消费组
- [Phase 04]: 护栏/预算/指标留给 04-04；本切片仅 Async 旁路 + 双轨验收 + 降级清单
- [Phase 04]: 护栏选型：静态 JobConfig --guardrail.keywords（非 Broadcast），对齐 e12-17 BLOCK 语义（D-12 Discretion）
- [Phase 04]: BudgetGate 仅在 ai.enabled=true 分支挂图；源码 BudgetGate 标识符仍位于 AsyncDataStream 之前
- [Phase 04]: PromQL 全名不臆造：README 以 :9249 grep 名片段为观察路径
- [Phase 04]: 压测默认 RATE=100 WARMUP=30s DURATION=90s（OrbStack 稳定 Discretion）
- [Phase 04]: drill_ai_degrade 同时 --ai.enabled=false + 不可达 endpoint
- [Phase 04]: 恰好 2 条硬演练：loadtest + drill-degrade
- [Phase 05]: Wave 0 故意不实现 ParseBehaviorJson/RuleScorer/RealtimeRecoJob，testCompile 失败建立 RED 反馈环
- [Phase 05]: drill/loadtest 用 --implemented 门闩；verify 走真实 CH 查询以便空表/未起时非 0
- [Phase 05]: PG reco_items 50 行种子由 make up-p02 psql 注入，p02-init 仅 Kafka+CH
- [Phase 05]: eventType 锁定 VIEW|CLICK|CART|BUY；Top-K=5；权重 VIEW=1/CLICK=3/CART=5/BUY=10
- [Phase 05]: BehaviorEvent 字段名对齐 Wave 0 单测 eventTime（非 plan 文案 eventTimeMs）
- [Phase 05]: RealtimeRecoJob 05-01 仅 print 透传，不接 CH/Kafka results
- [Phase 05]: pom testExclude RuleScorerTest 直至 05-02，避免挡住 Parse GREEN
- [Phase 05]: Redis 写语义文档锁定 at-least-once；写失败 catch+metric，主流 FeatureSnapshot 继续
- [Phase 05]: TopKScoreFunction feature_source=REDIS|STATE_ONLY；读失败不抛死作业
- [Phase 05]: CatalogLoader 显式 Class.forName(org.postgresql.Driver) + TopK ensureCatalog 懒重试
- [Phase 05]: compose PG_HOST_PORT/REDIS_HOST_PORT 可覆盖；本机 .env 用 15432/16379 避让端口冲突
- [Phase 05]: loadtest 墙钟吞吐用 gen 实际发送量，避免配置 eps 冒充实测
- [Phase 05]: drill EXIT trap 强制恢复 redis，避免污染后续 loadtest

### Pending Todos

None yet.

### Blockers/Concerns

- 研究员/roadmapper 子代理曾因 API 限额失败；后续 plan-phase 若再失败需内联降级
- P2/P3 遗留「沙箱未验证」债务：本里程碑禁止再以该理由标 ✅

## Deferred Items

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| *(none)* | | | |

## Session Continuity

Last session: 2026-07-18T04:29:56.091Z
Stopped at: Completed 05-02-PLAN.md
Resume file: 
None
