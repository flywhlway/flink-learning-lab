# Requirements: flink-learning-lab P4–P6

**Defined:** 2026-07-17
**Core Value:** 每个生产级项目必须在 OrbStack arm64 上独立 compose profile 一键起、端到端可复现，且压测与故障演练真实跑通——不可验证的内容不合入。

## v1 Requirements

### VEH · p03 车联网监控

- [x] **VEH-01**: 维护者可用独立 compose profile 一键启动 p03，且不影响 default `make up`
- [x] **VEH-02**: 维护者可复现告警链路（事件→Kafka→CEP→Side Output→ClickHouse/通知），验证脚本对可观察输出做断言
- [ ] **VEH-03**: 模式库至少 3 条模式，每条登记 within/连接语义/skip/状态上界五元组
- [ ] **VEH-04**: 作业支持通过 Broadcast 动态选择预编译模式集
- [ ] **VEH-05**: 维护者可复现监控大盘（窗口聚合指标 + Grafana + 异常检测规则）
- [ ] **VEH-06**: 压测脚本与故障演练（含 watermark 停滞）可执行，并记录 baseline 数字
- [ ] **VEH-07**: 交付架构文档、至少 1 篇 ADR、验证脚本与简历陈述页

### LOG · p01 日志 AI 平台

- [ ] **LOG-01**: 维护者可用独立 compose profile 一键启动 p01，端到端结构化日志流可复现
- [ ] **LOG-02**: 无 LLM/Milvus 时，流式富化/特征路径仍可完整演示
- [ ] **LOG-03**: 至少一条 AI 路径（ML_PREDICT / Agents / Milvus）可用，且文档给出显式降级核对清单
- [ ] **LOG-04**: 成本/护栏相关业务指标可在 Prometheus/日志中观察
- [ ] **LOG-05**: 交付压测、故障演练、架构文档、ADR、验证脚本与简历陈述页

### RECO · p02 实时推荐

- [ ] **RECO-01**: 维护者可用独立 compose profile 一键启动 p02，用户行为流可接入
- [ ] **RECO-02**: 在线特征（Keyed State 或 Redis）+ 召回/打分闭环，结果可在 Kafka/存储中观察
- [ ] **RECO-03**: 交付压测、故障演练、架构文档、ADR、验证脚本与简历陈述页

### PROD · P5 生产化

- [ ] **PROD-01**: benchmark 全矩阵可运行，并产出 baseline.md
- [ ] **PROD-02**: Flink Kubernetes Operator 1.15 可部署，OrbStack K8s 上完成可观察的 Blue/Green 演练
- [ ] **PROD-03**: CI/CD 与单一 GitOps 路径落地并可按文档复现
- [ ] **PROD-04**: best-practice 完整规范、interview ≥150 题、monitoring 看板 JSON 可导入

### QA · P6 总装

- [ ] **QA-01**: `scripts/qa_check.sh` 全绿（mvn 编译、compose config、断链、违禁词、案例计数）
- [ ] **QA-02**: 案例 ≥100、文档 ≥30k 行、README 终稿与 PHASES.md 状态一致

### ENG · 工程不变量（跨 Phase）

- [ ] **ENG-01**: 新增组件先写入根 README 版本矩阵与 pom 属性区再使用
- [ ] **ENG-02**: 文档/模块编号先在 docs/README.md 登记；项目文档遵守八段式或等价结构
- [ ] **ENG-03**: 合入内容均在 OrbStack arm64 实测；禁止 TODO/省略/略/自行实现/请参考官网
- [ ] **ENG-04**: 每个工作会话结束更新 CHANGELOG 未发布区与 PHASES 状态，并做约定式提交

## v2 Requirements

Deferred — 不在本里程碑 ROADMAP。

### Future

- **FUT-01**: 主线升级 Flink 2.3（待连接器/Agents/Operator 生态就绪）
- **FUT-02**: 商业动态 CEP / 完整多 Agent 生产网关
- **FUT-03**: 云多区域容灾与多租户控制面

## Out of Scope

| Feature | Reason |
|---------|--------|
| Flink 2.3 主线升级 | ADR-001：生态未齐 |
| StateFun 新选型 | 社区已停运 |
| 多租户计费/权限中台 | 超出学习工程目标 |
| 双 GitOps 工具并行深讲 | 稀释深度 |
| 云厂商托管作为唯一演示路径 | 破坏本地复现
| 真实车企/广告生产数据 | 合规与范围；用合成数据 |
| 重写 P0–P3 已交付教材/Demo | 本里程碑仅增量与回链 |

## Traceability

Which phases cover which requirements. Updated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| VEH-01 | Phase 1 | Complete |
| VEH-02 | Phase 1 | Complete |
| VEH-03 | Phase 2 | Pending |
| VEH-04 | Phase 2 | Pending |
| VEH-05 | Phase 3 | Pending |
| VEH-06 | Phase 3 | Pending |
| VEH-07 | Phase 3 | Pending |
| LOG-01 | Phase 4 | Pending |
| LOG-02 | Phase 4 | Pending |
| LOG-03 | Phase 4 | Pending |
| LOG-04 | Phase 4 | Pending |
| LOG-05 | Phase 4 | Pending |
| RECO-01 | Phase 5 | Pending |
| RECO-02 | Phase 5 | Pending |
| RECO-03 | Phase 5 | Pending |
| PROD-01 | Phase 6 | Pending |
| PROD-02 | Phase 6 | Pending |
| PROD-03 | Phase 6 | Pending |
| PROD-04 | Phase 6 | Pending |
| QA-01 | Phase 7 | Pending |
| QA-02 | Phase 7 | Pending |
| ENG-01 | Phase 7 | Pending |
| ENG-02 | Phase 7 | Pending |
| ENG-03 | Phase 7 | Pending |
| ENG-04 | Phase 7 | Pending |

**Coverage:**
- v1 requirements: 25 total
- Mapped to phases: 25
- Unmapped: 0 ✓

---
*Requirements defined: 2026-07-17*
*Last updated: 2026-07-17 after roadmap mapping*
