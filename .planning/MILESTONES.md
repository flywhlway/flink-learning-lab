# Milestones

## v1.0 P4-P6 Production (Shipped: 2026-07-19)

**Phases completed:** 7 phases, 32 plans, 82 tasks  
**Requirements:** 25/25 v1 satisfied  
**Audit:** tech_debt（无阻断）→ [v1.0-MILESTONE-AUDIT.md](milestones/v1.0-MILESTONE-AUDIT.md)  
**Known deferred items at close:** 3 (see STATE.md Deferred Items)

**Delivered (summary):**
1. p03 车联网：profile + CEP 告警 + Broadcast 三模式 + Grafana/压测/watermark + ADR/RESUME
2. p01 日志 AI：默认可降级规则路径 + Async Ollama + 护栏预算 + 全套纪律
3. p02 实时推荐：双通道特征 + Top-K + Redis 降级 + baseline/文档包
4. P5：benchmark 矩阵、Operator 1.15 Blue/Green、Argo GitOps、题库≥150、三块看板
5. P6：qa_check + eng_audit 双绿；mains≥100；撤销文档行数硬门

**Key accomplishments (from plans):**

- 独立 p03 Maven 骨架 + HarshThenFaultPatternTest（缺 within → RED）+ CH 权威 verify.sh（空库非 0）
- 独立 `profiles: ["p03"]` one-shot `p03-init` + `make up-p03`，幂等创建 vehicle topics 与 `flinklab.vehicle_alerts`，default `make up` 不受影响
- CEP within(30s)+TIMEOUT Handler 单测 GREEN，VehicleAlertJob 可 shade 打包，Makefile submit / submit-p03 固定 jar 与主类
- 造数 + CH 权威 verify 正负例在 OrbStack 转绿，八段式 README 与模块 15/CHANGELOG/PHASES 收尾，qa_check exit 0
- 四个 surefire RED 测试类锁定 Registry within / 两新模式 / Gate 决策契约，verify.sh 增加白名单 PATTERN_ID 骨架（生产代码留给后续 Wave）
- PatternIds/Registry 与 TRIPLE_HARSH/DTC_PAIR 工厂落地，within 门禁四测 GREEN；PATTERN-LIBRARY.md 三行五元组可评审（VEH-03）
- 非 keyed BroadcastProcessFunction 出口门控落地：控制消息模型 + version 单调 + 默认 HARSH_THEN_FAULT，PatternActivationGateTest 转绿（VEH-04 门控半段）。
- VehicleAlertJob 静态并行三 CEP + PatternActivationGate 出口门控可打包；AlertEvent/TIMEOUT/ClickHouse 贯通 pattern_id，p03-init 幂等创建 vehicle.pattern.control（VEH-04 作业半段）。
- 造数三 scenario + `--publish-control`、`make verify-switch` 在 OrbStack 上跑通默认 HARSH_THEN_FAULT 回归与 TRIPLE_HARSH 切换，README 交叉引用 PATTERN-LIBRARY，qa_check 绿
- 落地 VEH-05/06 失败态门禁与窗口 DDL/Grafana provisioning 骨架，建立 EventCountAgg RED→GREEN 与大盘/压测/演练反馈环，未实现窗口作业与完整面板。
- 旁路 VehicleWindowMetricsJob 写入 CH vehicle_window_metrics，Grafana 双 DS 大盘可 provisioning 且 verify_dashboard 在 OrbStack 上 exit 0；VehicleAlertJob CEP/Gate 未改。
- 扩展 gen --rate/--duration/--frozen-event-time，OrbStack 实测写出 docs/baseline.md（100 eps×120s），并以冻结 HEARTBEAT 完成 watermark 停滞→恢复 MATCH 演练。
- 交付 ARCHITECTURE / ADR-0001 / RESUME，并将 docs/README 15-03、CHANGELOG、PHASES 回填为 p03 P4 单项目完成态；qa_check.sh 绿。
- 独立 p01 Maven 骨架 + Parse/Rule/Budget RED 单测 + 四脚本失败态 + log_results DDL / p01-init / up-p01（不污染 default up）
- 独立 p01 启动面：ParseLogJson GREEN + JobConfig 默认 AI off + LogAiJob 透传可打包提交 + smoke_p01_profile 隔离断言
- 默认 AI off 下 Parse→FeatureEnricher→RuleTagger→ClickHouse 端到端可复现：`rule-auth-fail` 造数后 CH `rule_label=AUTH_FAIL` 且 `ai_source=DISABLED`，`make verify` exit 0
- Async Ollama `/api/chat` 风险分级旁路 + `verify-ai` 双轨验收 + DEGRADE-CHECKLIST：本机 `make verify-ai AI_MODEL=qwen3.5:9b-mlx` 绿，默认 `make verify` 仍零 Ollama 依赖
- 输出侧护栏 BLOCK、Async 前预算熔断、Flink Counter（p01）与 README :9249/:9090 观察命令齐备；BudgetGateTest GREEN，`make verify` 仍绿
- p01 达到与 p03 同等单项目完成态：OrbStack 实测 baseline（100 eps×90s，lag 8ms / ckpt 61ms / restarts 0）、AI 降级演练绿、ADR/ARCHITECTURE/RESUME/15-01 齐全、qa_check 全绿
- p02 独立模块 + jedis SSOT + Parse/RuleScorer RED 夹具 + verify/drill/loadtest 失败态骨架 + reco DDL/p02-init/up-p02
- ParseBehaviorJson GREEN + RealtimeRecoJob Kafka 透传 + gen feature-score + smoke_p02_profile 验证独立 profile
- 双通道在线特征（Keyed State + Redis Checkpointed Pipeline）+ PG catalog 规则 Top-K + Kafka/CH 双写，OrbStack make match 以 ClickHouse reco_results 放行
- Redis 降级演练 + 项目级 loadtest/baseline + ADR/ARCHITECTURE/RESUME/八段式 README + 15-02 回填，p02 达单项目完成态
- SSOT 登记 Operator/Argo chart、docs 13/14 骨架、Helm+OrbStack Ready 门禁，以及 PROD-01–04 四类可运行失败态 harness
- 在 OrbStack arm64 / compose Flink 上跑通 D-01 裁剪压测矩阵，产出仓库级 `benchmark/baseline.md`（九单元格实测 + 20k/ForSt SKIPPED），并完成 docs 模块 13
- Helm 安装 Flink Kubernetes Operator 1.15.0，并以 p03 VehicleAlertJob 的 FlinkBlueGreenDeployment 在 OrbStack 上跑通 ACTIVE_BLUE→ACTIVE_GREEN 可观察演练时间线
- 在 OrbStack 上 Helm 安装 Argo CD 10.1.4，Application 同步 p03 chart 至 Synced/Healthy，并交付 GitHub Actions 编译+qa_check 与可复现 gitops-cicd 清单
- 交付仓库级三块可打开 Grafana 看板、230 题完整答案题库、D-12 规范体系与 docs/PHASES/CHANGELOG 的 P5 完成态回填。
- 升级 `qa_check` 为六硬门并落地 `eng_audit`/`count_docs`；清洗「省略」误杀后本仓诚实红（mains=67、md≈9992）。
- 真实零依赖 Flink Demo 将 examples mains 从 67 提升到 100，并恢复全模块 `mvn -DskipTests compile` 绿。
- 将非 `.planning` Markdown 从 10688 行抬到 30620 行，以 docs 八段式、interview 答案加厚与 ai/best-practice/examples 互链完成 QA-02 文档轴（D-04/D-05/D-06）。
- 在 OrbStack arm64 上跑通 `qa_check`+`eng_audit` 双绿（mains=100、doc=30684），启用 ENG-04 严格 PHASES，并完成 README/PHASES/CHANGELOG 终稿；未打 git tag。

---
