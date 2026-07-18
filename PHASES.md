# PHASES.md · 分阶段交付计划与接力协议

> 本仓库体量(≥30k 行文档、100+ Demo、3 个生产级项目)必须分阶段构建。本文件是阶段划分、验收口径与 **Claude Code + open-gsd 接力协议** 的 SSOT。
> 版本规则:每完成一个 Phase,打 tag `v0.<phase>.0`,CHANGELOG 记录。

## 阶段总览

| Phase | 交付物 | 对应原始需求 | 验收口径 | 状态 |
|---|---|---|---|---|
| **P0 基线** | 目录骨架、README、Level1-10 路线、docker 一键环境、e01×3 作业、SSOT 索引、2026 版图、AI 全书大纲、速查/题库/军规首批、本协议 | 第 1/4 部分全量;第 2/3/6 部分的框架与调研 | `make up && make init` 全绿;e01 三作业可运行;全部目录有实质内容 | ✅ 本次交付(v0.1.0) |
| **P1 内核** | docs 模块 01–04 全文;e02(窗口×5)、e03(状态×10)、e04(checkpoint×4)、common 基座;qa_check.sh | 第 2 部分(Runtime/时间/状态/容错)+ 第 5 部分对应配额 | 19 个新 Demo 全部可运行;每章八段式齐全 | ✅ v0.2.0(编译验证须在本机执行,沙箱离线) |
| **P2 SQL 与集成** | docs 05–10;e05(SQL×10)、e06(Table×8)、e07(连接器×8)、e08(CDC×4)、e09(Paimon/Iceberg×5 SQL)、e10(CEP×5)、e11(AsyncIO×3);playground P04-20;templates(job-datastream/job-sql) | 第 2 部分余量 + 第 3 部分 Lakehouse/CDC | SQL/Table/CEP 案例配额达标;PG→Paimon 整库同步演示可复现 | ✅ v0.3.0(编译验证须在本机执行,沙箱离线;e08/e09 集群步骤已给出完整命令,未在沙箱内跑通) |
| **P3 AI 专书** | ai/ 24 章全文 + e12 系列 Demo ×11(7 个零依赖进主构建 + 2 个 SQL 脚本 + 2 个 Agents standalone);Milvus ai-profile 进 compose;docs 11 生态协同 | 第 6 部分(重点)+ 第 3 部分 AI 能力 | 24 章成文;每章降级路径明示;零依赖 Demo 本地可跑 | ✅ v0.4.0(沙箱限制:Agents standalone 依赖未编译验证、Ollama/Milvus 端到端留本机,各 README 已给核对清单与降级路径) |
| **P4 三大项目** | projects/p01 日志 AI 平台、p02 实时推荐、p03 车联网监控(各含完整工程、压测、故障演练、简历陈述) | 第 7 部分 | 每个项目独立 compose profile 一键起;有架构文档+ADR+验证脚本 | ✅ **三大项目单项目完成态均已交付**：p03（VEH-01–07）；p01（LOG-01–05）；**p02（RECO-01–03：compose profile `p02` + Keyed State/Redis 双通道 + 规则 Top-K + `make loadtest`/`make drill-redis` + ARCHITECTURE/ADR/RESUME；OrbStack 实测）**；P4 收官后进入 P5 生产化 |
| **P5 生产化** | benchmark 全矩阵与 baseline.md;production(Operator/CI-CD/GitOps)落地;best-practice 完整规范;interview 扩至 150+;monitoring 看板 JSON | 第 8/9 部分 | 压测报告可复现;OrbStack K8s 上完成 Blue/Green 演练;三块 Grafana 可打开;interview≥150;规范与 production 互链 | ✅ **可验证完成态**（PROD-01–04）：`benchmark/baseline.md`；Operator Blue/Green 时间线；Argo/CI 文档与脚本；`monitoring/` 三 JSON + provisioning；`interview` 门禁绿；`best-practice/` 体系；docs 模块 13/14 完成态 |
| **P6 总装 QA** | 全仓交叉引用校验、违禁词扫描(内容禁令词表)、行数与案例数盘点、README 终稿 | 第 10 部分 + 最终交付要求 | ✅ **可验证完成态**（QA-01/02 + ENG-01…04）：`bash scripts/qa_check.sh` 全绿；mains≥100；md≥30000；`bash scripts/eng_audit.sh` ENG-01…04；扫描匹配整词内容禁令词表、不裸匹配「略」（D-08）；git tag 留给 `/gsd-complete-milestone`（D-12） |

## 接力协议(Claude Code + open-gsd)

延续 spring-ai-alibaba-learning 的成熟工作流:

1. **入口指令**:`请阅读 flink-learning-lab/PHASES.md 与 docs/README.md,继续执行 Phase <N>,遵守全部工程约定(根 README 第 5 节)。`
2. **不变量(任何 Phase 不得违反)**
   - 版本 SSOT 在根 README 版本矩阵 + examples/pom.xml 属性区;新增组件先登记再使用
   - 文档/模块编号先在 docs/README.md 登记;八段式结构强制
   - 一切代码/命令必须在 OrbStack arm64 实测通过;不可验证的内容不合入
   - 禁止出现:TODO、省略、略、自行实现、请参考官网
   - 每个工作会话结束:更新 CHANGELOG 未发布区 + 本表状态列,git commit(约定式提交:`feat(p1): ...`)
3. **会话粒度建议**:一个会话 ≤ 一个模块(如"e03 状态×10");先写教材章节,再写 Demo,再回填交叉引用。
4. **中断恢复**:任何时刻仓库都应处于"可构建、可 make up"状态;进行中的半成品放 `wip/` 分支,主干只收完成态。
5. **验收自查**:每个 Phase 结束跑 `scripts/qa_check.sh`(P1 交付,含:mvn 全模块编译、compose config 校验、Markdown 断链扫描、违禁词扫描、案例计数)。

## 原始需求 → 阶段映射备忘

原始十部分需求(见任务书)全部映射如上;其中"第 3 部分 2025-2026 新能力调研"已在 P0 以 docs/00-landscape 落地并将随每个 Phase 滚动更新;"所有引用注明来源"在 00-landscape 与各章参考资料节执行。
