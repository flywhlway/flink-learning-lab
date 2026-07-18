# flink-learning-lab

> **企业级 Apache Flink 全栈学习工程(2026 · AI 时代版)**
> 面向已有 Flink 生产经验的架构师,目标:成长为 Enterprise Streaming Architect,并打通 Flink × AI Agent 全链路。

**当前版本:`v0.1.0-phase0`(Phase 0 基线交付)** · 交付状态与后续阶段见 [PHASES.md](./PHASES.md)

---

## 1. 这个仓库是什么

- 一套可长期维护的企业内部 **Flink Learning Repository + 培训教材 + Demo 工程 + 最佳实践合集**
- 全部基于 **2026 年现行版本**(Flink 2.2.x LTS 生态、Flink Agents 0.3、Flink CDC 3.6、Kubernetes Operator 1.15)
- 全部案例在 **Apple Silicon(MacBook Pro M5 Pro 48GB + OrbStack + Docker Compose)** 上验证运行
- 重实践、轻理论:每个知识点 = 背景 + 架构图(Mermaid)+ 可运行代码 + 验证方式 + 踩坑 + 最佳实践

## 2. 版本矩阵(SSOT,全仓库唯一版本来源)

| 组件 | 版本 | 说明 |
|---|---|---|
| Apache Flink(核心) | **2.2.1** | 本仓库主线版本。选 2.2.1 而非 2.3.0 的原因见下方 ADR-001 |
| Apache Flink(前沿追踪) | 2.3.0(2026-06-25 发布) | 新特性在 `docs/00-landscape/` 中跟踪,连接器生态就绪后升级主线 |
| Flink Docker 镜像 | `flink:2.2.1-java21` | 官方多架构镜像,原生支持 arm64v8 |
| JDK | 21(本地)/ 21(集群) | 镜像即 java21 变体 |
| flink-connector-kafka | **5.0.0-2.2** | 官方声明兼容 Flink 2.1.x / 2.2.x |
| Flink CDC | 3.6.0（Maven `3.6.0-2.2`） | 兼容 Flink 2.2.x；连接器坐标带 `-2.2` 后缀 |
| Flink Agents | 0.3.0(Preview) | 官方 AI Agent 子项目,兼容 Flink 1.20/2.0/2.1/2.2 |
| Flink Kubernetes Operator | 1.15.0 | 兼容 Flink 2.2 |
| Flink Kubernetes Operator Helm chart | **1.15.0** | repo `https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/`；chart `flink-kubernetes-operator`；学习工程可 `--set webhook.create=false`，此时不强制 cert-manager |
| Helm CLI | **4.2.3**（Homebrew `helm`） | Operator / Argo 安装前置；禁止 PyPI 伪 `helm` 包 |
| Argo CD Helm chart | **10.1.4**（app `v3.4.5`） | repo `https://argoproj.github.io/argo-helm`；chart `argo/argo-cd`；单一 GitOps 路径（禁止并行 Flux）；禁止 PyPI 伪 `argocd` 包 |
| Kafka | `apache/kafka:3.9.1`(KRaft) | 无 ZooKeeper |
| ClickHouse | `clickhouse/clickhouse-server:24.8` | LTS |
| PostgreSQL | `postgres:16-alpine` | 已开启 `wal_level=logical`(为 CDC 篇预留) |
| Redis | `redis:7-alpine` | |
| jedis | **5.2.0** | Redis 客户端（p02 / e07 / e12）；禁止漂移到 lettuce / jedis 6.x |
| MinIO | `minio/minio:latest` + `minio/mc` | S3 兼容,Checkpoint / Lakehouse 存储 |
| Prometheus / Grafana | `v2.53.x` / `11.x` | Flink 指标已接通 |
| grafana-clickhouse-datasource | via `GF_INSTALL_PLUGINS` | Grafana 官方 ClickHouse 插件；compose grafana 服务安装，供 p03 业务大盘（D-01） |
| Kafka UI | `ghcr.io/kafbat/kafka-ui` | |
| Python | 3.13(工具脚本)/ 3.12(PyFlink,见 notebook/README) | 统一用 `uv` 管理 |
| Milvus(Phase 3,profile=ai) | `milvusdb/milvus:v2.6.19` | 向量检索底座;选 2.6 GA 而非 v3.0-beta,同 ADR-001"稳定优先"原则 |
| Ollama(Phase 3,宿主机原生) | 建议 0.9.0+ | 不进容器,`http://host.docker.internal:11434`;模型建议 qwen3:8b(48GB 内存量化推荐档位) |

**ADR-001:为什么主线锁定 Flink 2.2.1 而不是最新的 2.3.0?**
Flink 2.3.0 于 2026-06-25 发布,但截至本仓库基线日期:① 外部连接器(Kafka 等)尚未发布 2.3 兼容版本(官方连接器文档明确 "There is no connector yet available for Flink 2.3");② Flink Agents 0.3.0 官方仅提供 Flink 1.20/2.0/2.1/2.2 四个构建;③ Kubernetes Operator 1.15 声明兼容到 2.2。企业选型原则:**核心引擎版本必须以连接器与周边生态的兼容矩阵为准,而不是以最新号为准**。2.3 的新能力(changelog 转换算子、Materialized Table 增强、原生 S3 FileSystem 等)在 `docs/00-landscape/` 中单独跟踪。

## 3. 五分钟快速开始

```bash
# 0) 前置:OrbStack 已运行,JDK 21、Maven、uv 已就绪
git clone <your-repo>/flink-learning-lab && cd flink-learning-lab

# 1) 一键起环境(Kafka + Flink + CH + PG + Redis + MinIO + Prom + Grafana + Kafka UI)
cd docker && make up          # 首次会拉镜像,arm64 原生,无 Rosetta

# 2) 初始化(建 topic / bucket / ClickHouse 库,幂等可重复执行)
make init

# 3) 打开控制台
make urls
#   Flink Dashboard   http://localhost:8081
#   Kafka UI          http://localhost:8080
#   Grafana           http://localhost:3000  (admin / flinklab)
#   MinIO Console     http://localhost:9001  (flinklab / flinklab123)
#   ClickHouse HTTP   http://localhost:8123

# 4) 构建并提交第一个作业(事件时间窗口 + Kafka 端到端)
cd ../examples && mvn -q clean package
cd ../docker && make submit-e01

# 5) 造数据,观察窗口输出
uv run ../scripts/gen_events.py --topic clicks --eps 200
```

看到 Flink Dashboard 中 `e01-kafka-clickstream-window` 作业 RUNNING、Kafka UI 中 `clicks.agg` topic 持续产出聚合结果,即环境全链路打通。逐条排错见 [docker/README.md](./docker/README.md#故障排查)。

## 4. 仓库导航

| 目录 | 内容 | 当前状态 |
|---|---|---|
| [roadmap/](./roadmap/) | **Level 1→10 学习路线**(从 Flink 开发者到 Enterprise Streaming Architect) | ✅ 完整交付 |
| [docs/](./docs/) | 全部教材章节(SSOT 索引 + 编号体系 + 2026 技术版图) | ✅ 索引 + 版图交付,章节按 Phase 推进 |
| [docker/](./docker/) | 一键启动的本地企业级流平台 | ✅ 完整交付 |
| [examples/](./examples/) | 可运行 Demo 工程(Maven 多模块,目标 100+) | ✅ e01 交付(3 个作业) |
| [scripts/](./scripts/) | 数据生成器与运维脚本(uv 单文件脚本) | ✅ 交付 |
| [playground/](./playground/) | Flink SQL Client 交互实验场 | ✅ 首批 SQL 练习交付 |
| [ai/](./ai/) | **《Flink AI Engineering》**(本仓库核心) | ✅ 全书大纲 + 2026 现状简报交付 |
| [datasets/](./datasets/) | 数据集规范与生成方案 | ✅ 规范交付 |
| [benchmark/](./benchmark/) | 压测方法论与基准工程 | ✅ P5：裁剪矩阵 + OrbStack 实测 [`baseline.md`](./benchmark/baseline.md) |
| [monitoring/](./monitoring/) | 指标体系、Grafana 看板 JSON | ✅ P5：值班五指标 + 恰好三块可导入看板（platform/job/ai） |
| [production/](./production/) | K8s Operator、CI/CD、GitOps 生产化 | ✅ P5：Operator 1.15 Blue/Green + 单一 Argo CD + CI 可复现 |
| [templates/](./templates/) | 作业工程脚手架模板 | 📐 规范交付 |
| [cheatsheet/](./cheatsheet/) | CLI / SQL / 配置速查 | ✅ 交付 |
| [interview/](./interview/) | 面试题库(按 Level 分层) | ✅ P5：≥150 题 + 完整参考答案（`python3 scripts/count_interview.py`） |
| [best-practice/](./best-practice/) | 生产最佳实践规范体系 | ✅ P5：完整规范（架构/uid/CP/TTL/反压/日志/GitOps/AI 降级）与 `production/` 互链 |
| [notebook/](./notebook/) | PyFlink / Jupyter 环境 | ✅ 环境说明交付 |
| [PHASES.md](./PHASES.md) | 分阶段交付计划 + Claude Code / open-gsd 接力协议 | ✅ |
| [CHANGELOG.md](./CHANGELOG.md) | 版本记录 | ✅ |

## 5. 工程约定(全仓库统一)

1. **编号即引用**:文档 `docs/<模块号>-<模块名>/<章节号>-<章节名>.md`;示例 `examples/eNN-<主题>`;两者在 `docs/README.md` 的 SSOT 索引中一一映射。
2. **SSOT**:版本号只出现在本 README 的版本矩阵与 `examples/pom.xml` 属性区;文档引用版本一律写"见版本矩阵"。
3. **图**:架构图 / 流程图 / 时序图一律 Mermaid,UML 用 PlantUML,禁止贴图片截图。
4. **代码**:Java 21 优先(records、pattern matching、virtual threads 按场景),Python 作为数据工具与 PyFlink 补充;所有 Java 模块继承 `examples/pom.xml` 统一依赖与插件版本。
5. **每章八段式**:背景 → 架构 → 代码 → 启动命令 → 验证方式 → 踩坑 → 最佳实践 → 面试题+参考资料。
6. **可运行是底线**:任何进入仓库的命令与代码必须在 OrbStack/arm64 环境实际跑通后才允许合入(QA 脚本见 `scripts/`)。

## 6. 学习入口建议

- 想按体系学 → [roadmap/README.md](./roadmap/README.md) 从你的当前 Level 切入(有 Flink 生产经验者一般从 **Level 3** 开始)
- 想直接上手 → 上面的五分钟快速开始 + [examples/e01](./examples/e01-hello-flink/)
- 只关心 AI 方向 → [ai/README.md](./ai/README.md)(Streaming × Agent 的完整知识体系)
- 备战面试 → [interview/](./interview/) + [cheatsheet/](./cheatsheet/)

---

维护者:flywhl · License: Apache-2.0 · 基线日期:2026-07-04
