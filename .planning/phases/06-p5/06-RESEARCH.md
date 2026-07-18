# Phase 6: P5 生产化 - Research

**Researched:** 2026-07-18
**Domain:** Flink Kubernetes Operator Blue/Green · benchmark 矩阵 · Argo CD GitOps · Grafana/interview/best-practice
**Confidence:** HIGH（栈版本与 BG CR API 已对照官方 1.15 文档；本机 OrbStack K8s 可 Ready，但 Helm 未装、端到端 BG 尚未实测）

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### Benchmark 矩阵与驱动（PROD-01）
- **D-01:** 矩阵采用 **学习工程可跑通的裁剪全矩阵**（保留 README 轴语义，禁止未实测数字）：作业轴 = **e01-J2（窗口聚合）+ e10 CEP + p03 VehicleAlertJob**；负载轴 = **1k / 5k eps** 为必跑，**20k eps** 为 stretch（OrbStack 不稳定则在 baseline 标注 SKIPPED + 原因，不得虚构吞吐）；State Backend = **HashMap + RocksDB 增量**（ForSt 仅可选附录一行，不挡 Phase）；Checkpoint 主路径 = **对齐 + 30s**，另用 **1–2 行对照**覆盖非对齐或 10s（禁止全笛卡尔积爆炸）。
- **D-02:** 压测驱动锁定 **既有 Python 造数 / 项目 gen 脚本 + `benchmark/` 包装脚本 + Makefile 目标**；**不**引入未登记的 k6/JMeter 镜像。方法论继续服从 `benchmark/README.md` 单变量原则与报告模板；热身可缩短为学习工程级 **30–60s**，但必须在 `benchmark/baseline.md` 声明相对「理想 3 分钟」的偏差。
- **D-03:** 权威产物路径锁定 **`benchmark/baseline.md`**（仓库级）；数字仅 OrbStack arm64 实测。项目级 `projects/*/docs/baseline.md` 可交叉引用，**不**替代仓库级矩阵报告。

#### Operator Blue/Green（PROD-02）
- **D-04:** Operator 版本锁定 **Flink Kubernetes Operator 1.15.0**（根 README 版本矩阵已登记）；部署场锁定 **OrbStack 内置 Kubernetes**；安装路径优先 **Helm**（与 STACK / `production/README.md` 一致）。须先登记任何新增镜像/chart 坐标到版本矩阵。
- **D-05:** Blue/Green 演练主对象锁定 **p03 `VehicleAlertJob` 的 FlinkDeployment**（P4 最成熟、JAR/verify 纪律齐全）。Session 模式与 Autoscaler 可写「可选附录实验」，**不**作为 PROD-02 硬门禁——硬门禁是可观察的 Blue/Green 发布时间线。
- **D-06:** 可观察证据必须 **脚本化时间线**（推荐落 `production/docs/bluegreen-timeline.md` 或等价路径）：至少包含 `kubectl`/Operator 事件、`FlinkDeployment` 状态迁移、作业/savepoint 相关日志片段；禁止「仅截图散文、无命令可复现」。演练入口放在 `production/scripts/`（或 Makefile 目标）并由文档一键可跟，失败非 0。

#### CI/CD 与单一 GitOps（PROD-03）
- **D-07:** GitOps 工具锁定 **Argo CD**（`production/README.md` 已写 ArgoCD；STACK 拒绝双栈）。**禁止**并行深讲 Flux。路径形态：**Git 仓库 manifests（Helm/Kustomize 二选一，推荐 Helm chart + values）→ Argo CD sync → OrbStack K8s**。
- **D-08:** CI/CD MVP：**GitHub Actions** 跑编译/`scripts/qa_check.sh`（或子集门禁）+ 文档化的镜像/chart 发布步骤；本机演示以 arm64/OrbStack 为准。多架构 `buildx` 可写完整 SOP，但 **不以多架构镜像推送为 PROD-03 硬门禁**（避免学习工程被 registry 账号挡住）。
- **D-09:** `production/` 交付可复现清单：Operator 安装、示例 FlinkDeployment、Blue/Green SOP、Argo 应用清单、回滚步骤——全部可按文档在本机跟完，无「请参考官网」占位。

#### 规范 / 题库 / 看板（PROD-04）
- **D-10:** `monitoring/` 交付 **恰好 3 块可导入 Grafana dashboard JSON**（对齐既有规划）：**平台总览 / 作业深潜 / AI 专项（含 token/成本类面板或等价业务指标）**。须可在现有 Grafana provisioning 或文档一键导入路径下打开；**Loki / OTel Tracing 不作为 PROD-04 硬验收**（可在 docs/14 提及为可选增强，禁止为此新拉未登记可观测栈挡门禁）。
- **D-11:** `interview/` 扩至 **≥150 题**，保持 Level 分层（L1–L8+）；P5 起每题附 **完整参考答案或等价完整考点推导**（可链回教材章节，禁止只留空题干）。首批 30 题可保留并升级答案完整度。
- **D-12:** `best-practice/` 从首批 12 条扩展为 **完整规范体系**（至少覆盖：架构/目录命名、uid/savepoint、checkpoint/Kafka 事务、状态与 TTL、反压基线、日志/异常、CI-CD/GitOps 检查清单、AI 降级），与 `production/` **互链**——规范正文在 `best-practice/`，落地清单在 `production/`。
- **D-13:** `docs/README.md` 模块 **13（性能与压测）** 与 **14（生产化）** 必须登记并回填完成态表述；正文可落 `docs/` 对应目录或明确指向 `benchmark/`/`production/`/`monitoring/` 权威路径，禁止编号空洞。

#### 交付切片建议（供 planner 切 wave）
- **D-14:** 推荐执行顺序（可微调，但依赖方向锁定）：**(1)** SSOT/编号登记 + benchmark 矩阵脚本与 `baseline.md` → **(2)** Operator 安装 + p03 Blue/Green 演练证据 → **(3)** Argo CD + CI 路径文档化 → **(4)** interview ≥150 + best-practice 完整 + 三块 Grafana JSON。Benchmark 与文档扩容可与 Operator 并行，但 **Blue/Green 不得先于可用作业 JAR**。

### Claude's Discretion
- Helm chart 目录布局、Argo Application CR 命名、GitHub Actions workflow 文件名、dashboard 面板具体 PromQL/CH 查询、interview 题目在各级的精确配比、best-practice 章节拆分粒度、20k eps / ForSt / Autoscaler 附录是否实跑。
- 压测默认并行度/TM 数在 OrbStack 可稳定跑通前提下由 executor 实测填写；CONTEXT 不锁死具体吞吐数字。

### Deferred Ideas (OUT OF SCOPE)
- 完整 Loki 日志 + OTel Tracing 接入 —— 可作为 docs/14 可选增强或后续里程碑，不挡 PROD-04
- Flux 作为第二 GitOps 路径 —— 明确 Out of Scope
- 云多区域灾备 / 多租户计费中台 —— FUT / Out of Scope
- ForSt 全负载轴、20k eps 必跑、Autoscaler 生产级调参 —— 可选附录，非硬门禁
- P6 `qa_check` 终检与案例/行数计量 —— Phase 7
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| PROD-01 | benchmark 全矩阵可运行，并产出 baseline.md | 裁剪矩阵轴（D-01）+ 复用 p03/`scripts/gen_events.py` harness + 仓库级 `benchmark/baseline.md` 模板与 Makefile |
| PROD-02 | Operator 1.15 可部署，OrbStack K8s 上完成可观察 Blue/Green | Helm 装 Operator 1.15.0；用 `FlinkBlueGreenDeployment` 包裹 p03 VehicleAlertJob；脚本化时间线（D-06） |
| PROD-03 | CI/CD 与单一 GitOps 路径可复现 | Argo CD only + GitHub Actions（编译/qa_check）+ production/ SOP；无 Flux、无强制多架构推送 |
| PROD-04 | best-practice 完整、interview ≥150、monitoring 看板 JSON 可导入 | 扩展 12→规范体系；30→≥150 完整答案；恰好 3 块 Grafana JSON + provisioning/导入路径；docs 13/14 登记 |
</phase_requirements>

## Summary

Phase 6 把已交付的 P4 三项目从「compose 工程化」抬到「仓库级生产化层」：`benchmark/` 产出可复现矩阵报告，`production/` 在 OrbStack K8s 上用 Flink Kubernetes Operator **1.15.0** 跑通 **可观察** Blue/Green，再用 **唯一** Argo CD + GitHub Actions 路径把 manifests 接上，最后补齐 interview/best-practice/三块 Grafana 与 docs 模块 13/14。五个目录（`benchmark/`、`production/`、`monitoring/`、`best-practice/`、`interview/`）目前仅有 README 骨架，内容与脚本均待落地；可复用资产集中在 p03 loadtest/baseline、e01-J2、`scripts/gen_events.py`、Grafana provisioning 与 p03 业务看板 JSON。

关键实现要点：① 压测**禁止**笛卡尔积与 k6，按 D-01 裁剪轴写 harness；② Blue/Green 在 Operator 1.15 的权威 CR 是 **`FlinkBlueGreenDeployment`**（内部管理 Blue/Green 两个 `FlinkDeployment` 子资源）——这与 D-05「以 p03 VehicleAlertJob 为演练对象」兼容：作业仍是 VehicleAlertJob，BG 控制面用官方 CR；③ 本机已具备 `kubectl` + OrbStack K8s（研究时已 `orb start k8s` 至 Ready），但 **Helm 未安装**，Planner 必须把 `brew install helm`（或等价）放进 Wave 0；④ Argo CD 需要可拉取的 Git 源——学习工程可接受「推送到本仓库 remote + Application 指向 `production/`」或文档化的本地演示变通，但不可换成 Flux。

**Primary recommendation:** 按 D-14 四波交付——先 SSOT + benchmark harness/`baseline.md`，再 Helm 装 Operator 1.15 + `FlinkBlueGreenDeployment`(p03) + 时间线脚本，再 Argo Application + GHA CI，最后 interview≥150 / best-practice 体系 / 三块 Grafana JSON + docs 13/14。

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| 压测矩阵驱动与 baseline 报告 | API / Backend（本机脚本层） | Database / Storage（Prom/CH 读指标） | Harness 在宿主机编排作业+造数；指标来自 Prometheus/可选 CH |
| Flink 作业运行（compose 压测） | API / Backend（Flink JM/TM in Docker） | Database / Storage（Kafka/MinIO） | 延续现有 docker 基座，不破坏 `make up` |
| Operator / Blue-Green 控制面 | API / Backend（K8s + Operator） | CDN / Static（—） | CR 协调与状态机在集群内 |
| GitOps sync（Argo CD） | API / Backend（Argo in K8s） | Browser / Client（Argo UI 可选） | Desired state 来自 Git manifests |
| CI 门禁（GHA） | API / Backend（GitHub runners） | — | 编译 + `qa_check`；不替代 OrbStack 实测 |
| Grafana 三看板 | Browser / Client（Grafana UI） | Database / Storage（Prom/CH DS） | JSON 定义 + provisioning；查询在 DS |
| interview / best-practice / docs 13–14 | CDN / Static（仓库文档） | — | 纯内容资产，互链 production/benchmark |

## Project Constraints (from .cursor/rules/)

`.cursor/rules/` 不存在。约束以根 `CLAUDE.md` / PROJECT 不变量为准：版本 SSOT、docs 编号先登记、OrbStack arm64 实测、禁止 TODO/省略/请参考官网、会话收尾 CHANGELOG+PHASES、`qa_check.sh` Phase 门禁。

## Standard Stack

### Core

| Library / Tool | Version | Purpose | Why Standard |
|----------------|---------|---------|--------------|
| Apache Flink | 2.2.1 | 作业运行时 | README SSOT；ADR-001 禁升 2.3 `[VERIFIED: README.md 版本矩阵]` |
| Flink Kubernetes Operator | 1.15.0 | CRD + Blue/Green | 矩阵已登记；官方兼容 Flink 2.2；Helm chart 在 Apache 发行区 `[CITED: nightlies Operator 1.15 Quick Start / Helm]` |
| OrbStack K8s | 本机内置（研究时 kubelet v1.34.8+orb1） | BG/GitOps 演练场 | CONTEXT/STACK 锁定；`orb start k8s` `[VERIFIED: kubectl get nodes]` |
| Helm | 3.x/4.x（Homebrew `helm` formula，研究时 stable 4.2.3） | 安装 Operator / 作业 chart | 官方 Quick Start 前置；本机 **未安装** → Wave 0 `[VERIFIED: brew info helm]` |
| Argo CD | 稳定版（Helm `argo/argo-cd` 或官方 `install.yaml`） | 唯一 GitOps | D-07；STACK 拒 Flux `[CITED: argo-helm / Argo CD docs via secondary]` |
| JDK / Maven | 21 / 3.9.x | 编译作业 JAR | 已就绪 `[VERIFIED: java -version / mvn -v]` |
| Python + uv | 3.13 工具脚本 | 造数 / harness | 既有 `gen_events.py`、p03 gen `[VERIFIED: scripts/]` |
| Prometheus / Grafana | v2.53.x / 11.x | 压测指标与三看板 | 基座已接通 `[VERIFIED: monitoring/README + docker provisioning]` |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| cert-manager | v1.18.2（Operator 文档示例） | Operator webhook TLS | 仅当启用 webhook；学习工程可 `--set webhook.create=false` 跳过 `[CITED: Operator 1.15 Helm docs]` |
| `flink:2.2.1-java21` + 项目 JAR | SSOT 镜像 | Application Mode 镜像 | OrbStack：**本地 tag 非 `:latest` + `imagePullPolicy: IfNotPresent`** `[CITED: docs.orbstack.dev/kubernetes]` |
| GitHub Actions | `ubuntu-latest` arm64 或文档注明本机验证 | CI 编译/`qa_check` | PROD-03 MVP `[ASSUMED: GHA 可用；私有 runner 非必须]` |
| ingress-nginx / Traefik（可选） | OrbStack 文档示例 | BG Ingress 流量切换演示 | Operator BG 支持关 Ingress 管理；可只用状态机+事件时间线满足 D-06 `[CITED: OrbStack K8s + Operator BG docs]` |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Argo CD | Flux | **拒绝**（D-07 / Out of Scope） |
| Helm 装 Operator | 裸 YAML / kustomize only | 官方主路径是 Helm；裸装增加维护成本 |
| `FlinkBlueGreenDeployment` | 仅改 `job.upgradeMode` 的单 `FlinkDeployment` | upgradeMode 是常规升级，**不是** 1.15 BG 状态机；PROD-02 硬门禁用官方 BG CR |
| k6 / JMeter | Python gen + harness | **拒绝**（D-02） |
| 云托管 Flink | OrbStack K8s | **拒绝**（项目 Out of Scope） |
| archive.apache.org Helm repo | downloads.apache.org | 二者对 1.15.0 均 200；Quick Start 用 downloads；release 公告示例用 archive——执行时以能 `helm repo add` 成功者为准 `[VERIFIED: curl -sI 两者 200]` |

**Installation（Wave 0 前置，研究机实测缺口已标）：**

```bash
# 1) Helm（本机缺失）
brew install helm

# 2) OrbStack K8s（研究时已 Ready；若 NotReady / API unknown）
orb start k8s
kubectl config use-context orbstack
kubectl get nodes   # 期望 Ready

# 3) Operator 1.15.0（建议先关 webhook，免 cert-manager）
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
helm repo update
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace \
  --set webhook.create=false \
  --set image.tag=1.15.0

# 备选镜像仓库（ghcr 不通时）
# --set image.repository=apache/flink-kubernetes-operator --set image.tag=1.15.0

# 4) Argo CD（PROD-03；Helm 或官方 manifest 二选一，推荐 Helm 与 Operator 同路径）
helm repo add argo https://argoproj.github.io/argo-helm
helm install argocd argo/argo-cd --namespace argocd --create-namespace
```

**Version verification notes:**
- Operator chart / 发行物：`https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/` 与 `archive.apache.org/dist/...` 均存在 `flink-kubernetes-operator-1.15.0-helm.tgz`（2026-05-07）`[VERIFIED: WebSearch + curl HEAD]`
- `flinkVersion` 枚举含 `v2_2`（对齐主线 2.2.1）`[VERIFIED: Operator 1.15 CR reference HTML]`
- 不要用 PyPI 上的 `helm` / `argocd` 包代替 CLI（见 Package Legitimacy）

## Package Legitimacy Audit

> 本 Phase **不**通过 npm/PyPI 安装业务依赖。安装面是：Homebrew CLI、Helm charts、K8s manifests、可选 cert-manager YAML。对 PyPI 误查的同名包标记为 **REMOVED（错误生态）**。

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| helm (CLI) | Homebrew core | 多年 | 高 | github.com/helm/helm | N/A（非 PyPI） | **Approved** — `brew install helm` |
| flink-kubernetes-operator chart 1.15.0 | Apache dist | 2026-05 | N/A | apache/flink-kubernetes-operator | N/A | **Approved** — 官方发行 |
| argo-cd (Helm chart) | argoproj.github.io/argo-helm | 多年 | N/A | argoproj/argo-helm | N/A | **Approved** — 社区官方 chart |
| cert-manager（可选） | jetstack GitHub release | 多年 | N/A | cert-manager/cert-manager | N/A | **Approved optional** — 仅 webhook 开启时 |
| `helm` (PyPI) | PyPI | — | — | 无关项目 | [OK] 但生态错误 | **REMOVED** — 禁止 pip 装「Helm」 |
| `argocd` (PyPI) | PyPI | — | 低 | 无关 | [OK]/低下载 | **REMOVED** — 禁止 pip 装 |
| `kubernetes-cli` (PyPI) | PyPI | — | — | 不存在 | [SLOP] | **REMOVED** — kubectl 已由 OrbStack/系统提供 |

**Packages removed due to slopcheck [SLOP] verdict:** `kubernetes-cli`（及所有 PyPI 伪 CLI）
**Packages flagged as suspicious [SUS]:** none for real install path

*Planner：任何新增 Maven 坐标仍须先写根 README 版本矩阵 + pom 属性（ENG-01）；Operator/Argo/cert-manager 镜像与 chart 版本写入版本矩阵后再用。*

## Architecture Patterns

### System Architecture Diagram

```text
                    ┌─────────────────────────────────────────┐
                    │  宿主机 OrbStack arm64                   │
                    │  docker compose 基座（保持 make up）      │
                    │  Kafka · CH · PG · Redis · MinIO ·      │
                    │  Prometheus:9090 · Grafana:3000         │
                    └───────────────┬─────────────────────────┘
                                    │ host gateway / 已暴露端口
         ┌──────────────────────────┼──────────────────────────┐
         │                          │                          │
         ▼                          ▼                          ▼
┌─────────────────┐    ┌──────────────────────────┐   ┌─────────────────┐
│ benchmark/      │    │ OrbStack K8s             │   │ monitoring/     │
│ run-matrix.sh   │    │                          │   │ 3× Grafana JSON │
│ → baseline.md   │    │ Operator 1.15            │   │ → provisioning  │
│ 造数: gen_*.py  │    │  FlinkBlueGreenDeployment│   └─────────────────┘
│ 作业: e01-J2 /  │    │   ├─ FlinkDeployment blue│
│  e10 CEP / p03  │    │   └─ FlinkDeployment green│
│ (compose Flink  │    │  p03 VehicleAlertJob JAR │
│  或文档指定)    │    │  ← Argo CD Application   │
└─────────────────┘    │     sync from Git        │
                       └────────────▲─────────────┘
                                    │
                       ┌────────────┴─────────────┐
                       │ GitHub Actions CI         │
                       │ mvn + qa_check.sh         │
                       │ （文档化镜像/chart 发布）   │
                       └──────────────────────────┘

文档层: best-practice/ ↔ production/清单 · interview/≥150 · docs/13 · docs/14
```

### Recommended Project Structure

```text
benchmark/
├── README.md                 # 方法论（已有）→ 回填矩阵裁剪说明
├── baseline.md               # PROD-01 权威产物（新建）
├── scripts/
│   ├── run_matrix.sh         # 单变量轮次编排；失败非 0
│   └── collect_metrics.py    # PromQL 刮取 → 行写入 baseline
└── Makefile                  # make matrix / make baseline

production/
├── README.md                 # 可复现总入口（替换蓝图占位）
├── docs/
│   ├── operator-install.md
│   ├── bluegreen-sop.md
│   ├── bluegreen-timeline.md # D-06 证据（脚本生成或追加）
│   └── gitops-cicd.md
├── scripts/
│   ├── install-operator.sh
│   ├── run-bluegreen-drill.sh  # 失败非 0；产出时间线
│   └── verify-argocd-sync.sh
├── charts/p03-vehicle-alert/   # Discretion：Helm chart + values
│   └── templates/
│       └── flink-bluegreen.yaml  # kind: FlinkBlueGreenDeployment
└── argocd/
    └── application-p03.yaml

monitoring/
├── README.md
├── platform-overview.json
├── job-deepdive.json
└── ai-cost.json

best-practice/
├── README.md                 # 索引
└── *.md                      # 分章规范（Discretion 拆分）

interview/
├── README.md                 # 索引 + Level 导航
└── L{n}/...                  # ≥150 题+完整答案（Discretion 布局）

.github/workflows/
└── ci.yml                    # Discretion 文件名：编译 + qa_check

docs/
├── 13-performance/           # 或指向 benchmark/ 的完成态登记
└── 14-production/            # 或指向 production/ 的完成态登记
```

### Pattern 1: 裁剪压测矩阵（单变量 + 权威 baseline）

**What:** 按 D-01 轴跑有限组合；每轮只改一个旋钮；热身 30–60s 并在报告声明相对「理想 3 分钟」偏差；输出唯一 `benchmark/baseline.md`。  
**When to use:** PROD-01 全部轮次。  
**Example matrix cells（推荐最小必跑集）：**

| # | Job | eps | State | Checkpoint | 备注 |
|---|-----|-----|-------|------------|------|
| 1 | e01-J2 | 1k | HashMap | 对齐 30s | 基线 |
| 2 | e01-J2 | 5k | HashMap | 对齐 30s | 负载轴 |
| 3 | e01-J2 | 1k | RocksDB 增量 | 对齐 30s | backend 轴 |
| 4 | e10 CEP（建议 C5） | 1k | HashMap | 对齐 30s | 作业轴 |
| 5 | e10 CEP | 5k | RocksDB 增量 | 对齐 30s | 交叉 |
| 6 | p03 VehicleAlertJob | 1k | HashMap | 对齐 30s | 复用 p03 loadtest 口径 |
| 7 | p03 VehicleAlertJob | 5k | RocksDB 增量 | 对齐 30s | |
| 8–9 | 任一作业 | 1k | 同 backend | 非对齐 **或** 10s | 1–2 行对照 |
| stretch | 任一 | 20k | — | — | 不稳则 SKIPPED+原因 |

驱动：`scripts/gen_events.py --eps`（e01）、p03 `gen_vehicle_events.py --rate`、e10 需补齐或复用现有造数（Discretion：最小补丁脚本）。**禁止**在 baseline 填未跑数字。

### Pattern 2: FlinkBlueGreenDeployment（p03 VehicleAlertJob）

**What:** Operator 1.15 将 `kind: FlinkDeployment` 迁为 `kind: FlinkBlueGreenDeployment`，原 spec 放入 `template.spec`；状态机 `ACTIVE_BLUE` ↔ `TRANSITIONING_*` ↔ `ACTIVE_GREEN`，可选 SAVEPOINTING。  
**When to use:** PROD-02 硬门禁。  
**Example:**

```yaml
# Source: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/concepts/bluegreen-controller-flow/
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  name: p03-vehicle-alert-bg
spec:
  configuration:
    kubernetes.operator.bluegreen.abort.grace-period: "10 min"
    kubernetes.operator.bluegreen.reconciliation.reschedule-interval: "15s"
  template:
    spec:
      image: flinklab/p03-vehicle-alert:dev   # 非 :latest；OrbStack 本地可见
      imagePullPolicy: IfNotPresent
      flinkVersion: v2_2
      flinkConfiguration:
        execution.checkpointing.interval: "30s"
        # checkpoint/savepoint → MinIO/S3 或文档化的存储
      job:
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        upgradeMode: savepoint   # 与 BG savepointing 协同
        state: running
      jobManager:
        resource:
          memory: "1024m"
          cpu: 0.5
      taskManager:
        resource:
          memory: "2048m"
          cpu: 1
```

**与 D-05 对齐说明：** 演练对象仍是 p03 `VehicleAlertJob`；控制面 CR 使用官方 BG 类型；时间线脚本须同时采集 `FlinkBlueGreenDeployment.status` 与子 `FlinkDeployment` 事件（满足 D-06「FlinkDeployment 状态迁移」字面要求）。

### Pattern 3: 混合拓扑（Compose 中间件 + K8s Flink）

**What:** Kafka/CH/MinIO/Prom 留在 `docker compose`；Flink Application 跑在 OrbStack K8s；Job 参数 `--kafka-bootstrap` 指向宿主机可达地址（非 compose 服务名 `kafka:9092`）。  
**When to use:** PROD-02 默认路径（避免在 K8s 再部署整套中间件）。  
**How to avoid breakage:** Wave 0 用探针 Pod/`nc` 验证 K8s→宿主机 Kafka 端口；失败则文档化备用（hostNetwork / 额外 Service）。`[ASSUMED: OrbStack 共享引擎下 host.docker.internal 或网桥 IP 可达——执行期必须实测并写入 SOP]`

### Pattern 4: Argo CD Application → production manifests

**What:** `Application` 指向本仓库 `production/charts/...` 或 `production/manifests/`，destination 为 in-cluster；sync 后 `kubectl get flinkbluegreendeployment` 可见。  
**When to use:** PROD-03。  
**Pitfall:** Argo 需要 Git URL——私有仓库凭据与「仅本地未 push」会挡演示；SOP 须写清：remote + path + 首次 `argocd app sync` / UI，并提供失败非 0 的 `verify-argocd-sync.sh`。CI **不**要求自动 push 到 cluster（D-08：本机 OrbStack 为准）。

### Pattern 5: Grafana 三看板 provisioning

**What:** 恰好 3 个 JSON；扩展 `docker/config/grafana/provisioning/dashboards/` 增加 `monitoring/` 挂载 provider，或文档一键 `curl` 导入 API。版式参考 `projects/p03-vehicle-monitoring/monitoring/dashboards/p03-vehicle-overview.json`。  
**When to use:** PROD-04。  
**AI 专项：** 面板绑定 p01 业务指标（如 `budget_trips` / guardrail / AI 调用计数——以 `:9249` 实际暴露名为准，禁止臆造 PromQL 全名；可与 CH 表互补）。

### Anti-Patterns to Avoid

- **纸面 Blue/Green：** 只交 YAML 无时间线脚本 → 违反 PITFALLS #9 与 D-06
- **双 GitOps：** 同时写 Flux → Out of Scope
- **笛卡尔积压测：** 全轴全交叉 → 违反 D-01
- **baseline 填假数 / 沙箱豁免 ✅：** 违反 Core Value
- **破坏 default `make up`：** K8s 路径必须并行，不改 compose 硬依赖
- **`:latest` 本地镜像：** OrbStack 会反复 pull → 用 `:dev` + `IfNotPresent` `[CITED: OrbStack docs]`
- **PyPI 伪 Helm/Argo：** 安装错误工具
- **interview 只留题干：** 违反 D-11
- **docs 13/14 空编号：** 违反 D-13 / ENG-02

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Flink on K8s 生命周期 | 自写 Deployment+脚本模拟 BG | Operator `FlinkBlueGreenDeployment` | 官方状态机含 savepoint/abort/ingress |
| GitOps 控制器 | 自写 sync loop | Argo CD | D-07 锁定；生态成熟 |
| 压测协议引擎 | 自研 k6 替代品框架 | 既有 Python gen + Prom 刮取 | D-02；已有 p03 样板 |
| 证书给 webhook | 手写 secret 轮转 | cert-manager **或** 关 webhook | 官方两条路径 |
| 指标抓取 | 解析 Flink REST 自制时序库 | Prometheus（已接通） | 口径与值班五指标一致 |
| Dashboard 协议 | 手写前端 | Grafana JSON | D-10 |

**Key insight:** P5 的差异点是「可观察的真实演练 + 单一路径可复现」，不是再造平台组件。

## Common Pitfalls

### Pitfall 1: OrbStack K8s 未 Ready / Helm 缺失却开始写 CR
**What goes wrong:** `kubectl` API unknown；无法 `helm install`。  
**Why it happens:** 默认可能只开 Docker；Helm 未进 PATH。  
**How to avoid:** Wave 0：`orb start k8s` + `brew install helm` + `kubectl get nodes` 绿灯门禁。  
**Warning signs:** RESEARCH/执行日志出现 `InternalError` / `helm: MISSING`（本研究机曾出现，已通过 `orb start k8s` 恢复 Ready）。

### Pitfall 2: 把 upgradeMode 当成 Blue/Green
**What goes wrong:** 只改 `stateless|savepoint|last-state`，无 BG 状态机时间线。  
**Why it happens:** production README 同时提到三种升级模式与 BG。  
**How to avoid:** PROD-02 验收以 `FlinkBlueGreenDeployment` 状态迁移 + 脚本时间线为准；三种 upgradeMode 可作附录对照。  
**Warning signs:** 文档标题写 Blue/Green，证据只有单 Deployment Rolling。

### Pitfall 3: K8s Job 连不上 compose Kafka
**What goes wrong:** 作业一直重启 / source 无数据；用了 `kafka:9092`。  
**Why it happens:** compose DNS 不在 K8s。  
**How to avoid:** Job 参数显式宿主机 bootstrap；Wave 0 连通性探针；时间线脚本依赖「作业 RUNNING + 有消费」再触发切换。  
**Warning signs:** `CrashLoopBackOff`、Kafka UNKNOWN_TOPIC、零吞吐。

### Pitfall 4: 压测矩阵爆炸或假完成
**What goes wrong:** 轮次跑不完或抄项目 baseline 数字。  
**Why it happens:** 旧 `benchmark/README` 全轴诱惑；赶工。  
**How to avoid:** 严格执行 D-01 表；20k/ForSt 可 SKIPPED；权威文件仅 `benchmark/baseline.md`。  
**Warning signs:** baseline 无采集时间戳/环境快照；与 p03 `docs/baseline.md` 数字雷同却声称 5k。

### Pitfall 5: Argo 无 Git 源导致「GitOps 散文」
**What goes wrong:** 文档写 sync，实际只有 `kubectl apply`。  
**Why it happens:** 本地未 push / 无私有库凭据。  
**How to avoid:** SOP 写明 remote、Application YAML、sync 命令与成功判据；允许首次 bootstrap `kubectl apply -f application.yaml`，但后续变更须走 Git→sync 可观察。  
**Warning signs:** 无 `Application` CR；无 sync 状态截取进文档。

### Pitfall 6: Grafana JSON 不可导入 / 指标名臆造
**What goes wrong:** dashboard 红面板；AI 看板空白。  
**Why it happens:** datasource uid 不匹配；Prom 指标名猜测。  
**How to avoid:** 复用已有 datasource uid（如 p03 的 clickhouse uid 模式）；AI 面板先 `:9249` grep / Prom 实际 series 再写查询。  
**Warning signs:** provisioning 路径未挂载 `monitoring/*.json`。

### Pitfall 7: 会话过大导致主干半成品
**What goes wrong:** 单 commit 塞满四波。  
**Why it happens:** P5 范围宽。  
**How to avoid:** 严格 D-14 波次；每波可验证再合入。  
**Warning signs:** production 有 YAML 但无时间线；interview 未满 150 却标完成。

## Code Examples

### Helm 安装 Operator（关 webhook）

```bash
# Source: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/try-flink-kubernetes-operator/quick-start/
# Source: https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/operations/helm/
helm repo add flink-operator-repo https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --set webhook.create=false \
  --set image.tag=1.15.0
kubectl get pods -n flink-operator   # 或 default，视 values
kubectl get crd | grep flink
```

### Blue/Green 时间线采集骨架（D-06）

```bash
# 推荐落 production/scripts/run-bluegreen-drill.sh
set -euo pipefail
OUT="${1:-production/docs/bluegreen-timeline.md}"
{
  echo "# Blue/Green Timeline"
  echo "UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
  echo '## CR status'
  kubectl get flinkbluegreendeployment -A -o yaml
  echo
  echo '## Child FlinkDeployments'
  kubectl get flinkdeployment -A -o wide
  echo
  echo '## Events'
  kubectl get events -A --sort-by='.lastTimestamp' | tail -n 80
} | tee "$OUT"
# 触发 TRANSITION：改 template.spec.image tag 或 flinkConfiguration 中需 TRANSITION 的字段
# 轮询直至 status 达 ACTIVE_GREEN/ACTIVE_BLUE，超时 exit 1
```

### 仓库级矩阵入口（概念）

```bash
# benchmark/scripts/run_matrix.sh（新建）
# 伪代码：对 D-01 单元格循环
#   1) 配置 state backend / checkpoint（作业参数或 flink-conf）
#   2) 提交作业（compose flink run 或已有 make submit-*）
#   3) 热身 WARMUP_SEC=45
#   4) gen --eps|--rate 持续 DURATION
#   5) collect_metrics → append markdown 表行
# 全部单元格结束生成 benchmark/baseline.md 头+环境快照
```

### Argo Application（示意）

```yaml
# Source pattern: Operator Helm docs "Working with Argo CD" + Argo CD Application CR
apiVersion: argoproj.io/v1alpha1
kind: Application
metadata:
  name: flinklab-p03-bg
  namespace: argocd
spec:
  project: default
  source:
    repoURL: https://github.com/<org>/flink-learning-lab.git  # 执行时替换为真实 remote
    targetRevision: HEAD
    path: production/charts/p03-vehicle-alert
  destination:
    server: https://kubernetes.default.svc
    namespace: flink
  syncPolicy:
    syncOptions:
      - CreateNamespace=true
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| 手动 savepoint + 二次 deploy 冒充 BG | `FlinkBlueGreenDeployment` 状态机 | Operator 1.14+ 文档化；1.15 完整 docs | PROD-02 必须用 CR，不只是 SOP 散文 |
| 全矩阵 Nexmark 式笛卡尔积 | 学习工程裁剪矩阵（D-01） | 本 Phase CONTEXT | 可在单机 OrbStack 跑完 |
| 双 GitOps 对比教材 | 单一 Argo CD | STACK / D-07 | 深度优先 |
| interview 仅考点骨架 | ≥150 + 完整答案 | D-11 | 内容工作量大，宜独立 wave |

**Deprecated/outdated:**
- 将 `benchmark/README` 中的 e05 Join / 倾斜全轴当作 PROD-01 必跑——已被 CONTEXT 裁剪，README 需在执行时回填说明。
- 以 Flux 或云托管作为主演示路径——拒绝。

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | K8s Pod 可通过 `host.docker.internal` 或等价网桥访问 compose Kafka/MinIO | Pattern 3 | BG 作业无数据；需改网络方案或把依赖迁入 K8s |
| A2 | GitHub Actions 对本仓库可用（public 或已配 secrets） | PROD-03 | CI 改为文档化本地 `qa_check` 门禁仍可过 PROD-03，但需用户确认 |
| A3 | e10 CEP 矩阵作业采用 C5（或文档固定的单一主类）即可满足「e10 CEP」轴 | PROD-01 | 需在执行前固定主类与造数脚本 |
| A4 | 关闭 Operator webhook 可被验收接受（仍装 1.15 + BG CR） | PROD-02 | 若用户坚持 webhook，则必须装 cert-manager 并登记版本 |
| A5 | Argo 指向 GitHub remote 可接受（非本地 file://） | PROD-03 | 无私有库访问时需改用公开 fork 或文档化替代源 |
| A6 | AI 看板可用 p01 自定义指标 + 等价业务面板，不强制真实 token 计费后端 | PROD-04 D-10 | 面板可能偏「成本护栏」而非严格 token |

## Open Questions

1. **K8s↔Compose 网络权威地址是什么？**
   - What we know: OrbStack 共享容器引擎；compose 端口已暴露到 Mac。
   - What's unclear: 从 Pod 访问 Kafka 的稳定 DNS/IP。
   - Recommendation: Wave 0 探针任务实测并写入 `production/docs/operator-install.md`；锁定一种写法。

2. **Argo 的 Git remote 用用户哪条 URL？**
   - What we know: Application 需要 repoURL。
   - What's unclear: 本机默认 remote / 是否 private。
   - Recommendation: Discretion——SOP 用占位符 + `git remote get-url origin` 检测；执行时填实。

3. **压测跑在 compose Flink 还是 K8s Flink？**
   - What we know: D-01 未锁部署形态；D-02 复用现有 gen；PITFALLS 要求可复现。
   - What's unclear: 5k+RocksDB 在双 TM compose 上是否更稳。
   - Recommendation: **PROD-01 默认 compose Flink**（与现有 submit/loadtest 一致）；K8s 仅服务 PROD-02/03，避免矩阵与 BG 耦合失败。

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| OrbStack | 全部实测 | ✓ | 2.2.1 app | — |
| OrbStack K8s | PROD-02/03 | ✓（研究时 Ready） | v1.34.8+orb1 | `orb start k8s`；勿用 minikube 替主路径 |
| kubectl | PROD-02/03 | ✓ | v1.33.9 client | — |
| Helm | PROD-02/03 | ✗ | —（brew 有 formula 4.2.3） | Wave 0: `brew install helm`（**blocking**） |
| Docker / compose | PROD-01 基座 | ✓ | Docker 29.4 / orbstack ctx | — |
| JDK 21 | 编译 | ✓ | 21.0.2 | — |
| Maven | 编译 | ✓ | 3.9.14 | — |
| uv / Python | 造数 | ✓ | uv 0.10.10 | — |
| gh | CI/远程（可选） | ✓ | 2.95.0 | 文档化手动 workflow |
| cert-manager | webhook | ✗（未装） | — | `--set webhook.create=false` |
| Argo CD | PROD-03 | ✗（未装） | — | 执行期 Helm 安装 |
| k6 | — | N/A | — | **不使用**（D-02） |

**Missing dependencies with no fallback:**
- **Helm CLI** — 必须安装后才能按官方路径装 Operator（blocking Wave 0）

**Missing dependencies with fallback:**
- cert-manager → 关闭 webhook
- Argo CD → 执行期安装（非研究机预装要求）
- K8s 曾 API unknown → `orb start k8s` 已验证可恢复

## Validation Architecture

> `workflow.nyquist_validation: true` — 必须提供可自动化采样。

### Test Framework

| Property | Value |
|----------|-------|
| Framework | Bash harness + Maven Surefire（既有项目单测）+ `scripts/qa_check.sh` |
| Config file | 各 `projects/*/pom.xml` surefire；新建 `benchmark/Makefile`、`production/scripts/*.sh` |
| Quick run command | `bash scripts/qa_check.sh` 中违禁词+链接子集；或 `make -C benchmark dry-run`（Wave 0 实现） |
| Full suite command | `benchmark` 矩阵冒烟（低 eps）+ `production/scripts/run-bluegreen-drill.sh` + `qa_check.sh` + interview 计数脚本 |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PROD-01 | 矩阵脚本可跑并生成/更新 `benchmark/baseline.md` | smoke | `make -C benchmark matrix EPS=100`（低负载冒烟）后 `[ -s benchmark/baseline.md ]` | ❌ Wave 0 |
| PROD-01 | baseline 含环境快照且无占位假数 | lint | `rg -n 'TBD\\|TODO\\|FIXME' benchmark/baseline.md` 必须空 | ❌ Wave 0 |
| PROD-02 | Operator CRD 存在 | smoke | `kubectl get crd flinkbluegreendeployments.flink.apache.org` | ❌ Wave 0 |
| PROD-02 | BG 演练脚本失败非 0；产出时间线文件 | e2e | `bash production/scripts/run-bluegreen-drill.sh` | ❌ Wave 0 |
| PROD-03 | Argo Application Synced（或文档化判据） | smoke | `bash production/scripts/verify-argocd-sync.sh` | ❌ Wave 0 |
| PROD-03 | GHA workflow 存在且可解析 | lint | `test -f .github/workflows/ci.yml` + `actionlint` 可选 | ❌ Wave 0 |
| PROD-04 | 恰好 3 个 dashboard JSON | lint | `test $(ls monitoring/*.json \| wc -l) -eq 3` | ❌ Wave 0 |
| PROD-04 | interview ≥150 | lint | `python3 scripts/count_interview.py` 或 `rg` 计数 | ❌ Wave 0 |
| PROD-04 | docs 13/14 非空洞 | lint | `rg '模块 13\\|模块 14' docs/README.md` 含完成态链接 | 📋 编号在、正文空 → 执行期填 |
| ENG-03 子集 | 无违禁词 | lint | `bash scripts/qa_check.sh` | ✅ |

### Sampling Rate
- **Per task commit:** 相关脚本 `--help`/dry-run + 违禁词扫描
- **Per wave merge:** 该波验收命令（上表）全绿
- **Phase gate:** `qa_check.sh` + BG 时间线文件存在且含事件片段 + `benchmark/baseline.md` 有实测表 + interview 计数 ≥150 + 3 JSON

### Wave 0 Gaps
- [ ] `benchmark/scripts/run_matrix.sh` + `Makefile` + 空壳/冒烟 `baseline.md` 夹具
- [ ] `production/scripts/run-bluegreen-drill.sh`（先 RED：集群无 Operator 时非 0）
- [ ] `production/scripts/verify-argocd-sync.sh` 骨架
- [ ] `scripts/count_interview.py`（或 Makefile 目标）统计题量
- [ ] Grafana JSON 占位 3 文件（可先 invalid → 导入校验 RED）或 schema 检查脚本
- [ ] Wave 0 环境门禁：`command -v helm` && `kubectl get nodes`
- [ ] Framework：无需新测试框架；复用 bash + 既有 Surefire

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | yes（Argo UI / Grafana） | 默认密码仅本机；文档禁止提交真实 secret；Argo initial admin 从 K8s secret 读取 |
| V3 Session Management | partial | 本机演示；不建多租户 SSO |
| V4 Access Control | yes | Operator/Argo RBAC 用 chart 默认 SA；`watchNamespaces` 可选收窄 |
| V5 Input Validation | yes | Job 参数/控制消息沿用项目白名单；dashboard JSON 来自本仓 |
| V6 Cryptography | no（不新做加密） | TLS 交给 cert-manager（若启用 webhook）或关闭 webhook；禁止手写证书轮转逻辑 |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| 供应链假 CLI（PyPI helm） | Spoofing | 仅 Homebrew/官方 chart；Package Legitimacy |
| GitOps 误 sync 毁集群 | Tampering | Application 限定 namespace；手动 sync 默认；演练 ns 隔离 |
| CI 泄露凭证 | Information Disclosure | GHA secrets；矩阵/文档不含密码；沿用 compose 演示口令仅本机 |
| 恶意 dashboard/CR YAML | Tampering | 仅本仓评审；qa_check 断链；不对外部 URL 盲目 apply |
| 作业连错 bootstrap 导致写生产 | Elevation | 参数外置；values 分 env；学习工程固定本地端口 |

## Sources

### Primary (HIGH confidence)
- [Apache Flink K8s Operator 1.15 Blue/Green Controller Flow](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/concepts/bluegreen-controller-flow/) — CR 迁移步骤与状态机
- [Operator 1.15 Quick Start](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/try-flink-kubernetes-operator/quick-start/) — Helm repo、webhook/cert-manager
- [Operator 1.15 Helm](https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-release-1.15/docs/operations/helm/) — values、`webhook.create=false`、Argo 示例
- [OrbStack Kubernetes docs](https://docs.orbstack.dev/kubernetes/) — `orb start k8s`、本地镜像、LoadBalancer
- 仓库 SSOT：`README.md` 版本矩阵、`.planning/phases/06-p5/06-CONTEXT.md`、`benchmark|production|monitoring|best-practice|interview/README.md`
- 本机探针：`kubectl get nodes`、`brew info helm`、`command -v` 工具链、`curl -sI` Apache Helm 目录

### Secondary (MEDIUM confidence)
- [Operator 1.15.0 Release Announcement](https://flink.apache.org/2026/05/26/apache-flink-kubernetes-operator-1.15.0-release-announcement/) — archive Helm repo 示例
- Argo CD Helm 安装社区指南（argo-helm）— 与官方 Application CR 模型交叉验证
- `.planning/research/{STACK,ARCHITECTURE,PITFALLS,FEATURES}.md` — 里程碑级约束

### Tertiary (LOW confidence)
- 第三方 OrbStack+Helm 博文（仅作启用步骤旁证；以 OrbStack 官方文档为准）

## Metadata

**Confidence breakdown:**
- Standard stack: **HIGH** — 版本与安装坐标来自 README + Operator 官方 docs；Helm 缺失为环境事实
- Architecture: **HIGH** — BG CR 与混合拓扑模式清晰；网络细节 MEDIUM（A1）
- Pitfalls: **HIGH** — 对齐 PITFALLS #9 与本机已踩坑（K8s API / Helm）

**Research date:** 2026-07-18  
**Valid until:** 2026-08-18（Operator/Argo 小版本快移时提前重验 Helm repo URL）
