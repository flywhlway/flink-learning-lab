# production/ · 生产化蓝图(Phase 5 落地)

目标形态:OrbStack 内置 Kubernetes 上运行 Flink Kubernetes Operator **1.15**,复刻企业发布流。

## 范围

1. **Operator 实操**:FlinkDeployment/FlinkSessionJob CRD;三种升级模式(stateless/savepoint/last-state)对照实验;1.14+ 的 Blue/Green 发布演练;Autoscaler 弹性实验。
2. **HA 与状态治理**:K8s ConfigMap HA、savepoint 留存策略、uid 纪律的流水线强制检查。
3. **CI/CD & GitOps**:镜像流水线(多架构 buildx)→ Helm chart → ArgoCD 同步 → 金丝雀/回滚 SOP。
4. **规范文档**:架构/目录/命名/异常/日志/配置中心规范(与 best-practice/ 互链,规范正文落在那边,这里放落地清单)。

## 为什么放到 Phase 5

Operator 层的一切都建立在"作业本身已工程化"(uid、savepoint、指标、压测基线)之上 —— 顺序颠倒会把平台工程做成空中楼阁。当前请先用 docker compose 形态完成 L1–L6。
