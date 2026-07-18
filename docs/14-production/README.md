# 模块 14 · 生产化

> 背景：在 OrbStack 内置 Kubernetes 上用 Flink Kubernetes Operator 复刻企业发布流（Blue/Green、单一 Argo CD GitOps、CI 门禁），与 compose 演示面并行、互不替代。
> 权威路径：[`production/`](../../production/)（Helm/manifests、演练与校验脚本）· [`monitoring/`](../../monitoring/)（仓库级三块 Grafana JSON）· [`best-practice/`](../../best-practice/)（规范正文）
> 状态：✅ 完成（PROD-02/03/04：Operator Blue/Green 可观察 + Argo/CI 可复现 + 规范/题库/看板收官）

## 本章入口

| 主题 | 路径 |
|---|---|
| 生产化总入口 | [`production/README.md`](../../production/README.md) |
| 环境门禁 | `production/scripts/check_env.sh`（Helm + kubectl Ready） |
| Operator 安装 | [`production/docs/operator-install.md`](../../production/docs/operator-install.md) |
| Blue/Green SOP + 时间线 | [`production/docs/bluegreen-sop.md`](../../production/docs/bluegreen-sop.md) · [`bluegreen-timeline.md`](../../production/docs/bluegreen-timeline.md) |
| GitOps + CI 勾选表 | [`production/docs/gitops-cicd.md`](../../production/docs/gitops-cicd.md) |
| 规范体系（uid/CP/TTL/反压/GitOps/AI 降级…） | [`best-practice/README.md`](../../best-practice/README.md) |
| 仓库级看板（恰好 3 JSON） | [`monitoring/README.md`](../../monitoring/README.md) |
| 面试题库 ≥150 | [`interview/README.md`](../../interview/README.md) |
| 压测基线（模块 13） | [`benchmark/baseline.md`](../../benchmark/baseline.md) |

## 章节地图

- **14-01** K8s Operator 1.15：CRD / 升级模式 / Blue-Green（硬门禁=状态机时间线）/ Autoscaler 附录
- **14-02** 可观测：Prometheus + Grafana 三块看板；**Loki / OTel 为可选增强，非 PROD-04 硬门禁**
- **14-03** CI/CD 与 GitOps：Helm + **唯一** Argo CD + GitHub Actions（禁止并行 Flux）
- **14-04** 多租户与成本治理：作业隔离倾向 Application Mode；AI 成本用 p01 Counter + `ai-cost` 看板代理
- **14-05** 安全与合规视角：密钥不入库、Guardrail/脱敏；车企 UN R155 等延伸阅读见专书，不挡本模块门禁

## 硬约束

- GitOps 仅 Argo CD；禁止并行深讲 Flux
- 新增 chart/镜像坐标须先登记根 README 版本矩阵
- 关闭 Operator webhook 时不强制安装 cert-manager
- 一切演练命令须在 OrbStack arm64 可观察；不可验证不合入
