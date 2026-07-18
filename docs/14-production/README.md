# 模块 14 · 生产化

> 背景:在 OrbStack 内置 Kubernetes 上用 Flink Kubernetes Operator 复刻企业发布流（Blue/Green、单一 Argo CD GitOps、CI 门禁），与 compose 演示面并行、互不替代。
> 权威路径:[production/](../../production/)（蓝图、Helm/manifests、演练与校验脚本）
> 状态:🚧 P5 进行中（Wave 0 已登记编号与环境门禁；Operator/Argo 落地在后续 wave）

## 本章入口

- 生产化蓝图 → [production/README.md](../../production/README.md)
- 环境门禁 → `production/scripts/check_env.sh`（Helm + kubectl Ready）
- Blue/Green 演练入口 → `production/scripts/run-bluegreen-drill.sh`
- Argo sync 校验 → `production/scripts/verify-argocd-sync.sh`
- 规范正文互链 → [best-practice/](../../best-practice/)；看板 JSON → [monitoring/](../../monitoring/)

## 硬约束

- GitOps 仅 Argo CD；禁止并行深讲 Flux
- 新增 chart/镜像坐标须先登记根 README 版本矩阵
- 关闭 Operator webhook 时不强制安装 cert-manager
