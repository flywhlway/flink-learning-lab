# 01 · 架构 / 目录 / 命名

## 规则

1. **仓库分层固定：** `docs/`（教材）· `examples/`（Demo）· `projects/`（生产级）· `production/`（K8s/GitOps）· `benchmark/` · `monitoring/` · `best-practice/` · `interview/`。新能力先登记 [`docs/README.md`](../docs/README.md) 编号再落文件。
2. **作业主类 / jar / Flink `job_name` / Helm release 命名可追溯：** 使用 `p0N-…` / `eNN-…` 前缀，禁止 `test`/`demo1` 进生产清单。
3. **配置外置：** bootstrap、group-id、checkpoint 路径、AI endpoint 一律参数或 ENV；同一 jar 三环境可跑（总则 #7）。
4. **版本 SSOT：** 组件版本只写根 [`README.md`](../README.md) 矩阵与 `examples/pom.xml`；文档写「见版本矩阵」。

## 理由

学习工程与生产演练共用同一套命名时，Blue/Green、Argo Application、Grafana `job_name` 过滤才能对得上；配置进 jar 会导致「改环境必须重打包」。

## 反例

- 在 `VehicleAlertJob` 里写死 `localhost:9092`，K8s 演练全部连错 listener。
- 新建 `docs/99-misc/` 却未在 `docs/README.md` 登记 → 断链与编号漂移。
- chart 名与镜像 tag 无项目前缀，Argo 多 Application 时无法辨认。

## 落地互链

- 生产目录与脚本：[`production/README.md`](../production/README.md)
- GitOps 路径约定：[`production/docs/gitops-cicd.md`](../production/docs/gitops-cicd.md)
