# benchmark/ · 压测方法论与基准工程（P5 / PROD-01）

## 方法论（先于工具确立）

1. **单变量原则**：每轮只动一个旋钮（并行度/内存/state backend/序列化），其余冻结。
2. **口径固定**：吞吐 = 配置 produce eps（墙钟对照）；延迟 = `currentEmitEventTimeLag`；checkpoint = `lastCheckpointDuration`；反压/busy/restarts 取值班五指标（见 `monitoring/README.md`）。
3. **热身丢弃**：学习工程默认 **30–60s**（`WARMUP_SEC`）不计入；须在 `baseline.md` 声明相对方法论「理想 3 分钟」的偏差。
4. **报告模板**：环境快照 → 负载定义 → 矩阵指标表 → 结论与复跑命令；**禁止未实测数字**。

## 裁剪矩阵轴（D-01，非全笛卡尔积）

| 轴 | 必跑取值 | 说明 |
|---|---|---|
| 作业 | **e01-J2**（`KafkaClickstreamWindowJob`）/ **e10 CEP**（固定 `C5VehicleDtcPatternJob`）/ **p03 `VehicleAlertJob`** | 不再把 e05 Join、倾斜全轴列为必跑 |
| 负载 | **1k / 5k eps** 必跑；**20k** 为 stretch | 不稳则 `SKIPPED` + 原因，禁止假数 |
| State Backend | **HashMap** + **RocksDB 增量** | ForSt 仅可选附录 |
| Checkpoint | 主路径 **对齐 + 30s**；另 **1–2 行**对照（非对齐或 10s） | 禁止全组合爆炸 |
| 部署 | **compose Flink**（`docker/make up`） | PROD-01 **不**依赖 K8s Operator |
| 驱动 | `scripts/gen_events.py` / p03 `gen_vehicle_events.py` / e10 作业内 `Labs.events --eps` | **禁止 未登记 HTTP 压测工具** |

权威产物：**[`baseline.md`](./baseline.md)**（仓库级）。`projects/*/docs/baseline.md` 可交叉引用，**不**替代本报告（D-03）。

## 一键入口

```bash
# 基座 + p03 topic/DDL
cd docker && make up && make up-p03

# 打包作业（若尚未 package）
cd ../examples && mvn -q -pl e01-hello-flink,e10-cep -am package -DskipTests
cd ../projects/p03-vehicle-monitoring && mvn -q package -DskipTests

# 低负载冒烟（单单元格；写入 baseline.dry-run.md，不覆盖权威报告）
make -C benchmark dry-run

# 全量必跑矩阵 → 写入权威 baseline.md
make -C benchmark matrix
```

环境变量：`WARMUP_SEC`（30–60）、`DURATION_SEC`、`CELL_LIMIT`、`PROM_URL`、`FLINK_URL`、`BOOTSTRAP`。

## 脚本

| 路径 | 作用 |
|---|---|
| `scripts/run_matrix.sh` | 裁剪矩阵编排：提交 → 热身 → 计量 → 刮取 → 写 `baseline.md` |
| `scripts/collect_metrics.py` | Prometheus 值班五指标 → `key=value` |
| `Makefile` | `matrix` / `dry-run` / `baseline` |

参照系：Nexmark 等方法论对齐见教材模块 13；本机（OrbStack arm64）实测数字只写在 `baseline.md`。
