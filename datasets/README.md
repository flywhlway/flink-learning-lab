# datasets/ · 数据集规范

原则:**仓库不入库大文件,一切数据可再生**。数据 = 生成器脚本 + 固定随机种子 + 本 README 的 schema 契约。

| 数据集 | Schema(JSON 字段) | 生成器 | 用途 |
|---|---|---|---|
| clicks(点击流) | userId:string, page:string, ts:epoch_ms | scripts/gen_events.py | e01/e02/e05/案例二 |
| app_logs(应用日志) | ts, level, service, traceId, msg, stack? | scripts/gen_logs.py(P4) | 案例一 |
| vehicle_gps / vehicle_can / vehicle_dtc | vin, ts, lat/lon/speed …(详表 P4 定稿) | scripts/gen_vehicles.py(P4) | 案例三 |
| orders(交易) | orderId, userId, amount, status, ts | P2 交付 | e05 Join/CDC 实验 |

约定:所有生成器支持 `--seed` 保证可复现;事件时间一律毫秒;脏数据注入统一用 `--dirty-ratio` 参数(为死信/旁路实验服务)。大体量样本(如日志回放素材)生成到 MinIO `datasets` bucket,不进 git。
