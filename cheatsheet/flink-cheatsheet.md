# Flink 速查表(2.2 主线 · 本仓库 docker 环境适配版)

## CLI(容器内 `docker compose exec jobmanager …`)

```bash
flink run -d -c <MainClass> /opt/flink/usrlib/<job>.jar   # 提交(detached)
flink list                                                # 运行中作业
flink cancel <jobId>                                      # 取消(不留 savepoint)
flink stop --savepointPath s3://flink/savepoints <jobId>  # 优雅停止 + savepoint
flink savepoint <jobId> s3://flink/savepoints             # 在线触发 savepoint
flink run -d -s s3://flink/savepoints/savepoint-xxxx \
      -c <MainClass> /opt/flink/usrlib/<job>.jar          # 从 savepoint 恢复
flink run --allowNonRestoredState ...                     # 拓扑删算子后的恢复
```

## REST(http://localhost:8081)

```
GET /jobs/overview                      作业列表
GET /jobs/<id>/checkpoints              checkpoint 统计
GET /jobs/<id>/vertices/<vid>/backpressure   反压采样
POST /jobs/<id>/savepoints              触发 savepoint
```

## 高频配置键(Flink 2.x 键名)

```yaml
taskmanager.numberOfTaskSlots: 4
taskmanager.memory.process.size: 2048m
execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.unaligned.enabled: true      # 反压下保 checkpoint
state.backend.type: rocksdb                           # hashmap | rocksdb | forst
execution.checkpointing.incremental: true
execution.checkpointing.dir: s3://flink/checkpoints
restart-strategy.type: exponential-delay
pipeline.operator-chaining.enabled: true
table.exec.mini-batch.enabled: true                   # SQL 微批
table.exec.state.ttl: 1 d                             # SQL 状态 TTL
```

## SQL 速记

```sql
-- 窗口 TVF
TUMBLE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
HOP(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '5' MINUTE)
CUMULATE(TABLE t, DESCRIPTOR(ts), INTERVAL '1' MINUTE, INTERVAL '1' HOUR)

-- 去重(保留每 key 最新一条)
SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY ts DESC) rn FROM t) WHERE rn = 1

-- Lookup Join(维表)
FROM fact AS f JOIN dim FOR SYSTEM_TIME AS OF f.proc_time AS d ON f.id = d.id

-- AI(2.1+/2.2+,详见 ai/ 与 docs/05-08)
CREATE MODEL m WITH (...);  SELECT * FROM ML_PREDICT(TABLE t, MODEL m, DESCRIPTOR(col));
```

## DataStream 常用骨架(Flink 2.x)

```java
env.enableCheckpointing(30_000, org.apache.flink.core.execution.CheckpointingMode.EXACTLY_ONCE);
WatermarkStrategy.<E>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((e, ts) -> e.ts).withIdleness(Duration.ofSeconds(30));
stream.keyBy(...).window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
      .aggregate(agg, windowFn).uid("必须显式");
```

## 排错三板斧

1. 窗口不出数 → 查 watermark(UI Watermarks 列)→ 分区空闲?时间单位?乱序上界?
2. 反压 → UI 反压页定位第一个 busy≈100% 的算子 → 看它做什么(外呼?倾斜?序列化?)
3. checkpoint 慢/超时 → 分解 sync/async/alignment 耗时 → 大状态上增量+非对齐,外呼改 Async I/O

---

## Wave 2 扩写

### 速查卡 1

命令/配置片段意图 1：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 2

命令/配置片段意图 2：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 3

命令/配置片段意图 3：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 4

命令/配置片段意图 4：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 5

命令/配置片段意图 5：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 6

命令/配置片段意图 6：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 7

命令/配置片段意图 7：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 8

命令/配置片段意图 8：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 9

命令/配置片段意图 9：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 10

命令/配置片段意图 10：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 11

命令/配置片段意图 11：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 12

命令/配置片段意图 12：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 13

命令/配置片段意图 13：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 14

命令/配置片段意图 14：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 15

命令/配置片段意图 15：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 16

命令/配置片段意图 16：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 17

命令/配置片段意图 17：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 18

命令/配置片段意图 18：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 19

命令/配置片段意图 19：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 20

命令/配置片段意图 20：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 21

命令/配置片段意图 21：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 22

命令/配置片段意图 22：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 23

命令/配置片段意图 23：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

### 速查卡 24

命令/配置片段意图 24：说明何时用、常见误用、关联模块编号（docs/01–14 或 ai 章），并提醒版本以根 README 矩阵为准。

