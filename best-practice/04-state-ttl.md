# 04 · 状态与 TTL

## 规则

1. **选型：** 小状态 HashMap；GB 级 RocksDB（+增量 CP）；ForSt 等须先登记版本矩阵再启用。
2. **SQL 必须设 `table.exec.state.ttl`** 并书面论证对正确性影响（总则 #11）。
3. **DataStream TTL** 用于会话/去重/维表缓存；禁止对仍需 `allowedLateness` 的窗口状态设过短 TTL。
4. **精确去重** 必须给出 key 基数 × 每 key 字节上界，否则改用 HLL/下推 OLAP（总则 #5）。

## 理由

默认永不过期 → 状态单调涨 → checkpoint 变慢 → 反压与超时连锁。TTL 过短则「忘却」导致重复计数或错连。

## 反例

- 无窗口 `GROUP BY` 用户维度不做 TTL，一周后 TM 磁盘打满。
- 把精确 UV 放在 Flink 长驻 MapState，无上界论证。

## 落地互链

- 状态教材：[`docs/03-state/`](../docs/03-state/)
- 压测观察 checkpoint size：[`benchmark/baseline.md`](../benchmark/baseline.md)
