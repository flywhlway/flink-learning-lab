# benchmark/ · 压测方法论与基准工程(Phase 5 落地)

## 方法论(先于工具确立)

1. **单变量原则**:每轮只动一个旋钮(并行度/内存/state backend/序列化),其余冻结。
2. **口径固定**:吞吐 = source 侧 numRecordsOutPerSecond 稳态均值;延迟 = 事件时间到 sink 落地的 P50/P99;checkpoint 指标取连续 20 次的 P95;GC 用 JVM 指标 + jstat 双口径。
3. **热身丢弃**:前 3 分钟数据不计入。
4. **报告模板**:环境快照(镜像/TM 数/slots/内存)→ 负载定义(eps/倾斜/乱序)→ 指标表 → 火焰图/反压截图(Mermaid/文本化)→ 结论与下一步。

## 计划中的基准矩阵(P5)

| 轴 | 取值 |
|---|---|
| 作业 | e01-J2(窗口聚合)/ e05 双流 Join / e10 CEP |
| 负载 | 1k / 5k / 20k eps;倾斜 0% / 70% |
| State Backend | HashMap / RocksDB(增量)/ ForSt |
| Checkpoint | 对齐 / 非对齐;30s / 10s |
| 观测 | TPS、P99、ckpt 时长、反压比、GC、CPU、单 slot 吞吐 |

参照系:Nexmark(社区标准基准)选取 q4/q7/q17 对齐验证方法正确性。本机(M5 Pro, 2TM×4slots)的**参考基线**将在 P5 实测后写入 `baseline.md`,此前不放任何未实测数字。
