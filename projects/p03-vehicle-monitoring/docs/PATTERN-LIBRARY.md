# p03 CEP 模式库（五元组评审）

本页登记 **恰好 3 条**预编译模式（D-01），与 `PatternIds` / `PatternRegistry` 常量一一对应。开源 Flink CEP 的 Pattern 在编译期固定；运行期切换激活集走 Broadcast 选择预编译模式集（e10 README / docs/10-cep），**不在本页声称已交付 Grafana 大盘或压测 baseline**（属后续 Phase）。

## 五元组登记表

| patternId | 业务含义 | within | 连接语义 | skip 策略 | 状态上界论证 |
|-----------|----------|--------|----------|-----------|--------------|
| `HARSH_THEN_FAULT` | 急加速后短时出现故障码（基线告警） | `30s` | `followedBy`（可穿插 `HEARTBEAT`） | 默认 `noSkip` | 单 vin 进行中部分匹配：已捕获 harsh、等待 fault 的分支数 ≤ 窗口内候选事件数；`within(30s)` 为部分匹配 TTL，watermark 推进后超时释放（可走 TIMEOUT Side Output） |
| `TRIPLE_HARSH` | 20s 内连续 3 次急加速突发 | `20s` | `times(3).consecutive()`（中间非匹配事件打断） | 默认 `noSkip`（量词内部 consecutive） | 进行中部分匹配长度 ≤ 2（凑满 3 次才 MATCH）；`within(20s)` 清理未完成序列；造数禁止在三次 `HARSH_ACCEL` 之间插入 `HEARTBEAT` |
| `DTC_PAIR` | 15s 内两次高严重度 DTC | `15s` | `followedBy`（两次 `DTC` value&gt;480） | **`skipPastLastEvent`**（挂在 `begin`） | 匹配成功后跳过至末事件，抑制 DTC 滑动窗口式重叠告警；`within(15s)` 限制「等待第二 DTC」部分匹配寿命 |

工厂实现：

- `HarshThenFaultPattern.build()` — `followedBy` + `within(30s)`
- `TripleHarshPattern.build()` — `times(3).consecutive()` + `within(20s)`
- `DtcPairPattern.build()` — `begin(..., skipPastLastEvent())` + `followedBy` + `within(15s)`

## Broadcast 门控与状态（踩坑）

**Broadcast 门控关闭仅抑制输出，三路 CEP 仍占状态。** 静态作业图始终并行跑满三条 `CEP.pattern(...)`；激活集只过滤落库/落 Kafka 的 `AlertEvent`，不会停掉未激活模式的 NFA。未激活模式仍会按各自 `within` TTL 增长与回收部分匹配（RESEARCH Pitfall 4）。

## 评审 checklist（缺项即失败）

合入前逐项勾选；**五元组任一缺项即失败**；**无 `within` 不得合入**（D-11 / docs/10-cep 红线）。

- [ ] 表格中恰好三行，且 `patternId` ∈ `{HARSH_THEN_FAULT, TRIPLE_HARSH, DTC_PAIR}`，与 `PatternIds` 同名
- [ ] 每行五列齐全：业务含义 / within / 连接语义 / skip 策略 / 状态上界（状态上界须写清 TTL/skip 如何限制部分匹配，禁止空话）
- [ ] 对应工厂方法含 `within`；`PatternRegistryWithinTest` GREEN（`getWindowSize().isPresent()`）
- [ ] `DTC_PAIR` 代码在 `begin` 显式挂 `skipPastLastEvent`（禁止仅文档写 skip、默认 `begin("dtc1")`）
- [ ] 未新增 `signalType`；模式谓词仅用 `HARSH_ACCEL` / `DTC` / `HEARTBEAT` 白名单
- [ ] 未声称 Grafana 大盘完成或压测 baseline 已交付

## 代码锚点

| 符号 | 路径 |
|------|------|
| `PatternIds` | `src/main/java/.../cep/PatternIds.java` |
| `PatternRegistry` | `src/main/java/.../cep/PatternRegistry.java` |
| within 门禁单测 | `src/test/java/.../cep/PatternRegistryWithinTest.java` |
