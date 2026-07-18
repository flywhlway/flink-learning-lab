# ADR-0001：开源 CEP 采用编译期 Pattern + Broadcast 选择预编译激活集

- **Status:** Accepted
- **Date:** 2026-07-18
- **Deciders:** flink-learning-lab P4 / p03（VEH-07 / D-12）
- **Tags:** cep, broadcast-state, pattern-library, flink-2.2

## Context

车联网告警需要在运行期切换「当前生效的模式集合」（例如从基线 `HARSH_THEN_FAULT` 切到 `TRIPLE_HARSH`），且验收要求 **不必重启作业换参数**。开源 Apache Flink CEP 把 Pattern 编译为 NFA：Pattern 定义在作业图构建期固定；社区路线并未提供「运行时任意字符串编译新 Pattern」的一等公民 API。

可选路径对比：

| 路径 | 能力 | 代价 / 风险 |
|------|------|-------------|
| A. 商业动态 CEP / 运行时编译规则引擎 | 运维侧热编任意规则 | 学习工程未纳入 SSOT；依赖与许可证超出本仓库范围；STACK 明确拒绝 |
| B. 重启作业换 Pattern 参数 | 实现简单 | 违反 p03 验收主路径（D-10：控制消息切换，非重启） |
| C. **静态预编译 Pattern 集 + Broadcast 选择激活集** | 不重启即可切换出口；可单测门控 | 未激活模式仍占 CEP 状态（须用 `within` TTL 回收） |

教材已给出开源动态化边界：[docs/10-cep §10-05](../../../../docs/10-cep/README.md)——预编译模式集配合 Broadcast State 做选择，而非运行时动态编译。

## Decision

采用路径 **C**：

1. **编译期**登记恰好三条预编译模式，常量与工厂一一对应：`PatternIds` / `PatternRegistry` / `HarshThenFaultPattern` / `TripleHarshPattern` / `DtcPairPattern`；每条强制 `within`（`PatternRegistryWithinTest` 门禁）。
2. **作业图**始终并行挂载三路 `CEP.pattern(...)`；出口经 `PatternActivationGate`（Broadcast State）按控制面 `activePatterns` ∩ 白名单过滤 `AlertEvent`。
3. **控制面** topic `vehicle.pattern.control`，JSON `{"activePatterns":["..."],"version":N}`；`version` 单调才生效（见项目 README §5.2）。
4. **明确不做**：商业动态 CEP、运行时编译 Pattern 字符串、用「重启作业换模式」作为验收主路径。

五元组与门控纪律写在 [PATTERN-LIBRARY.md](../PATTERN-LIBRARY.md)。架构总图见 [ARCHITECTURE.md](../ARCHITECTURE.md)。

## Consequences

### 正向

- 切换激活集可在 OrbStack 上用 `make verify-switch` 复现；权威出口仍是 ClickHouse `pattern_id`（`scripts/verify.sh`），Kafka 仅诊断。
- 模式评审可走五元组 checklist；无 `within` 不得合入，状态上界可论证。
- 与 e10-C5 / docs/10-cep 叙事一致，简历陈述可指向脚本路径（见 [RESUME.md](../RESUME.md)）。

### 负向 / 约束

- **Broadcast 门控关闭仅抑制输出，三路 CEP 仍占状态。** 未激活模式继续按各自 `within` 增长与回收部分匹配；运维不得假设「关闸 = 零状态」。详见 PATTERN-LIBRARY「Broadcast 门控与状态」。
- 新增业务模式必须改代码、发版、扩展白名单与造数 scenario——这是刻意的正确性边界，不是缺陷。
- 激活集不能表达「任意谓词热编」；阈值类运营观察走 Grafana 面板（[ANOMALY-THRESHOLDS.md](../ANOMALY-THRESHOLDS.md)），不扩 CEP 冒充异常检测。

### 可验证锚点

| 符号 / 产物 | 路径 |
|-------------|------|
| `PatternIds` | `src/main/java/.../cep/PatternIds.java` |
| `PatternRegistry` | `src/main/java/.../cep/PatternRegistry.java` |
| `PatternActivationGate` | `src/main/java/.../cep/PatternActivationGate.java` |
| 五元组评审 | [docs/PATTERN-LIBRARY.md](../PATTERN-LIBRARY.md) |
| 切换验收 | `make verify-switch` → CH `PATTERN_ID` |
| CEP 权威 verify | `PATTERN_ID=... make verify` → `scripts/verify.sh` |
