---
phase: 02-p03-broadcast
plan: "02b"
type: execute
wave: 3
depends_on:
  - "02-02"
files_modified:
  - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/AlertEvent.java
  - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/AlertPatternHandler.java
  - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/HarshThenFaultHandler.java
  - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/TripleHarshHandler.java
  - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/DtcPairHandler.java
  - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java
  - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/JobConfig.java
  - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/sink/ClickHouseAlertSink.java
  - projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/HarshThenFaultPatternTest.java
  - projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql
  - docker/docker-compose.yml
autonomous: true
requirements:
  - VEH-04
user_setup: []

must_haves:
  truths:
    - "作业静态并行挂载 3 条 CEP.pattern（各自 within），Handler 输出带 patternId；union 后经 Broadcast 门控再双写（D-07/D-08/D-09）"
    - "控制面消费 Kafka topic vehicle.pattern.control；空 Broadcast State 默认激活 [HARSH_THEN_FAULT] 时 Phase 1 路径语义保持（D-04/D-06）"
    - "AlertEvent 与 ClickHouse vehicle_alerts 含 pattern_id；TIMEOUT 与 MATCH 均带 patternId 并受同一激活集门控（D-08/D-09）"
    - "p03-init 幂等创建 vehicle.pattern.control；mvn test 与 package GREEN"
  artifacts:
    - path: "projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java"
      provides: "三 CEP + control broadcast + gate + 双写 gated（D-07）"
      contains: "p03-gate-pattern-activation"
    - path: "projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql"
      provides: "pattern_id 列 + 幂等 ALTER（D-08）"
      contains: "pattern_id"
    - path: "docker/docker-compose.yml"
      provides: "p03-init 创建 vehicle.pattern.control（D-04）"
      contains: "vehicle.pattern.control"
  key_links:
    - from: "vehicle.pattern.control"
      to: "PatternActivationGate Broadcast State"
      via: "KafkaSource → parse → broadcast(ACTIVE_DESC)"
      pattern: "pattern.control|broadcast|PatternActivationGate"
    - from: "三 CEP Handler AlertEvent.patternId"
      to: "gated → Kafka alerts + ClickHouse"
      via: "activePatterns.contains(patternId)"
      pattern: "patternId|pattern_id|activePatterns"
---

## Phase Goal

**As a** 仓库维护者，**I want to** 维护恰好 3 条可评审 CEP 模式库并通过 Kafka Broadcast 控制消息切换预编译激活集、在 ClickHouse 按 pattern_id 断言匹配变化，**so that** 无 within 不得合入且不必重启作业即可演示开源 CEP 动态化路线。

<objective>
垂直切片 VEH-04（作业半段）：静态三 CEP + 接线已有 PatternActivationGate + pattern_id DDL/Sink + control topic，使可打包作业在默认激活集下保持 Phase 1 语义。

Purpose: 打通「预编译模式集 + Broadcast 选择」运行时路径；造数切换剧本与 e2e 留给 02-03。
Output: VehicleAlertJob 三分支+门控；DDL/compose/Sink 含 pattern_id；mvn package 成功。
</objective>

<execution_context>
@$HOME/.cursor/get-shit-done/workflows/execute-plan.md
@$HOME/.cursor/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/phases/02-p03-broadcast/02-CONTEXT.md
@.planning/phases/02-p03-broadcast/02-RESEARCH.md
@.planning/phases/02-p03-broadcast/02-PATTERNS.md
@.planning/phases/02-p03-broadcast/02-02-SUMMARY.md
@projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java
@projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/AlertPatternHandler.java
@projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/PatternActivationGate.java
@projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: AlertEvent.patternId + 三模式 Handler 族</name>
  <files>projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/AlertEvent.java, projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/AlertPatternHandler.java, projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/HarshThenFaultHandler.java, projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/TripleHarshHandler.java, projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/DtcPairHandler.java, projects/p03-vehicle-monitoring/src/test/java/com/flywhl/flinklab/p03/cep/HarshThenFaultPatternTest.java</files>
  <read_first>
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/AlertPatternHandler.java（MATCH/TIMEOUT；Pitfall 7 勿硬读步骤名）
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/model/AlertEvent.java
    - .planning/phases/02-p03-broadcast/02-CONTEXT.md（D-08/D-09）
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/cep/PatternIds.java
    - 02-02 交付的 PatternActivationGate（确认 AlertEvent.patternId 字段名）
  </read_first>
  <behavior>
    - AlertEvent 增加 public String patternId；构造函数同步；JSON schema/序列化若已有则带上 patternId（per D-08）
    - 每模式独立 Handler（或参数化工厂）：写入对应 PatternIds；TIMEOUT Side Output 同样带 patternId（per D-09）；禁止 Triple/Dtc Handler 硬读 match.get("harsh")/"fault"
    - HarshThenFaultPatternTest TIMEOUT 用例更新为断言 patternId=HARSH_THEN_FAULT；相关 cep 单测 GREEN
  </behavior>
  <action>
    落地 AlertEvent.patternId 与 Handler 族，使每条 CEP 输出带正确 patternId（含 TIMEOUT）。若 02-02 Gate 测试曾用 stub 字段，统一到正式 AlertEvent.patternId。不在本任务改 VehicleAlertJob 接线或 DDL/compose（Task 2）。
  </action>
  <verify>
    <automated>cd projects/p03-vehicle-monitoring &amp;&amp; mvn -q -Dtest=HarshThenFaultPatternTest,PatternActivationGateTest test</automated>
  </verify>
  <acceptance_criteria>
    - AlertEvent 含 patternId 字段
    - 三 Handler（或等价）存在且写入 PatternIds 常量
    - TIMEOUT 路径带 patternId（源码或测试可观察）
    - 上述 surefire exit 0
  </acceptance_criteria>
  <done>patternId 贯通 AlertEvent/Handler/TIMEOUT（D-08/D-09）。</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: 三 CEP 作业接线 + pattern_id DDL/Sink + control topic</name>
  <files>projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java, projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/JobConfig.java, projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/sink/ClickHouseAlertSink.java, projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql, docker/docker-compose.yml</files>
  <read_first>
    - projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java（现有单 CEP + union + 双写）
    - .planning/phases/02-p03-broadcast/02-RESEARCH.md（作业图接线示意）
    - .planning/phases/02-p03-broadcast/02-CONTEXT.md（D-04/D-07/D-08）
    - projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql
    - docker/docker-compose.yml p03-init 段
    - PatternActivationGate（02-02）与三 Handler（本计划 Task 1）
  </read_first>
  <behavior>
    - VehicleAlertJob：保留 events→Parse→WM(ooo=5s,idle=30s)；并行三条 CEP.pattern(keyBy vin, PatternX.build()).process(HandlerX).uid("p03-cep-...")；各 MATCH∪TIMEOUT → allAlerts；新增 control KafkaSource（JobConfig.controlTopic 默认 vehicle.pattern.control，WatermarkStrategy.noWatermarks）→ parse JSON → broadcast → allAlerts.connect → PatternActivationGate.uid("p03-gate-pattern-activation")；仅 gated 双写 Kafka+CH（per D-04/D-07）
    - ClickHouseAlertSink INSERT 列含 pattern_id；validate 对 patternId 拒引号/反斜杠
    - clickhouse_alerts.sql：CREATE 含 pattern_id；并 ALTER TABLE ... ADD COLUMN IF NOT EXISTS pattern_id（per D-08）
    - p03-init 追加 kafka-topics --create --if-not-exists vehicle.pattern.control；保持 profiles: ["p03"]；不污染 default make up（per D-04）
    - 全模块 mvn test 与 package GREEN
  </behavior>
  <action>
    按 RESEARCH 作业图改造 VehicleAlertJob；JobConfig.from 增加 --control-topic 默认 vehicle.pattern.control。ParseVehicleJson 白名单零扩展（D-01）。禁止商业动态 CEP / 运行时编译 Pattern。禁止在 CEP 前用 Broadcast 过滤事件（门控必须在出口）。算子均 .uid("p03-...")。完成后 mvn -q test 与 mvn -q package 必须通过。造数切换 e2e 留给 02-03。
  </action>
  <verify>
    <automated>cd projects/p03-vehicle-monitoring &amp;&amp; mvn -q test &amp;&amp; mvn -q package &amp;&amp; rg -n 'vehicle.pattern.control' docker/docker-compose.yml &amp;&amp; rg -n 'pattern_id' projects/p03-vehicle-monitoring/sql/clickhouse_alerts.sql projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/sink/ClickHouseAlertSink.java &amp;&amp; rg -n 'p03-gate-pattern-activation' projects/p03-vehicle-monitoring/src/main/java/com/flywhl/flinklab/p03/VehicleAlertJob.java</automated>
  </verify>
  <acceptance_criteria>
    - VehicleAlertJob 出现三处 CEP.pattern（或三 uid p03-cep-harsh-then-fault / triple-harsh / dtc-pair）及 p03-gate-pattern-activation
    - clickhouse_alerts.sql 与 Sink 均含 pattern_id
    - docker-compose p03-init 含 vehicle.pattern.control
    - mvn test 与 mvn package exit 0
  </acceptance_criteria>
  <done>VEH-04 作业可打包：静态三 CEP + Broadcast 门控 + pattern_id 落库路径就绪（D-04–D-09）。</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Kafka control topic → Broadcast State | 未认证 lab 网络上的控制 JSON 可改变激活集 |
| AlertEvent.patternId → ClickHouse INSERT | 字段进入 HTTP SQL 字符串 |
| 事件流 → 三路 CEP | 脏事件放大部分匹配状态 |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-02-01 | Tampering | PatternControlMessage / Gate 接线 | mitigate | 沿用 02-02 白名单求交；坏消息跳过；version 单调（D-05） |
| T-02-06 | Tampering | ClickHouseAlertSink.pattern_id | mitigate | validate 拒引号/反斜杠；可选 PatternIds 白名单 |
| T-02-07 | Denial of Service | 三路 CEP 并行 | mitigate | 强制 within；Parse 白名单不扩；文档标明门控≠停状态 |
| T-02-08 | Elevation of Privilege | control topic 无 ACL | accept | 学习环境本地 Compose；README 标明勿对公网暴露（本 Phase 不做 ACL） |
| T-02-SC | Tampering | 依赖安装 | accept | 无新 Maven/PyPI 包 |
</threat_model>

<verification>
- 全模块 mvn test + package GREEN
- compose 含 control topic；DDL/Sink 含 pattern_id
- VehicleAlertJob 含三 CEP + p03-gate-pattern-activation
</verification>

<success_criteria>
- 静态三 CEP + 出口门控接线完成（D-07）
- control topic + patternId 贯通 AlertEvent/TIMEOUT/CH（D-04/D-08/D-09）
- 造数切换 e2e 留给 02-03
</success_criteria>

<output>
Create `.planning/phases/02-p03-broadcast/02-02b-SUMMARY.md` when done
</output>
