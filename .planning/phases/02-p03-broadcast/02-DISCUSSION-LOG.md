# Phase 2: p03 模式库与 Broadcast - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-18
**Phase:** 2-p03 模式库与 Broadcast
**Mode:** `--auto --chain`
**Areas discussed:** 模式库条目选型, Broadcast 控制面, CEP 与 Broadcast 接线, 验收与 within 门禁

---

## 模式库条目选型

| Option | Description | Selected |
|--------|-------------|----------|
| 恰好 3 条、复用现有信号白名单（HARSH_THEN_FAULT / TRIPLE_HARSH / DTC_PAIR） | 教学覆盖 followedBy / times+consecutive / skip；Parse 零扩展 | ✓ |
| ≥5 条并扩展 SPEED 等 signalType | 模式更丰富但扩大 Parse/造数/DDL 面 | |
| 仅文档登记 3 条、代码仍单模式 | 不满足 VEH-04 可观察切换 | |

**User's choice:** [auto] 恰好 3 条、复用现有信号白名单
**Notes:** `[auto] 模式库条目选型 — Q: "模式库交付几条、是否扩展信号？" → Selected: "恰好 3 条、复用现有信号白名单" (recommended default)`

---

## Broadcast 控制面

| Option | Description | Selected |
|--------|-------------|----------|
| Kafka topic `vehicle.pattern.control` + 确定性 JSON | 对齐 e03-C7，可造数/可验收，不重启换模式 | ✓ |
| 作业启动参数 / 配置文件热读 | 演示弱、不像 Broadcast 动态选择 | |
| REST / 外部配置中心 | 超出学习工程本 Phase 范围 | |

**User's choice:** [auto] Kafka control topic + 确定性 JSON
**Notes:** `[auto] Broadcast 控制面 — Q: "如何下发激活模式集？" → Selected: "Kafka vehicle.pattern.control" (recommended default)`；默认激活 `HARSH_THEN_FAULT` 保 Phase 1 绿。

---

## CEP 与 Broadcast 接线

| Option | Description | Selected |
|--------|-------------|----------|
| 静态多 CEP 分支 + Broadcast 出口门控（patternId 过滤） | 开源可行；清晰表达「选预编译集」 | ✓ |
| 运行时动态编译 / 替换 Pattern | 需商业引擎；STACK 拒绝 | |
| 仅 Broadcast 改阈值、仍单模式 | 不满足模式库 ≥3 | |

**User's choice:** [auto] 静态多 CEP + 出口门控
**Notes:** `[auto] CEP 与 Broadcast 接线 — Q: "作业图如何表达动态选择？" → Selected: "静态多 CEP + Broadcast 门控" (recommended default)`；AlertEvent/CH 增加 patternId。

---

## 验收与 within 门禁

| Option | Description | Selected |
|--------|-------------|----------|
| CH 按 pattern_id 断言切换 + 注册表/单测强制 within + 五元组文档 | 延续 Phase 1 验收纪律并满足成功标准 1–3 | ✓ |
| 仅人工看 Flink UI / 日志 | 不可自动化、易漂 | |
| 文档清单无代码门禁 | 无法阻止无 within 合入 | |

**User's choice:** [auto] CH pattern_id 断言 + within 自检 + PATTERN-LIBRARY.md
**Notes:** `[auto] 验收与 within 门禁 — Q: "如何证明切换与五元组纪律？" → Selected: "CH 断言 + 注册表/单测 + 五元组文档" (recommended default)`

---

## Claude's Discretion

- 门控算子命名、Broadcast descriptor key、控制 JSON 字段微调
- TRIPLE_HARSH / DTC_PAIR 阈值与 within 秒数微调（须写入五元组）

## Deferred Ideas

- Grafana / 压测演练 / ADR·简历页 → Phase 3
- 商业动态 CEP → 拒绝
- 新增 signalType → backlog
