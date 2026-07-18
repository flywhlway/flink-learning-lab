# Phase 7: P6 总装 QA - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-07-19
**Phase:** 7-P6 总装 QA
**Mode:** `--auto`（via `/gsd-progress --next --auto`）
**Areas discussed:** 案例计量与补齐, 文档行数计量与补齐, qa_check 门禁硬化, ENG 终检与状态终稿

---

## 案例计量与补齐

| Option | Description | Selected |
|--------|-------------|----------|
| examples mains≥100 + 真实 Demo 扩容（优先 e12 缺口） | 与现有 qa_check ④ 段一致；禁虚增 | ✓ |
| 改口径计入 projects/C 标签凑数 | 不扩代码，重定义计数 | |
| 下调阈值为现状 67 | 违反 QA-02 / PHASES | |

**User's choice:** [auto] examples mains≥100 + 真实 Demo 扩容（recommended default）
**Notes:** 阈值 67→100；空壳 main 禁止

---

## 文档行数计量与补齐

| Option | Description | Selected |
|--------|-------------|----------|
| 全仓 md（排除 .planning）≥30k + 实质扩写 | docs/interview/best-practice 等；禁注水 | ✓ |
| 仅 docs/ 目录 ≥30k | 更窄口径 | |
| 计入 .planning 刷数 | 规划稿冒充产品文档 | |

**User's choice:** [auto] 全仓 md（排除 .planning）≥30k + 实质扩写（recommended default）
**Notes:** qa_check 新增行数硬检查

---

## qa_check 门禁硬化

| Option | Description | Selected |
|--------|-------------|----------|
| 六项硬门禁 + compile 硬失败 + 扩展违禁词（省略；略不裸匹配） | OrbStack 实测全绿 | ✓ |
| 保持 compile/行数为 warn | 弱门禁 | |
| 「略」字裸匹配全仓 | 误杀策略等词 | |

**User's choice:** [auto] 六项硬门禁 + compile 硬失败 + 扩展违禁词（recommended default）
**Notes:** D-07–D-09

---

## ENG 终检与状态终稿

| Option | Description | Selected |
|--------|-------------|----------|
| 脚本化 ENG 清单 + README/PHASES 终稿；tag 留给 complete-milestone | 可追溯关闭 ENG-* | ✓ |
| 本 Phase 直接打 git tag | 与里程碑完成命令重叠 | |
| 仅手写散文审计无脚本 | 难复现 | |

**User's choice:** [auto] 脚本化 ENG 清单 + 终稿；tag 延期（recommended default）
**Notes:** D-10–D-13 切片顺序：先红门禁→案例→文档→终稿复跑

---

## Claude's Discretion

- 具体案例/章节扩写顺序、ENG 脚本形态、是否产出 QA-REPORT.md

## Deferred Ideas

- git tag / Release → `/gsd-complete-milestone`
- Loki/OTel、第四生产项目
