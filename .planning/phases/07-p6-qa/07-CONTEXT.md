# Phase 7: P6 总装 QA - Context

**Gathered:** 2026-07-19
**Status:** Ready for planning

<domain>
## Phase Boundary

本 Phase 交付里程碑收官质量门禁：**QA-01**（`scripts/qa_check.sh` 全绿）、**QA-02**（案例 ≥100、文档 ≥30k 行、README 终稿与 PHASES.md 状态一致），以及对 **ENG-01…ENG-04** 的终检与追溯关闭。对应 PHASES.md **P6 总装 QA**，使仓库达到可打 tag 的完成态。

本 Phase **不**新增第四个生产项目、**不**新拉可观测栈、**不**重做 P5 Operator/GitOps；只做计量补齐、门禁硬化、文档/状态终稿与工程不变量审计。

</domain>

<decisions>
## Implementation Decisions

### 案例计量与补齐（QA-02 案例轴）
- **D-01:** 案例口径锁定 **`examples/` 下含 `public static void main` 的可运行 Java 作业数**（与现有 `qa_check.sh` ④ 段一致）。`projects/` 主类可计入诊断输出，但 **硬门禁阈值只看 examples mains ≥100**。禁止用 README「C1/C2」标签虚增、禁止把 `.planning/` 或测试类计入。
- **D-02:** 补齐策略锁定 **真实可编译 Demo 扩容**（当前约 67 → ≥100）：优先补全 **e12 AI 专题缺失编号**（已有 01/02/03/04/06/07/08/15/17/18/22，缺口按学习路径补薄而完整的 runnable 作业），其次在配额不足的 e0x 模块按既有包结构追加案例；每个新案例须有 README 八段式要点或模块 README 登记，且本机 `mvn -pl … -am -DskipTests compile` 通过。禁止空壳 `main`、禁止「TODO/自行实现」占位。
- **D-03:** `qa_check.sh` 案例阈值从 **67（Phase3 口径）升级为 100**；失败非 0。诊断行继续打印实际 mains 数。

### 文档行数计量与补齐（QA-02 文档轴）
- **D-04:** 文档行数口径锁定 **仓库内全部 `*.md`，排除 `.planning/` 与 `.git/`**（当前约 ~10k，目标 ≥30000）。`docs/`、`interview/`、`best-practice/`、`production/`、`examples/**/README.md`、`projects/**/docs` 等均计入；**禁止**把 `.planning/` 规划稿计入以刷数。
- **D-05:** 补齐策略锁定 **实质内容扩写，禁止注水**：优先扩写（1）`docs/` 仍偏薄模块与交叉引用；（2）`interview/` 参考答案深度（已 ≥150 题，加厚答案与链接教材）；（3）`best-practice/` / `production/` 互链正文缺口。禁止大段复制粘贴官网、禁止重复同一段落刷行数。
- **D-06:** `qa_check.sh` 新增 **文档行数 ≥30000** 硬检查（`find`+`wc -l` 或等价 Python）；失败非 0，并打印实际行数。

### qa_check 门禁硬化（QA-01）
- **D-07:** QA-01 硬门禁集合锁定为：**(1)** docker compose config（或既有 yaml 回退）**(2)** 违禁词扫描 **(3)** Markdown 相对断链 **(4)** 案例 ≥100 **(5)** 文档 ≥30k **(6)** `examples` Maven compile（升级为硬失败，去掉「warn 可忽略」路径——无 mvn 时明确 fail 并提示合入前必须本机通过）。
- **D-08:** 违禁词扫描扩展为匹配 **`TODO|FIXME|自行实现|请参考官网|省略`**；对单独汉字「略」**不**做裸匹配（避免误杀「策略/省略说明中的合法用词」），但「省略」整词保留。扫描排除规则保持：`.planning/`、`PHASES.md`、`CLAUDE.md`、`AGENTS.md`、`scripts/qa_check.sh`、`scripts/README.md`、`docs/README.md`（规则声明处）。
- **D-09:** 本 Phase 结束前必须在 OrbStack arm64 上 **真实跑通** `bash scripts/qa_check.sh` 退出码 0；禁止沙箱伪绿。

### ENG 终检与状态终稿（ENG-01…04 + QA-02 状态轴）
- **D-10:** ENG 终检交付 **可脚本化检查清单**（推荐 `scripts/eng_audit.sh` 或并入 `qa_check.sh` ⑥ 段）：ENG-01 版本矩阵 vs pom 属性抽样；ENG-02 `docs/README.md` 编号登记完整性；ENG-03 抽检近期交付物无违禁词且关键路径有 verify/baseline 证据指针；ENG-04 CHANGELOG 未发布区与 PHASES 状态列可更新痕迹。清单失败则 Phase 不标完成。
- **D-11:** README / PHASES / PROJECT 终稿：根 `README.md` 版本矩阵与完成态表述对齐；`PHASES.md` P6 状态列改为可验证完成态；`.planning/PROJECT.md` / `REQUIREMENTS.md` 将 QA-* / ENG-* 标为完成（在实测绿之后）；三生产项目与 P5 目录交叉引用无断链。
- **D-12:** **git tag / GitHub Release 不在本 Phase 硬门禁**——留给 `/gsd-complete-milestone`；本 Phase 只保证「可打 tag」（门禁绿 + 状态一致 + CHANGELOG 具备发布说明草稿）。

### 交付切片建议（供 planner 切 wave）
- **D-13:** 推荐执行顺序：**(1)** 升级 `qa_check.sh` 阈值与 ENG 审计骨架（先红）→ **(2)** 案例扩容至 ≥100（编译绿）→ **(3)** 文档实质扩写至 ≥30k → **(4)** 清违禁词/断链 + README/PHASES/CHANGELOG 终稿 + 全绿复跑。计量脚本变更可与内容扩写并行，但 **终态验收必须以一次全绿 `qa_check.sh` 为准**。

### Claude's Discretion
- 具体补哪些 e12/e0x 案例编号与类名、文档扩写优先章节顺序、ENG 审计脚本是独立文件还是并入 qa_check、CHANGELOG 发布说明文案细节、是否额外产出 `docs/QA-REPORT.md` 人读摘要（不挡门禁）。

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Roadmap / Requirements
- `.planning/ROADMAP.md` — Phase 7 目标与成功标准（QA-01/02、ENG-01…04）
- `.planning/REQUIREMENTS.md` — QA-01、QA-02、ENG-01…ENG-04 条文
- `.planning/PROJECT.md` — P6 总装 QA 勾选与核心不变量
- `PHASES.md` — P6 行：qa_check 全绿；案例 ≥100；文档 ≥30k 行；工程接力协议不变量

### 门禁与计量权威
- `scripts/qa_check.sh` — 现有五段检查；本 Phase 升级阈值与硬化 compile/行数
- `scripts/README.md` — 脚本索引
- `scripts/count_interview.py` — interview 计数样板（Phase 6）；文档/案例计数可类比

### 文档与编号 SSOT
- `docs/README.md` — 模块编号登记（ENG-02）
- `README.md` — 版本矩阵 SSOT（ENG-01）与仓库完成态表述
- `examples/pom.xml` — 属性区版本 SSOT
- `CHANGELOG.md` — 未发布区 / 发布说明草稿（ENG-04）

### 先前 Phase 完成态样板
- `.planning/phases/06-p5/06-CONTEXT.md` — P5 决策与「P6 计量终检」延期项
- `.planning/phases/06-p5/06-VERIFICATION.md` — 验证报告格式样板
- `projects/p03-vehicle-monitoring/`、`projects/p01-log-ai-platform/`、`projects/p02-realtime-reco/` — 八段式/verify 纪律样板（终检抽检对象）

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/qa_check.sh`：compose / 违禁词 / 断链 / mains 计数 / 可选 mvn — 本 Phase 主演进点
- `scripts/count_interview.py`：结构化计数 + exit 码门禁模式，可复用于文档行数或案例报告
- `examples/` Maven 多模块 + 各 e* README：案例扩容落点
- `interview/L1–L8.md`：已达 ≥150 题，适合加厚答案抬升行数且不注水

### Established Patterns
- 硬门禁失败非 0；诊断信息打印实际计量值
- 排除 `.planning/` 出扫描范围，避免规划稿污染产品门禁
- 完成态回填：docs/README 登记 + CHANGELOG + PHASES 状态列同会话更新
- 内容禁令：无 TODO / 自行实现 / 请参考官网；OrbStack 实测才标 ✅

### Integration Points
- CI（Phase 6 GHA）已跑 `qa_check.sh` — 阈值升级后 CI 同步变严
- 根 README / PHASES / PROJECT 状态列在终检绿后一次性对齐
- e12 模块编号缺口是最快的案例补齐路径

</code_context>

<specifics>
## Specific Ideas

- 现状快照（2026-07-19 讨论时）：`examples` mains ≈ **67**；全仓非 `.planning` md ≈ **~9992** 行；距 100 / 30000 均有硬缺口。
- e12 已存在模块编号不连续，补齐编号比从零开新专题更符合学习路径。
- 「略」字不做裸正则，避免误杀「策略」等正常中文。

</specifics>

<deferred>
## Deferred Ideas

- git tag / GitHub Release —— `/gsd-complete-milestone`
- 新生产项目 / 新可观测栈（Loki/OTel）—— 超出 P6 总装范围
- 把案例口径改为「含 projects」或「README C 标签」—— 已拒绝，避免虚增

None — discussion stayed within phase scope（除里程碑 tag 明确延期）

</deferred>

---

*Phase: 7-P6 总装 QA*
*Context gathered: 2026-07-19*
