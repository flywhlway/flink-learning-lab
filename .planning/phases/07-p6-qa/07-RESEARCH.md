# Phase 7: P6 总装 QA - Research

**Researched:** 2026-07-19
**Domain:** 仓库质量门禁硬化 · 案例/文档计量补齐 · ENG 不变量终检脚本
**Confidence:** HIGH（缺口与脚本行为均本机实测；Flink Demo 模式复用仓库既有零依赖样板）

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### 案例计量与补齐（QA-02 案例轴）
- **D-01:** 案例口径锁定 **`examples/` 下含 `public static void main` 的可运行 Java 作业数**（与现有 `qa_check.sh` ④ 段一致）。`projects/` 主类可计入诊断输出，但 **硬门禁阈值只看 examples mains ≥100**。禁止用 README「C1/C2」标签虚增、禁止把 `.planning/` 或测试类计入。
- **D-02:** 补齐策略锁定 **真实可编译 Demo 扩容**（当前约 67 → ≥100）：优先补全 **e12 AI 专题缺失编号**（已有 01/02/03/04/06/07/08/15/17/18/22，缺口按学习路径补薄而完整的 runnable 作业），其次在配额不足的 e0x 模块按既有包结构追加案例；每个新案例须有 README 八段式要点或模块 README 登记，且本机 `mvn -pl … -am -DskipTests compile` 通过。禁止空壳 `main`、禁止「TODO/自行实现」占位。
- **D-03:** `qa_check.sh` 案例阈值从 **67（Phase3 口径）升级为 100**；失败非 0。诊断行继续打印实际 mains 数。

#### 文档行数计量与补齐（QA-02 文档轴）
- **D-04:** 文档行数口径锁定 **仓库内全部 `*.md`，排除 `.planning/` 与 `.git/`**（当前约 ~10k，目标 ≥30000）。`docs/`、`interview/`、`best-practice/`、`production/`、`examples/**/README.md`、`projects/**/docs` 等均计入；**禁止**把 `.planning/` 规划稿计入以刷数。
- **D-05:** 补齐策略锁定 **实质内容扩写，禁止注水**：优先扩写（1）`docs/` 仍偏薄模块与交叉引用；（2）`interview/` 参考答案深度（已 ≥150 题，加厚答案与链接教材）；（3）`best-practice/` / `production/` 互链正文缺口。禁止大段复制粘贴官网、禁止重复同一段落刷行数。
- **D-06:** `qa_check.sh` 新增 **文档行数 ≥30000** 硬检查（`find`+`wc -l` 或等价 Python）；失败非 0，并打印实际行数。

#### qa_check 门禁硬化（QA-01）
- **D-07:** QA-01 硬门禁集合锁定为：**(1)** docker compose config（或既有 yaml 回退）**(2)** 违禁词扫描 **(3)** Markdown 相对断链 **(4)** 案例 ≥100 **(5)** 文档 ≥30k **(6)** `examples` Maven compile（升级为硬失败，去掉「warn 可忽略」路径——无 mvn 时明确 fail 并提示合入前必须本机通过）。
- **D-08:** 违禁词扫描扩展为匹配 **`TODO|FIXME|自行实现|请参考官网|省略`**；对单独汉字「略」**不**做裸匹配（避免误杀「策略/省略说明中的合法用词」），但「省略」整词保留。扫描排除规则保持：`.planning/`、`PHASES.md`、`CLAUDE.md`、`AGENTS.md`、`scripts/qa_check.sh`、`scripts/README.md`、`docs/README.md`（规则声明处）。
- **D-09:** 本 Phase 结束前必须在 OrbStack arm64 上 **真实跑通** `bash scripts/qa_check.sh` 退出码 0；禁止沙箱伪绿。

#### ENG 终检与状态终稿（ENG-01…04 + QA-02 状态轴）
- **D-10:** ENG 终检交付 **可脚本化检查清单**（推荐 `scripts/eng_audit.sh` 或并入 `qa_check.sh` ⑥ 段）：ENG-01 版本矩阵 vs pom 属性抽样；ENG-02 `docs/README.md` 编号登记完整性；ENG-03 抽检近期交付物无违禁词且关键路径有 verify/baseline 证据指针；ENG-04 CHANGELOG 未发布区与 PHASES 状态列可更新痕迹。清单失败则 Phase 不标完成。
- **D-11:** README / PHASES / PROJECT 终稿：根 `README.md` 版本矩阵与完成态表述对齐；`PHASES.md` P6 状态列改为可验证完成态；`.planning/PROJECT.md` / `REQUIREMENTS.md` 将 QA-* / ENG-* 标为完成（在实测绿之后）；三生产项目与 P5 目录交叉引用无断链。
- **D-12:** **git tag / GitHub Release 不在本 Phase 硬门禁**——留给 `/gsd-complete-milestone`；本 Phase 只保证「可打 tag」（门禁绿 + 状态一致 + CHANGELOG 具备发布说明草稿）。

#### 交付切片建议（供 planner 切 wave）
- **D-13:** 推荐执行顺序：**(1)** 升级 `qa_check.sh` 阈值与 ENG 审计骨架（先红）→ **(2)** 案例扩容至 ≥100（编译绿）→ **(3)** 文档实质扩写至 ≥30k → **(4)** 清违禁词/断链 + README/PHASES/CHANGELOG 终稿 + 全绿复跑。计量脚本变更可与内容扩写并行，但 **终态验收必须以一次全绿 `qa_check.sh` 为准**。

### Claude's Discretion
- 具体补哪些 e12/e0x 案例编号与类名、文档扩写优先章节顺序、ENG 审计脚本是独立文件还是并入 qa_check、CHANGELOG 发布说明文案细节、是否额外产出 `docs/QA-REPORT.md` 人读摘要（不挡门禁）。

### Deferred Ideas (OUT OF SCOPE)
- git tag / GitHub Release —— `/gsd-complete-milestone`
- 新生产项目 / 新可观测栈（Loki/OTel）—— 超出 P6 总装范围
- 把案例口径改为「含 projects」或「README C 标签」—— 已拒绝，避免虚增

None — discussion stayed within phase scope（除里程碑 tag 明确延期）
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| QA-01 | `scripts/qa_check.sh` 全绿（mvn 编译、compose config、断链、违禁词、案例计数） | D-07 六硬门 + D-08 词表 + D-09 OrbStack 实测；见「qa_check 升级设计」与 Common Pitfalls |
| QA-02 | 案例 ≥100、文档 ≥30k 行、README 终稿与 PHASES.md 状态一致 | D-01…D-06 计量口径；案例扩容清单 + 文档 +20k 行目标表；D-11 终稿 |
| ENG-01 | 新增组件先写入根 README 版本矩阵与 pom 属性区再使用 | `eng_audit` ENG-01：矩阵↔`examples/pom.xml` `<*.version>` 抽样对齐 |
| ENG-02 | 文档/模块编号先在 docs/README.md 登记；八段式或等价 | `eng_audit` ENG-02：`docs/README.md` 模块 00–15 链接存在；新 e12 登记到 `examples/README.md` + ai 目录 |
| ENG-03 | OrbStack arm64 实测；禁止 TODO/省略/… | D-08/D-09 + ENG-03 抽检 verify/baseline 指针；注意「省略」误杀需先改写 |
| ENG-04 | 会话结束更新 CHANGELOG 未发布区与 PHASES 状态 | `eng_audit` ENG-04：`CHANGELOG.md` 未发布区非空 + PHASES P6 可验证表述 |
</phase_requirements>

## Summary

Phase 7 是里程碑收官闸门，不是新功能交付。核心约束是三类计量/门禁同时变绿：**(1)** `examples/` 内 `public static void main` ≥100（本机实测 **67**，缺口 **~33**）；**(2)** 全仓 `*.md`（排除 `.planning/`/`.git/`）≥30000 行（本机实测 **9992**，缺口 **~20008**）；**(3)** 将现有 `scripts/qa_check.sh` 从「五段、compile 可 warn 跳过、阈值 67」硬化为 D-07 六硬门，并交付可脚本化的 ENG-01…04 终检。

案例侧：e12-03/04 仅为 SQL 脚本（**不计 main**）；父 `examples/pom.xml` **未聚合** e09/e12-03/04/e12-07/08（07/08 有独立 pom 但仍有 main 被 grep 计入）。最快合规路径是按 ai/ 24 章缺口补 **零依赖 DataStream** 模块（复用 `Labs` + e12-01/17/22 骨架），再在 e08（仅 1 main）与 e01/e04/e11 等补齐差额。文档侧：`docs/` 合计仅 **918** 行、`interview/` 约 **4.1 行/题**、`best-practice/` **230** 行——加厚这三块即可在禁止注水前提下覆盖 +20k；`production/docs/bluegreen-timeline.md`（1408 行）已很厚，不应再堆重复 timeline。

**Primary recommendation:** 严格按 D-13 四波：先改 `qa_check`/`eng_audit` 让门禁变红 → 批量加 ≥33 个真实零依赖 mains（优先 e12-09/11/12/13/14/16/19/20/21/23 + e08/e0x）→ 按目标表实质扩写 docs/interview/best-practice → 清「省略」误杀与断链后 OrbStack 全绿；tag 留给 complete-milestone。

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| qa_check / eng_audit 门禁脚本 | API / Backend（宿主机 CLI） | — | 在开发机/CI runner 执行；不进 Flink JM |
| examples Demo 编译与 main 计量 | API / Backend（Maven + JDK 21） | — | 父工程 `examples/` 聚合编译 |
| Flink 作业语义（Demo 本体） | API / Backend（Flink DataStream） | Database / Storage（可选 Kafka/Redis，优先 datagen） | 零依赖优先 datagen；外呼用 mock |
| 文档/题库/规范扩写 | CDN / Static（仓库 Markdown） | — | 纯内容资产；计入 D-04 行数 |
| compose YAML 校验 | API / Backend（Docker/OrbStack 或 PyYAML） | — | 现有①段；CI 已装 PyYAML |
| OrbStack 终验（D-09） | API / Backend（本机 arm64） | — | 禁止沙箱伪绿；CI 不替代 |
| README/PHASES/CHANGELOG 终稿 | CDN / Static | — | D-11/D-12；tag 不在本 Phase |
| GHA CI 同步变严 | API / Backend（GitHub runners） | — | 已跑 `qa_check.sh`；阈值升级后自动收紧 |

## Project Constraints (from .cursor/rules/)

**`.cursor/rules/` 不存在**（研究时已确认）。约束来自 `CLAUDE.md` / PROJECT / PHASES 接力协议，与 CONTEXT 锁定一致：

- 版本 SSOT：根 README 矩阵 + `examples/pom.xml` 属性区；新增组件先登记
- 文档编号：先在 `docs/README.md` 登记；八段式强制
- 运行环境：OrbStack arm64 实测；不可验证不合入
- 内容禁令：禁止 TODO、省略、略、自行实现、请参考官网（扫描实现以 **D-08** 为准：不裸匹配「略」）
- Tech stack：Flink **2.2.1**、JDK **21**、Kafka connector **5.0.0-2.2**
- 会话收尾：CHANGELOG 未发布区 + PHASES 状态列；约定式提交
- GSD：规划产物经 GSD 工作流；本 Phase 不新拉可观测栈/第四生产项目

## Standard Stack

### Core

| Library / Tool | Version | Purpose | Why Standard |
|----------------|---------|---------|--------------|
| Apache Flink | 2.2.1 | Demo 运行时 | 仓库 SSOT / ADR-001；禁止升 2.3 `[VERIFIED: examples/pom.xml + README]` |
| JDK | 21 | 编译运行 | `maven.compiler.release=21`；镜像 `flink:2.2.1-java21` `[VERIFIED: pom + README]` |
| Apache Maven | 3.9.x（本机 3.9.14） | `examples` 全模块 compile | D-07 硬门；CI 已 `mvn -B -T1C -DskipTests compile` `[VERIFIED: 本机 + .github/workflows/ci.yml]` |
| bash + find/grep/wc | macOS/Linux 自带 | qa_check 门禁 | 现有脚本模式；保持可移植 `[VERIFIED: scripts/qa_check.sh]` |
| Python 3.12+ | 本机/CI | 行数/案例诊断、compose YAML 回退、`count_interview.py` 类比 | 标准库即可；CI 另装 PyYAML `[VERIFIED: ci.yml]` |
| Docker / OrbStack | Context: orbstack | compose config + D-09 实测 | arm64 原生 `[VERIFIED: docker info]` |

### Supporting

| Library / Tool | Version | Purpose | When to Use |
|----------------|---------|---------|-------------|
| `flink-connector-datagen` | ${flink.version} | 零依赖无界源 | 新 e12/e0x Demo 默认源 `[VERIFIED: examples/pom.xml]` |
| `common` / `Labs` | 0.1.0 | 事件流脚手架 | 克隆 e12-01 模式 `[VERIFIED: codebase]` |
| `flink-cep` / Broadcast 模式 | 2.2.1 | 护栏/灰度类 Demo | 复用 e12-17/22、e03-C7 `[VERIFIED: existing e12]` |
| PyYAML | CI: pip；本机 6.0.3 | compose 无 docker 时回退 | 保持现有①段 `[VERIFIED: 本机 import]` |
| `scripts/count_interview.py` | 仓库内 | 结构化计数 + exit 码样板 | 文档行数脚本可类比 `[VERIFIED: scripts/count_interview.py]` |
| GitHub Actions `ci.yml` | 已有 | 阈值升级后自动变严 | 勿再引入第二 CI 栈 `[VERIFIED: .github/workflows/ci.yml]` |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| examples mains ≥100 | 计入 projects/ 主类 | **已拒绝（D-01）**——会虚增且偏离现有④段 |
| 文档含 `.planning/` | 刷行数 | **已拒绝（D-04）** |
| 空壳 main / SQL-only 充数 | 快速凑 100 | **违反 D-02**；e12-03/04 SQL **不计** main |
| Agents Preview 新模块进主 pom | 跟章 07/08 | Preview 依赖不稳；新 Demo **必须零依赖进主构建**（07/08 保持 standalone） |
| 裸匹配「略」 | 对齐 PHASES 字面 | **D-08 禁止**；「策略」等会误杀（本机 ≥58 处） |
| 独立 `docs/QA-REPORT.md` | 人读摘要 | Claude's Discretion；**不挡门禁** |

**Installation:**

本 Phase **不引入新 Maven/npm 坐标**。新 Demo 仅依赖父 POM 已管理构件：

```bash
# 新模块克隆 e12-01 骨架后登记到 examples/pom.xml <modules>
# 本机验证（单模块）
cd examples && mvn -pl e12-XX-name -am -DskipTests compile

# 全仓门禁（OrbStack 终验）
bash scripts/qa_check.sh
# 建议并行：
bash scripts/eng_audit.sh   # 或并入 qa_check ⑦ 段
python3 scripts/count_interview.py
```

**Version verification:** Flink/Kafka/CDC/Agents 版本以 README 矩阵与 `examples/pom.xml` 属性区为准（ENG-01 抽样对象）。本机已确认 `flink.version=2.2.1`、`flink.kafka.connector.version=5.0.0-2.2`、`flink.agents.version=0.3.0`。

## Package Legitimacy Audit

> 本 Phase 以 bash/Python 标准库脚本与既有 Maven 坐标为主，**不新增**外部 npm/PyPI 运行时依赖。

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| （无新增） | — | — | — | — | N/A | Approved — 不安装新包 |
| PyYAML（CI 已有） | PyPI | 既有 | — | yaml/pyyaml | 未对本机新装复测 | Keep — CI 既定依赖，非本 Phase 引入 |
| Maven Central Flink 工件 | Maven | 既有 SSOT | — | apache/flink | N/A | Keep — 禁止新 Preview 坐标进主构建 |

**Packages removed due to slopcheck [SLOP] verdict:** none  
**Packages flagged as suspicious [SUS]:** none  

*若后续 executor 坚持新增第三方包，必须先登记版本矩阵并跑 `slopcheck`；当前研究结论是不需要。*

## Architecture Patterns

### System Architecture Diagram

```mermaid
flowchart TB
  subgraph entry [Entry]
    DEV[开发者 / GHA runner]
    QA[scripts/qa_check.sh]
    ENG[scripts/eng_audit.sh]
  end

  subgraph gates [Hard Gates QA-01]
    G1[① compose config / PyYAML]
    G2[② 违禁词 TODO|FIXME|自行实现|请参考官网|省略]
    G3[③ MD 相对断链]
    G4[④ mains ≥100]
    G5[⑤ md lines ≥30000]
    G6[⑥ mvn -DskipTests compile HARD FAIL]
  end

  subgraph content [Content fill QA-02]
    EX[examples/ e12 gaps + e0x]
    DOC[docs/ + ai/ + interview/ + best-practice/]
    SYNC[README + PHASES + CHANGELOG + REQUIREMENTS]
  end

  subgraph eng [ENG-01..04]
    E1[版本矩阵 ↔ pom]
    E2[docs/README 编号]
    E3[违禁词 + verify/baseline 指针]
    E4[CHANGELOG + PHASES 痕迹]
  end

  DEV --> QA --> G1 & G2 & G3 & G4 & G5 & G6
  DEV --> ENG --> E1 & E2 & E3 & E4
  EX --> G4
  EX --> G6
  DOC --> G5
  SYNC --> E4
  QA -->|exit 0 OrbStack| DONE[可打 tag 完成态]
  ENG -->|exit 0| DONE
```

### Recommended Project Structure

```
scripts/
├── qa_check.sh              # 升级：阈值 100、行数门、compile 硬失败、省略入词表
├── eng_audit.sh             # 新建（推荐独立，qa_check 末尾调用）
├── count_interview.py       # 已有；保持
└── count_docs.py            # 可选：打印分目录行数诊断（不挡门禁也可内联 qa_check）

examples/
├── pom.xml                  # <modules> 追加新 e12-*（零依赖）
├── e12-09-…/ … e12-23-…/    # 优先缺口编号；每模块 ≥1 main + README
├── e08-cdc/                 # 补至 ≥4 mains（配额回填）
└── e01|e04|e11…/            # 差额补齐

docs/                        # 各模块 README 实质扩写（最大行数杠杆）
interview/L1–L8.md           # 加厚参考答案（第二杠杆）
best-practice/*.md           # 规范正文加厚 + production 互链
```

### Pattern 1: 零依赖 e12 Demo 模块（推荐模板）

**What:** 每个缺口章对应一个 Maven 子模块 + 单一 `*Job.java` + README 八段要点；`Labs.events(...)` 或 datagen 作源；`uid(...)` 纪律；`env.execute("e12-XX-...")`。  
**When to use:** ai/ 章仅有「代码示意/方法论」且可用 DataStream 表达核心思想时。  
**Example（结构对齐现有作业）:**

```java
// Source: examples/e12-01-polling-vs-event/.../PollingVsEventDrivenJob.java [VERIFIED: codebase]
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
DataStream<Event> events = Labs.events(env, "signals", 30, 5, 8, 200);
events.keyBy(e -> e.userId)
      .process(new SomeFunction())
      .uid("e12-XX-op")
      .print();
env.execute("e12-XX-name");
```

### Pattern 2: qa_check 计数与失败语义

**What:** 诊断 `note` 打印实际值；比较失败走 `bad` → `FAIL=1` → 末尾 `exit 1`。  
**When to use:** 所有新硬门（100 / 30000 / compile）。  
**Example:**

```bash
# Source: scripts/qa_check.sh ④ 段模式 [VERIFIED: codebase]
MAINS=$(grep -rl --include='*.java' 'public static void main' examples | wc -l | tr -d ' ')
note "info  可运行作业数(main 计数)= $MAINS"
[ "$MAINS" -ge 100 ] && ok "案例数 ≥ 100" || bad "案例数不足:$MAINS < 100"

DOC_LINES=$(find . -name '*.md' -not -path './.planning/*' -not -path './.git/*' \
  -print0 | xargs -0 wc -l | tail -1 | awk '{print $1}')
note "info  文档行数(excl .planning/.git)= $DOC_LINES"
[ "$DOC_LINES" -ge 30000 ] && ok "文档行数 ≥ 30000" || bad "文档行数不足:$DOC_LINES < 30000"
```

### Pattern 3: 计数脚本类比 `count_interview.py`

**What:** Python 结构化输出 + `sys.exit(1)`；可被 eng_audit/qa_check 调用。  
**When to use:** 需要分目录诊断（docs vs interview vs ai）时。  
**Source:** `scripts/count_interview.py` `[VERIFIED: codebase]`。

### Anti-Patterns to Avoid

- **空壳 main / 复制粘贴改类名无逻辑：** 违反 D-02；审核时看是否有独立教学断言或可观察输出。
- **把 e12-03/04 SQL「包装」成无逻辑 Java 仅调用 `main` 空转：** 仍属虚增；若做 Java，须实现章内降级路径（如 AsyncIO mock Ollama），并进主 pom。
- **用 `.planning/` 或重复粘贴官网进 md 刷行数：** 违反 D-04/D-05。
- **违禁词裸匹配「略」：** 误杀「策略」等（本机大量命中）。
- **compile 继续 warn 跳过：** 违反 D-07；CI 虽已硬编译，本地只跑 qa_check 时仍会假绿。
- **本 Phase 打 git tag：** 违反 D-12。

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Demo 事件源 | 自写 Kafka 集群依赖作业 | `Labs.events` / datagen | 零依赖可本地 compile+跑 |
| Broadcast/护栏/灰度 | 新造状态机框架 | 克隆 e12-17 / e12-22 / e03-C7 | 已验证模式 |
| Async 外呼类 Demo | 真调 Ollama/LangGraph | e11 风格 mock HTTP / 内存延时 | 主构建不绑外部服务 |
| 文档行数统计 | 手算或含 .planning | find+wc 或 `count_docs.py` | 口径必须与 D-04 一致 |
| 面试题计数 | 新写正则门禁 | 复用 `count_interview.py` | 已有 exit 码契约 |
| 版本对齐检查 | 人工肉眼 | eng_audit 解析 README 表 + pom properties | 可重复 |
| 第二套 CI | 新 workflow 框架 | 扩展现有 `ci.yml` 所调用的 qa_check | 单一门禁源 |

**Key insight:** P6 的复杂度在「计量诚实 + 内容实质」，不在新技术栈；一切新能力应复用 P1–P5 已验证脚手架。

## Common Pitfalls

### Pitfall 1: 「省略」入词表立即红灯
**What goes wrong:** D-08 加入 `省略` 后，现有 3 处合法叙述会 FAIL（e12-06 README/Java、e12-07 README「不可省略」）。  
**Why it happens:** 词表扩展未先清洗存量。  
**How to avoid:** Wave 1 改词表的**同一 commit 或前置 commit** 改写为「未包含 / 必须保留 / 简化掉」等；排除列表**不要**扩大到 examples。  
**Warning signs:** qa_check ② 段一升级就红，且命中教学说明句。

### Pitfall 2: SQL-only / 非聚合模块对 main 无效
**What goes wrong:** 在 e12-03/04 或 e09 加 SQL 脚本，main 计数不变。  
**Why it happens:** ④ 段只 grep Java main；e09 **不在**父 pom modules。  
**How to avoid:** 新案例必须是 `examples/**/*.java` 含 main；进主构建的模块登记 `<modules>`。  
**Warning signs:** 目录增多但 `MAINS` 仍 67。

### Pitfall 3: 离线 / 无 mvn 环境
**What goes wrong:** D-07 无 mvn → hard fail；沙箱无 Central → compile fail。  
**Why it happens:** 旧脚本把失败降为 warn。  
**How to avoid:** 本地 OrbStack 装 JDK21+Maven（已具备）；CI 保持 setup-java + cache；文档写明「合入前必须本机/CI compile」。  
**Warning signs:** `warn  mvn compile` 文案仍在脚本中。

### Pitfall 4: Markdown 断链扫描噪声
**What goes wrong:** 相对链接、`./`、`../`、锚点、仓库根绝对风格路径混用导致假断链；新增 20k 行文档时断链爆炸。  
**Why it happens:** ③ 段用 `dirname(f)/tgt` 拼接，对以 `/` 开头的「根相对」链接不友好。  
**How to avoid:** 新文档统一相对路径（`docs/README.md` 交叉引用规则）；扩写后先跑 qa_check ③ 分段修链；不要在同一波加大量坏链。  
**Warning signs:** `断链 foo → /examples/...` 或 `](docs/...)` 从非根文件出发。

### Pitfall 5: 文档注水 vs 达标
**What goes wrong:** 重复段落、粘贴 changelog、无考点的空表「冲行数」在 review 被打回。  
**Why it happens:** +20k 压力大。  
**How to avoid:** 按下方「行数增益目标表」分配；interview 加厚答案（场景/反例/链教材）；docs 按八段式补全而非复制。  
**Warning signs:** 行数达标但 diff 大量重复块。

### Pitfall 6: CI 双重 compile 耗时
**What goes wrong:** `ci.yml` 已硬编译，qa_check 再编译一次（~数分钟）。  
**Why it happens:** D-07 要求 qa_check 自洽。  
**How to avoid:** 接受双重；或 qa_check 支持 `SKIP_MVN=1` **仅**给 CI 在已编译后跳过——但 **默认本地不得跳过**（Discretion；若做必须文档化）。  
**Warning signs:** CI timeout（当前 45min，一般足够）。

### Pitfall 7: ENG-03 条文与 D-08「略」不一致
**What goes wrong:** REQUIREMENTS/PHASES 仍写禁止「略」，扫描却不匹配裸「略」。  
**Why it happens:** CONTEXT 显式锁定 D-08。  
**How to avoid:** 实现跟 D-08；终稿时可在 PHASES 注「扫描匹配整词省略，不裸匹配略」。以 CONTEXT 为准。  
**Warning signs:** 审计文档争论是否应用 `略` 正则。

## Code Examples

### qa_check.sh ⑥ compile 硬失败（替换现 47–56 行）

```bash
# Source: 升级自 scripts/qa_check.sh ⑤ 段 [CITED: scripts/qa_check.sh]
if ! command -v mvn >/dev/null 2>&1; then
  bad "未检测到 mvn：合入前必须在本机安装 Maven 并通过 examples 编译"
else
  if (cd examples && mvn -q -T1C -DskipTests compile); then
    ok "mvn compile 全模块"
  else
    bad "mvn compile 失败（禁止 warn 跳过；见 D-07）"
  fi
fi
```

### 违禁词（D-08）

```bash
# Source: 升级自 scripts/qa_check.sh ② 段 [CITED: scripts/qa_check.sh]
# 注意：不要写单独的「略」；「策略」会误杀
HITS=$(grep -rn --include='*.md' --include='*.java' --include='*.py' \
        -E '(TODO|FIXME|自行实现|请参考官网|省略)' . \
      | grep -v -E '(\./\.planning/|PHASES\.md|CLAUDE\.md|AGENTS\.md|scripts/README\.md|scripts/qa_check\.sh|docs/README\.md)' || true)
```

### eng_audit.sh 骨架（推荐独立文件）

```bash
#!/usr/bin/env bash
# ENG-01..04 终检 — 失败非 0
set -uo pipefail
cd "$(dirname "$0")/.."
FAIL=0
# ENG-01: README 矩阵关键版本字符串 ⊆ pom properties
# ENG-02: docs/README.md 中 ](...) 相对链接存在；模块 01-14 目录存在
# ENG-03: 抽检 projects/p0{1,2,3}/docs/baseline.md 与 production/docs/bluegreen-timeline.md 存在；再跑违禁词子集
# ENG-04: grep -q '未发布\|Unreleased\|## \[Unreleased\]' CHANGELOG.md；PHASES P6 行非「📋」占位（终态）
```

### 文档行数（与 D-04 一致）

```bash
find . -name '*.md' -not -path './.planning/*' -not -path './.git/*' -print0 \
  | xargs -0 wc -l | tail -1
# 本机 2026-07-19: 9992 [VERIFIED: 本机测量]
```

## 现状盘点（2026-07-19 本机实测）

### 案例 mains 分模块 `[VERIFIED: grep]`

| 模块 | mains | 备注 |
|------|------:|------|
| e01-hello-flink | 3 | |
| e02-time-window | 5 | |
| e03-state | 10 | |
| e04-checkpoint | 5 | C3 含 V1/V2 |
| e05-sql | 10 | |
| e06-table-api | 8 | |
| e07-connectors | 8 | |
| e08-cdc | 1 | 配额叙事曾为 ×4；**优先补 +3** |
| e10-cep | 5 | |
| e11-async-io | 3 | 可 +2～3 |
| e12-01,02,06,07,08,15,17,18,22 | 9×1 | 07/08 standalone 仍计入 grep |
| e12-03,04 | 0 | **仅 SQL**，不计 |
| e09-lakehouse | 0 | **不在父 pom**；SQL only |
| **合计** | **67** | 目标 100 → **缺口 33** |

### e12 编号缺口与推荐 Demo 主题（Discretion 具体清单）

| 编号 | ai 章 | 现状 | 推荐实现（零依赖 / 进主 pom） | 预估 mains |
|------|-------|------|------------------------------|----------:|
| 05 | RAG | 共享 e12-04 SQL | `e12-05-streaming-rag-lite`：Keyed State 文档片段索引 + 规则检索（无 Milvus） | 1 |
| 09 | Tool Call | 示意 | 幂等键 + 副作用日志侧输出（章内降级路径） | 1 |
| 10 | MCP | 有意无独立 Demo | **可选跳过**或 AsyncIO「工具调用」mock（若跳过则从 e0x 补） | 0–1 |
| 11 | Workflow | 示意 | 有限状态机 ProcessFunction | 1 |
| 12 | Multi-Agent | 架构 | 双 keyBy 流 + connect 协作拓扑 | 1 |
| 13 | LangGraph | 骨架 | AsyncIO mock HTTP「外呼」+ 超时降级（复用 e11） | 1 |
| 14 | Knowledge Graph | 方法 | 事件驱动三元组 upsert MapState | 1 |
| 16 | Trace | 骨架 | traceId 注入 metrics/日志字段传播 | 1 |
| 19 | AI Gateway | 架构 | Broadcast 路由表按 modelId 分流 | 1 |
| 20 | Embedding Cache | 骨架 | 本地 LRU MapState + 命中率 metric（e11-C3 扩展） | 1 |
| 21 | Evaluation | 方法 | 窗口化准确率/延迟模拟指标 | 1 |
| 23 | Online Learning | 方法 | 特征样本侧输出到 print/文件（不真训 Ray） | 1 |
| 24 | 参考架构 | Mermaid | **不强制 Java**；行数走文档 | 0 |

**e12 优先批次合计：** 约 **11–12** mains。  
**剩余 ~21：** e08 +3；e11 +3；e01 +2；e04 +2；e02/e10 各 +2；其余可在热门 e12 模块放 **第二 Job**（同模块多 main 合法，且减少 pom 样板）。

### 文档行数分目录 `[VERIFIED: wc]`

| 目录/文件 | 行数 | 扩写优先级 |
|-----------|-----:|------------|
| docs/ | 918 | **P0** — 模块 README 极薄（多 <100 行） |
| ai/chapters + ai/README | ~1928 | **P0** — 多章 58–90 行，可加厚至 200–300 |
| interview/ | 1012 | **P0** — ~4.1 行/题；加厚至 25–40 行/题 |
| best-practice/ | 230 | **P1** — 规范正文过短 |
| production/ | 1939 | P2 — timeline 已 1408；扩 SOP/互链即可 |
| examples/**/*.md | 962 | P1 — 新 Demo README |
| projects/ | 1606 | P2 — 交叉引用与缺口补段 |
| roadmap/ + playground/ + 根 md 等 | ~余量 | P2 |
| **合计 excl .planning** | **9992** | 目标 **30000**（缺口 **~20008**） |

### 文档 +20k 行增益目标表（禁止注水）

| 目标面 | 当前 | 建议目标 | 预估增益 | 实质内容写法 |
|--------|-----:|---------:|---------:|--------------|
| docs/01–11,13–14 | 918 | 11000–13000 | **+10k～12k** | 每模块按八段式补：背景/架构/代码锚点/启动/验证/踩坑/最佳实践/面试题；互链 examples |
| interview L1–L8 | 1012 | 7000–9000 | **+6k～8k** | 每题保留题干；答案扩：定义→机制→反例→与仓库路径对照（已有骨架可扩） |
| ai/chapters | 1837 | 4500–5500 | **+2.5k～3.5k** | 薄章补「踩坑实证」「与 p01/e11 对照」「降级决策树」 |
| best-practice | 230 | 2000–2800 | **+1.8k～2.5k** | 每条规范：为何/怎么做/检查清单/反模式/链 production |
| examples 新 README | 962 | 2000+ | **+1k** | 每新 Demo 80–120 行八段要点 |
| production 非 timeline | ~531 | 1200 | **+0.7k** | operator/gitops/sop 补排障树 |
| **合计** | | | **~22k–28k** | 留余量应对断链返工 |

## qa_check.sh 逐段现状 → 升级映射 `[VERIFIED: 逐行阅读 scripts/qa_check.sh]`

| 段 | 现状（行号） | Phase 7 目标 |
|----|--------------|--------------|
| 头注释 1–4 | 五段描述；compile「有 mvn 且可联网时」 | 改为六硬门描述 |
| `set -uo pipefail` 5–10 | FAIL/note/bad/ok | 保持 |
| ① 12–19 | docker compose config，否则 PyYAML | **保持硬失败** |
| ② 21–25 | `TODO\|FIXME\|自行实现\|请参考官网` | **加入 `省略`**；不裸匹配略；排除集保持 |
| ③ 27–40 | 相对断链；排除 .planning | 保持；扩文档后必回归 |
| ④ 42–45 | mains ≥ **67** | 改为 ≥ **100**（D-03） |
| （新）⑤ | 无 | **文档行数 ≥30000**（D-06） |
| 旧⑤→新⑥ 47–56 | compile 失败仅 warn；无 mvn 跳过 | **hard fail**（D-07） |
| 尾 58 | PASS/FAIL | 可选：调用 `eng_audit.sh` |

**CI 影响：** `.github/workflows/ci.yml` 已先 `mvn compile` 再 `qa_check.sh`；阈值升级后案例/行数不足会直接红 CI——符合预期。Planner 应在 Wave 1 说明「先红是故意的」（D-13）。

## ENG 审计脚本设计（ENG-01…04）

**推荐：** 独立 `scripts/eng_audit.sh`，由 `qa_check.sh` 末尾 `bash scripts/eng_audit.sh || bad "eng_audit"` 调用（单一入口）；亦允许单独跑。

| 检查 | 断言（可自动化） | 失败语义 |
|------|------------------|----------|
| ENG-01 | 从 README 版本矩阵抽取 `2.2.1`、`5.0.0-2.2`、`3.6.0`、`1.15.0`、`0.3.0` 等关键值；断言出现在 `examples/pom.xml` `<properties>` 或 README 已声明「仅文档」的 Operator chart（Operator 不在 examples pom——审计应允许 README-only 组件白名单：`Operator Helm chart`、`Milvus`、`Ollama`） | FAIL |
| ENG-02 | `docs/README.md` 存在；`docs/0{0-9}-*` / `13` / `14` 目录存在；模块 15 三项目 README 链接可解析 | FAIL |
| ENG-03 | （1）复用违禁词扫描或依赖 qa_check ②；（2）硬存在：`projects/p01.../docs/baseline.md`、`p02.../baseline.md`、`p03.../baseline.md`、`benchmark/baseline.md`、`production/docs/bluegreen-timeline.md` | FAIL |
| ENG-04 | `CHANGELOG.md` 含未发布区标题；本 Phase 终态要求 PHASES P6 状态含可验证完成表述（非单独 `📋`）——**仅在 Wave 4 启用严格断言**，Wave 1 可只检查 CHANGELOG 区存在以免先红死锁 | Wave 1 soft / Wave 4 hard |

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| 案例阈值 67（P3） | 100（P6） | Phase 7 | 需 +33 Demo |
| 无文档行数门 | ≥30000 | Phase 7 | 需 +~20k 实质文档 |
| mvn warn 可忽略 | hard fail | Phase 7 / D-07 | 与 CI 对齐 |
| 违禁词无「省略」 | 含「省略」、无裸「略」 | D-08 | 先清洗 3 处存量 |
| ENG 靠人工纪律 | 脚本化终检 | D-10 | Phase 完成门槛 |

**Deprecated/outdated:**
- qa_check「离线可忽略 compile」路径 — 删除
- 以 README C 标签或 projects 主类充案例 — 已拒绝

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | e12-10 可跳过、用 e0x 补 mains 仍满足 D-02「优先 e12」精神 | 案例清单 | 若用户强制每章一模块，需再 +1 模块 |
| A2 | CI 双重 mvn 可接受；可不做 SKIP_MVN | Pitfalls | CI 变慢；一般 <45min |
| A3 | ENG-01 白名单允许 Operator/Milvus/Ollama 仅在 README | ENG audit | 过严会导致误红 |
| A4 | interview 加厚至 ~30 行/题不违反「已有完整答案」质量观 | 文档表 | 若认为已够厚，则 docs/ai 需承担更多行数 |
| A5 | 新 Demo 不登记新 Maven 坐标即可完成 | Standard Stack | 若某 Demo 强依赖新连接器，必须先改矩阵（ENG-01） |

## Open Questions (RESOLVED)

1. **ENG 并入 qa_check 还是独立？** (RESOLVED)
   - What we know: D-10 允许二者；独立更易单测与 Wave 1 骨架。
   - Recommendation: **独立 `eng_audit.sh` + qa_check 调用**（Discretion 默认）。
   - RESOLVED: 采用独立 `scripts/eng_audit.sh`，由 `qa_check.sh` 末尾调用（见 07-00-PLAN）。

2. **是否产出 `docs/QA-REPORT.md`？** (RESOLVED)
   - Recommendation: **可选**；若做，用一次 `qa_check`/`eng_audit` 输出粘贴 + 计量表；不挡门禁。
   - RESOLVED: 可选；07-03 Discretion 允许生成，须先在 `docs/README.md` 登记；不挡门禁。

3. **e12-03/04 是否补 Java 降级作业？** (RESOLVED)
   - What we know: SQL 不计 main；章内已写 AsyncIO 降级。
   - Recommendation: **各加 1 个 mock 降级 Job 进主构建**可同时抬 mains 与学习路径；非必须若 e12 缺口批已满 33。
   - RESOLVED: 非必须；07-01 以 e12 缺口批 + e0x 回填凑满 ≥100；e12-03/04 Java 降级仅在缺口不足时再补。

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| JDK 21 | mvn compile | ✓ | 21.0.2 | — |
| Maven | D-07 | ✓ | 3.9.14 | — |
| Docker / OrbStack | ① compose + D-09 | ✓ | 29.4.0 / orbstack | PyYAML 仅解析，不替代 D-09 实测 |
| Python 3 | 行数/YAML/count_* | ✓ | （pyenv） | — |
| PyYAML | compose 回退 | ✓ | 6.0.3 | CI pip install |
| uname arch | D-09 | ✓ | arm64 | — |
| kubectl/Helm | — | 不需要 | — | 本 Phase 不演练 Operator |
| 知识图谱 `.planning/graphs` | — | ✗ | — | 跳过 graphify |

**Missing dependencies with no fallback:** none（本机已具备终验条件）  
**Missing dependencies with fallback:** compose 无 docker 时用 PyYAML（CI）；**不能**替代 OrbStack 全绿要求。

## Validation Architecture

> `workflow.nyquist_validation: true`（`.planning/config.json`）— 本节供后续 VALIDATION.md 生成。

### Test Framework

| Property | Value |
|----------|-------|
| Framework | 仓库门禁脚本（bash）+ 可选 Python 计数；**无** JUnit 测试源（`examples/**/*Test.java` = 0） |
| Config file | `scripts/qa_check.sh` + 建议 `scripts/eng_audit.sh` |
| Quick run command | `python3 -c` 行数/mains 快检 **或** `bash -c` 仅 ②④⑤ 段 |
| Full suite command | `bash scripts/qa_check.sh`（含 mvn）；另 `bash scripts/eng_audit.sh`；`python3 scripts/count_interview.py` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| QA-01 | 六硬门全绿 | smoke / gate | `bash scripts/qa_check.sh` | ✅ 脚本在；须升级 |
| QA-02 案例 | mains ≥100 | gate | qa_check ④；`grep -rl 'public static void main' examples \| wc -l` | ✅ 逻辑在；阈值待改 |
| QA-02 文档 | lines ≥30000 | gate | qa_check 新⑤ | ❌ Wave 0 加检查 |
| QA-02 状态 | README/PHASES 一致 | audit | eng_audit ENG-04 + 人工读 | ❌ Wave 0 eng_audit |
| ENG-01 | 矩阵↔pom | audit | `bash scripts/eng_audit.sh` | ❌ Wave 0 |
| ENG-02 | docs 编号登记 | audit | eng_audit ENG-02 | ❌ Wave 0 |
| ENG-03 | 无违禁词 + 证据指针 | gate+audit | qa_check ② + eng_audit ENG-03 | ⚠️ ② 在；ENG-03 指针检查待加 |
| ENG-04 | CHANGELOG/PHASES | audit | eng_audit ENG-04 | ❌ Wave 0 |
| D-09 | OrbStack 实测 | manual-only | 本机 `bash scripts/qa_check.sh`；CI 不替代 | 手测清单 |

### Sampling Rate

- **Per task commit:** 快检 mains 与（若已实现）doc lines；相关模块 `mvn -pl … -am -DskipTests compile`
- **Per wave merge:** `bash scripts/qa_check.sh`（Wave 1 预期红；Wave 2 起 compile+案例；Wave 3 行数；Wave 4 全绿）
- **Phase gate:** OrbStack 上 qa_check + eng_audit 双绿；再勾 REQUIREMENTS

### Wave 0 Gaps

- [ ] 升级 `scripts/qa_check.sh`：阈值 100、行数门、compile 硬失败、省略词表（先红）
- [ ] 新增 `scripts/eng_audit.sh`（或等价）覆盖 ENG-01…04
- [ ] 可选 `scripts/count_docs.py` 分目录诊断（类比 count_interview）
- [ ] 更新 `scripts/README.md` 索引
- [ ] 清洗存量「省略」三处，避免 ② 段误杀
- [ ] 无 JUnit 框架需求 — **不**为 P6 强行引入 Testcontainers 替代 OrbStack

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|------------------|
| V2 Authentication | no | 无用户认证面 |
| V3 Session Management | no | — |
| V4 Access Control | no | 本地学习工程 |
| V5 Input Validation | yes | 链接扫描路径拼接；grep 模式固定；避免对用户输入 `eval` |
| V6 Cryptography | no | — |

### Known Threat Patterns for shell QA gates

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| 门禁脚本被排除列表过宽绕过违禁词 | Tampering | 排除集冻结为 D-08 列表；PR review |
| 断链扫描路径穿越 / 意外读敏感文件 | Information Disclosure | 仅检查路径存在性；不读文件内容 |
| 依赖混淆（若新增包） | Tampering | 本 Phase 不新增包；ENG-01 先登记 |
| CI 假绿掩盖本机失败 | Spoofing | D-09 强制 OrbStack；CI 不宣称替代 |

## Sources

### Primary (HIGH confidence)

- `scripts/qa_check.sh` — 逐行分析（①–⑤ 段行为）
- `scripts/count_interview.py` — 计数门禁样板
- `examples/pom.xml` — modules / 版本属性 SSOT
- `docs/README.md` — 编号登记 SSOT
- `README.md` — 版本矩阵
- `ai/README.md` — e12 章↔Demo 映射与缺口
- 本机测量：mains=67、md lines=9992、arm64、mvn/docker 可用
- `.github/workflows/ci.yml` — CI 已硬编译 + qa_check
- `.planning/phases/07-p6-qa/07-CONTEXT.md` — D-01…D-13
- `.planning/REQUIREMENTS.md` — QA-01/02、ENG-01…04
- `.planning/config.json` — `nyquist_validation: true`

### Secondary (MEDIUM confidence)

- PHASES.md P6 行 / ENG「略」字面 vs D-08 实现差异（以 CONTEXT 为准）
- 文档增益表中的目标行数为规划估算（执行时以 `wc` 为准）

### Tertiary (LOW confidence)

- e12-10 是否实现：章内曾论证可无独立 Demo — 标为可选 `[ASSUMED: A1]`

## Metadata

**Confidence breakdown:**
- Standard stack: **HIGH** — 复用仓库 SSOT，无新坐标
- Architecture: **HIGH** — 门禁/扩容路径清晰，与 D-13 一致
- Pitfalls: **HIGH** — 「省略」误杀、SQL 不计 main、断链噪声均本机验证
- 文档行数分配: **MEDIUM** — 增益表为估算，需执行期 `wc` 校准

**Research date:** 2026-07-19  
**Valid until:** 2026-08-18（门禁脚本稳定；Demo 清单可微调）
