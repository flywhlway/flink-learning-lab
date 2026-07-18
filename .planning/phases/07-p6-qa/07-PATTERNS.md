# Phase 7: P6 总装 QA - Pattern Map

**Mapped:** 2026-07-19
**Files analyzed:** 18（脚本/门禁 5 + Demo 骨架 4 + 文档扩写面 5 + 状态终稿 4）
**Analogs found:** 18 / 18

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `scripts/qa_check.sh`（升级） | utility / gate | batch | `scripts/qa_check.sh`（自身演进） | exact |
| `scripts/eng_audit.sh`（新建） | utility / gate | batch | `scripts/qa_check.sh` + `scripts/count_interview.py` | role-match |
| `scripts/count_docs.py`（可选） | utility | batch / file-I/O | `scripts/count_interview.py` | exact |
| `scripts/README.md`（索引更新） | config / docs | file-I/O | `scripts/README.md` | exact |
| `examples/e12-XX-*/` 新模块（≥11） | service / demo | streaming | `examples/e12-01-polling-vs-event/` | exact |
| `examples/e12-XX-*/pom.xml` | config | batch | `examples/e12-01-polling-vs-event/pom.xml` | exact |
| `examples/e12-XX-*/README.md` | docs | file-I/O | `examples/e12-01-polling-vs-event/README.md` | exact |
| `examples/pom.xml` `<modules>` | config | batch | `examples/pom.xml`（e12 登记段） | exact |
| `examples/README.md`（e12 表） | docs | file-I/O | `examples/README.md` e12 行 | exact |
| `examples/e08-cdc/` 等补 main | service / demo | streaming / event-driven | `examples/e01-hello-flink/HelloEventTimeWindowJob.java` | role-match |
| Async/外呼类 e12（13/09 等） | service / demo | streaming | `examples/e11-async-io/C2TimeoutRetryJob.java` + `FakeDimClient` | exact |
| Broadcast/路由类 e12（19/17 型） | service / demo | streaming / pub-sub | `examples/e12-17-streaming-guardrail/StreamingGuardrailJob.java` | exact |
| 窗口/指标类 e12（18/21 型） | service / demo | streaming | `examples/e12-18-streaming-cost-control/StreamingCostControlJob.java` | exact |
| `interview/L1–L8.md` 加厚 | docs | file-I/O | `interview/L1.md` 题干+参考答案块 | exact |
| `docs/*/README.md` 扩写 | docs | file-I/O | `docs/01-runtime/README.md` + `docs/README.md` | exact |
| `ai/chapters/*.md` 加厚 | docs | file-I/O | `ai/chapters/09-streaming-tool-call.md` | exact |
| `best-practice/*.md` 扩写 | docs | file-I/O | `best-practice/02-uid-savepoint.md` | exact |
| `PHASES.md` / `README.md` / `CHANGELOG.md` / `.planning/PROJECT.md` | docs / config | file-I/O | `PHASES.md` P5 完成态行 + `CHANGELOG.md` Unreleased | exact |

## Pattern Assignments

### `scripts/qa_check.sh`（utility/gate, batch）— 升级

**Analog:** `scripts/qa_check.sh`（就地演进；勿另起第二套门禁）

**Shell 骨架**（lines 1–10）:
```bash
#!/usr/bin/env bash
# 仓库 QA 自检(PHASES.md 约定的验收工具)。用法:bash scripts/qa_check.sh
set -uo pipefail
cd "$(dirname "$0")/.."
FAIL=0
note() { printf '%s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; FAIL=1; }
ok()   { printf 'ok    %s\n' "$*"; }
```

**① compose 硬失败**（lines 12–19）— 保持:
```bash
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  (cd docker && docker compose config -q) && ok "docker compose config" || bad "docker compose config"
else
  python3 - << 'PY' && ok "compose yaml parse (python fallback)" || { bad "compose yaml parse"; }
import yaml; yaml.safe_load(open('docker/docker-compose.yml'))
PY
fi
```

**② 违禁词**（lines 21–25）— 升级词表（D-08），排除集不变:
```bash
# 加入「省略」；禁止裸匹配「略」
HITS=$(grep -rn --include='*.md' --include='*.java' --include='*.py' \
        -E '(TODO|FIXME|自行实现|请参考官网|省略)' . \
      | grep -v -E '(\./\.planning/|PHASES\.md|CLAUDE\.md|AGENTS\.md|scripts/README\.md|scripts/qa_check\.sh|docs/README\.md)' || true)
if [ -n "$HITS" ]; then bad "违禁词:"; printf '%s\n' "$HITS"; else ok "违禁词扫描"; fi
```

**③ 断链**（lines 27–40）— 保持相对路径拼接；扩文档后必回归。

**④ 案例阈值**（lines 42–45）— 67 → 100:
```bash
MAINS=$(grep -rl --include='*.java' 'public static void main' examples | wc -l | tr -d ' ')
note "info  可运行作业数(main 计数)= $MAINS"
[ "$MAINS" -ge 100 ] && ok "案例数 ≥ 100" || bad "案例数不足:$MAINS < 100"
```

**⑤ 文档行数（新段，D-04/D-06）:**
```bash
DOC_LINES=$(find . -name '*.md' -not -path './.planning/*' -not -path './.git/*' \
  -print0 | xargs -0 wc -l | tail -1 | awk '{print $1}')
note "info  文档行数(excl .planning/.git)= $DOC_LINES"
[ "$DOC_LINES" -ge 30000 ] && ok "文档行数 ≥ 30000" || bad "文档行数不足:$DOC_LINES < 30000"
```

**⑥ compile 硬失败**（替换 lines 47–56 warn 路径）:
```bash
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

**尾部**（line 58）— 可选调用 eng_audit:
```bash
bash scripts/eng_audit.sh || bad "eng_audit"
if [ "$FAIL" -eq 0 ]; then note "== QA PASS =="; else note "== QA FAIL =="; exit 1; fi
```

---

### `scripts/eng_audit.sh`（utility/gate, batch）— 新建

**Analog:** `scripts/qa_check.sh`（FAIL/note/bad/ok）+ `scripts/count_interview.py`（结构化诊断 + exit 码）

**Core pattern — 复用 qa_check 退出语义:**
```bash
#!/usr/bin/env bash
# ENG-01..04 终检 — 失败非 0
set -uo pipefail
cd "$(dirname "$0")/.."
FAIL=0
note() { printf '%s\n' "$*"; }
bad()  { printf 'FAIL  %s\n' "$*"; FAIL=1; }
ok()   { printf 'ok    %s\n' "$*"; }

# ENG-01: README 矩阵关键版本 ⊆ examples/pom.xml <properties>
#   抽样: 2.2.1 / 5.0.0-2.2 / 3.6.0 / 0.3.0；README-only 白名单: Operator Helm / Milvus / Ollama
# ENG-02: docs/README.md 存在；docs/0{0-9}-* / 13 / 14 目录存在；模块 15 三项目链接可解析
# ENG-03: baseline 硬存在 paths +（可依赖 qa_check ②）
# ENG-04: CHANGELOG 含 Unreleased/未发布；Wave 4 再断言 PHASES P6 非单独 📋

[ "$FAIL" -eq 0 ] && note "== ENG AUDIT PASS ==" || { note "== ENG AUDIT FAIL =="; exit 1; }
```

**ENG-03 证据指针清单（硬存在断言）:**
- `projects/p01-log-ai-platform/docs/baseline.md`
- `projects/p02-realtime-reco/docs/baseline.md`
- `projects/p03-vehicle-monitoring/docs/baseline.md`
- `benchmark/baseline.md`
- `production/docs/bluegreen-timeline.md`

**ENG-01 版本 SSOT 对照源:**
- `README.md` lines 17–43（版本矩阵表）
- `examples/pom.xml` lines 34–52（`<properties>`）

---

### `scripts/count_docs.py`（utility, batch）— 可选

**Analog:** `scripts/count_interview.py`

**Imports + 门禁契约**（lines 1–16, 36–52）:
```python
#!/usr/bin/env python3
"""统计仓库 *.md 行数（排除 .planning/ 与 .git/）（QA-02 / D-04）。"""
from __future__ import annotations
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MIN_LINES = 30000

def main() -> int:
    # 分目录诊断打印；合计 < MIN_LINES → exit 1
    ...
    if total < MIN_LINES:
        print(f"FAIL: doc_lines {total} < {MIN_LINES}", file=sys.stderr)
        return 1
    print(f"ok doc_lines={total}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
```

**Copy from count_interview:**
- `ROOT = Path(__file__).resolve().parent.parent`
- 打印诊断行 + `sys.exit(1)` 失败契约
- 分文件/分目录明细便于 Wave 3 定位缺口

---

### `scripts/README.md`（docs）— 索引更新

**Analog:** `scripts/README.md` 表格行模式（lines 5–10）

新增行示例:
```markdown
| `qa_check.sh` | 六硬门：compose / 违禁词 / 断链 / mains≥100 / md≥30k / mvn compile | `bash scripts/qa_check.sh` |
| `eng_audit.sh` | ENG-01…04 终检 | `bash scripts/eng_audit.sh` |
| `count_interview.py` | interview 题量 ≥150 | `python3 scripts/count_interview.py` |
| `count_docs.py` | 文档行数诊断（可选） | `python3 scripts/count_docs.py` |
```

---

### `examples/e12-XX-*` 新模块（service/demo, streaming）

**Analog（首选）:** `examples/e12-01-polling-vs-event/`

**pom 模板**（`e12-01-polling-vs-event/pom.xml` 全文）:
```xml
<parent>
  <groupId>com.flywhl.flinklab</groupId>
  <artifactId>flink-learning-lab-examples</artifactId>
  <version>0.1.0</version>
</parent>
<artifactId>e12-01-polling-vs-event</artifactId>
<dependencies>
  <dependency><groupId>com.flywhl.flinklab</groupId><artifactId>common</artifactId></dependency>
  <dependency><groupId>org.apache.flink</groupId><artifactId>flink-streaming-java</artifactId></dependency>
  <dependency><groupId>org.apache.flink</groupId><artifactId>flink-clients</artifactId></dependency>
  <dependency><groupId>org.apache.flink</groupId><artifactId>flink-connector-datagen</artifactId></dependency>
  <dependency><groupId>org.apache.flink</groupId><artifactId>flink-connector-base</artifactId></dependency>
  <dependency><groupId>org.slf4j</groupId><artifactId>slf4j-api</artifactId></dependency>
</dependencies>
```
**禁止：** 新 Preview Agents 坐标进主 pom（e12-07/08 保持 standalone）。

**Job 脚手架**（`PollingVsEventDrivenJob.java` lines 37–52）:
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(1);
DataStream<Event> events = Labs.events(env, "signals", 30, 5, 8, 200);
events.keyBy(e -> e.userId)
      .process(new SomeFunction())
      .uid("e12-XX-op")
      .print();
env.execute("e12-XX-name");
```

**约定（从 e12-01 / e12-17 / e12-18 提取）:**
- `public final class …Job` + private ctor
- 包名 `com.flywhl.flinklab.e12`
- 每个有状态/关键算子显式 `.uid("e12-XX-…")`
- Javadoc 写清对应 ai 章、运行命令、可观察输出
- 零外部服务：`Labs.events` 或 `DataGeneratorSource`（见 e12-17/18）

**README 要点**（`e12-01-polling-vs-event/README.md`）:
```markdown
# e12-XX · 标题
> 对应 [ai/chapters/…] · Level:…
> 运行:`mvn -q -Plocal compile exec:java -pl e12-XX-… -Dexec.mainClass=…`
## 背景
## 验证方式
## 源码要点
## 面试题
```
目标 80–120 行八段要点（可扩踩坑/最佳实践），禁止空壳。

**父 pom 登记**（`examples/pom.xml` lines 25–31）:
```xml
<module>e12-01-polling-vs-event</module>
<!-- 追加新零依赖模块；勿把 e12-07/08 standalone 强行并入若依赖 Preview -->
```

**examples/README 表**（lines 25–35）— 每新模块加一行，状态 ✅ Phase 7。

---

### Broadcast / 路由类 Demo（e12-19 等）

**Analog:** `examples/e12-17-streaming-guardrail/StreamingGuardrailJob.java`

**Core**（lines 52–80）:
```java
env.setParallelism(1);
DataStream<String> llmOutputs = env.fromSource(
    new DataGeneratorSource<>(…), WatermarkStrategy.noWatermarks(), "llm-output-source");
BroadcastStream<GuardrailRule> rules = env.fromSource(…)
    .broadcast(RULES_DESC);
llmOutputs.keyBy(s -> "single-key")
          .connect(rules)
          .process(new GuardrailProcessFn())
          .uid("e12-17-guardrail");
```
适用：AI Gateway 路由表、护栏热更、灰度规则。

---

### Async / 外呼 mock 类 Demo（e12-13 / e12-09 降级）

**Analog:** `examples/e11-async-io/C2TimeoutRetryJob.java` + `FakeDimClient.java`

**Core**（C2 lines 34–45）:
```java
AsyncDataStream.unorderedWaitWithRetry(
    Labs.events(env, "clicks", 30, 5, 10, 500),
    new FlakyEnrich(), 3, TimeUnit.SECONDS, 100, retryStrategy)
```
**Fake 客户端**（FakeDimClient lines 23–38）: `CompletableFuture` + 可控 latency/failRatio；禁止真调 Ollama/LangGraph。

---

### 窗口 / 成本 / Evaluation 类 Demo（e12-18 / e12-21）

**Analog:** `examples/e12-18-streaming-cost-control/StreamingCostControlJob.java`

**Core**（lines 30–38 起）: datagen 模拟 token/指标 → `TumblingEventTimeWindows` 聚合 → 阈值熔断输出。复制「模拟外部系统、零依赖」叙事。

---

### e0x / e08 配额补 main（同模块多 Job）

**Analog:** `examples/e01-hello-flink/HelloEventTimeWindowJob.java`（同模块追加 Job）

**Core**（lines 31–38）:
```java
public final class HelloEventTimeWindowJob {
    private HelloEventTimeWindowJob() {}
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // …
        env.execute("e01-j1-…");
    }
}
```
**注意：** e08 现有 `C4PostgresCdcDataStreamJob` 绑 Postgres——新增配额 Job 应优先零依赖（datagen / Labs），避免把 CDC 外部依赖扩散到「凑数」路径。同模块多 main 合法且计入 ④ 段 grep。

---

### `interview/L*.md` 加厚（docs）

**Analog:** `interview/L1.md` + `interview/README.md`

**题块结构**（L1 lines 5–12）:
```markdown
1. 题干？

**参考答案：** 答案：…
1) …
考点推导：… 见 docs/…
```

**加厚目标（D-05）：** 每题扩至约 25–40 行：定义 → 机制 → 反例 → 仓库路径对照。  
**门禁保持：** `python3 scripts/count_interview.py`（题量已 ≥150，本 Phase 抬行数不增题亦可）。

---

### `docs/*/README.md` 扩写（docs）

**Analog:** `docs/01-runtime/README.md`（实质章节体）+ `docs/README.md`（编号 SSOT）

**docs/README 登记纪律**（lines 1–4, 81–84）:
```markdown
> 本文件是全仓库章节编号、命名与交叉引用的**唯一事实来源**。任何新增文档必须先在此登记编号。
### 交叉引用规则
- 文档引用示例统一写相对路径 `../../examples/eNN-.../README.md#锚点`
```

**模块正文结构**（`01-runtime/README.md`）: 章节标题 → mermaid/表 → 决策线 → 面试陷阱 → 链 examples。  
扩写优先按八段式补全偏薄模块（背景/架构/代码锚点/启动/验证/踩坑/最佳实践/面试题），互链 examples；**禁止** `.planning/` 计入行数。

---

### `ai/chapters/*.md` 加厚（docs）

**Analog:** `ai/chapters/09-streaming-tool-call.md`

**结构**（lines 1–25）: 章题 + Demo 指针 → 问题陈述 → mermaid → 机制正文 → 代码示例 → 降级路径。  
薄章补：踩坑实证、与 p01/e11 对照、降级决策树（增益表 +2.5k～3.5k）。

---

### `best-practice/*.md` 扩写（docs）

**Analog:** `best-practice/02-uid-savepoint.md`

**结构**（全文）:
```markdown
## 规则
## 理由
## 反例
## 落地互链
```
扩写时加：为何 / 怎么做 / 检查清单 / 反模式 / 链 `production/docs/*`（已有互链模式 lines 19–22）。

---

### 状态终稿：`PHASES.md` / `README.md` / `CHANGELOG.md` / PROJECT

**PHASES P6 行**（line 16）— 终态对齐 P5 完成态表述风格（line 15）:
```markdown
| **P5 生产化** | … | ✅ **可验证完成态**（PROD-01–04）：… |
| **P6 总装 QA** | … | ✅ **可验证完成态**（QA-01/02 + ENG-01…04）：qa_check 全绿；mains≥100；md≥30k；… |
```
（仅在 OrbStack `qa_check`+`eng_audit` 双绿后改；Wave 1 勿提前勾 ✅）

**CHANGELOG**（lines 5–14）— Unreleased Added 块追加 P6 条目，风格同 P5:
```markdown
## [Unreleased]
### Added
- **P6 总装 QA（QA-01/02 / ENG-01–04）**：qa_check 六硬门；案例≥100；文档≥30k；eng_audit；…
```

**README 版本矩阵**（lines 17–43）— ENG-01 抽样源；本 Phase 原则上不新增坐标，仅完成态表述对齐。

---

## Shared Patterns

### 门禁失败语义（FAIL=1 → exit 1）
**Source:** `scripts/qa_check.sh` lines 7–10, 58  
**Apply to:** `qa_check.sh`、`eng_audit.sh`、`count_docs.py`、`count_interview.py`  
```bash
bad()  { printf 'FAIL  %s\n' "$*"; FAIL=1; }
# …
if [ "$FAIL" -eq 0 ]; then note "== QA PASS =="; else note "== QA FAIL =="; exit 1; fi
```

### 排除 `.planning/` 出产品门禁
**Source:** `scripts/qa_check.sh` lines 21–24, 37–38  
**Apply to:** 违禁词、断链、文档行数（D-04）  
规划稿不得刷案例/行数/违禁词扫描结果。

### 零依赖 Demo 依赖集
**Source:** `examples/e12-01-polling-vs-event/pom.xml` + `Labs.events`  
**Apply to:** 所有进主构建的新 e12 / e0x Job  
仅 `common` + `flink-streaming-java` + `flink-clients` + `flink-connector-datagen` + `flink-connector-base` + `slf4j-api`。

### uid 纪律
**Source:** e12-01 `.uid("e12-01-…")`；`best-practice/02-uid-savepoint.md`  
**Apply to:** 每个新 Job 的有状态/关键算子。

### 文档交叉引用相对路径
**Source:** `docs/README.md` lines 81–84  
**Apply to:** 所有新增 md，降低 qa_check ③ 断链噪声。

### 会话收尾三联
**Source:** `PHASES.md` 接力协议 #2；`CHANGELOG.md` Unreleased  
**Apply to:** Phase 结束 commit：CHANGELOG 未发布区 + PHASES 状态列 +（可选）docs/README 登记。

### CI 单一门禁源
**Source:** `.github/workflows/ci.yml`（`bash scripts/qa_check.sh`）  
**Apply to:** 阈值升级后自动变严；勿新建第二 workflow。

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| — | — | — | 本 Phase 全部交付物均有仓库内可复用样板；无「从零发明」文件 |

*注：`docs/QA-REPORT.md` 为人读可选摘要（Discretion），若产出可类比 `benchmark/baseline.md` 的「命令输出 + 计量表」叙事，非硬门禁。*

## Metadata

**Analog search scope:** `scripts/`、`examples/e12-*`、`examples/e01-*`、`examples/e08-*`、`examples/e11-*`、`examples/pom.xml`、`interview/`、`docs/`、`ai/chapters/`、`best-practice/`、`PHASES.md`、`CHANGELOG.md`、`README.md`、`.github/workflows/ci.yml`  
**Files scanned:** ~45（含 glob 命中与定向 Read）  
**Strong analogs capped:** 5 主样板（qa_check、count_interview、e12-01、e12-17、L1/docs-01）+ 角色匹配扩展  
**Pattern extraction date:** 2026-07-19
