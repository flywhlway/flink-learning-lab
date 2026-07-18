# Phase 6: P5 生产化 - Pattern Map

**Mapped:** 2026-07-18
**Files analyzed:** 28
**Analogs found:** 22 / 28

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `benchmark/scripts/run_matrix.sh` | utility | batch / streaming | `projects/p03-vehicle-monitoring/scripts/loadtest.sh` | exact |
| `benchmark/scripts/collect_metrics.py` | utility | request-response | `loadtest.sh` `prom_query` / `prom_scalar_*` (L22–66, L135–159) | role-match |
| `benchmark/baseline.md` | config | file-I/O | `projects/p03-vehicle-monitoring/docs/baseline.md` | exact |
| `benchmark/Makefile` | config | batch | `projects/p03-vehicle-monitoring/Makefile` | exact |
| `benchmark/README.md` | config | file-I/O | `benchmark/README.md`（回填裁剪轴）+ p03 baseline 互链句式 | role-match |
| `production/scripts/run-bluegreen-drill.sh` | utility | event-driven | `projects/p03-vehicle-monitoring/scripts/drill_watermark_stall.sh` | role-match |
| `production/scripts/install-operator.sh` | utility | request-response | `docker/init.sh` + RESEARCH Helm 片段 | partial |
| `production/scripts/verify-argocd-sync.sh` | utility | request-response | `projects/p03-vehicle-monitoring/scripts/verify.sh` | role-match |
| `production/docs/bluegreen-timeline.md` | config | file-I/O | drill 脚本追加证据 + loadtest 写 baseline 模板 | role-match |
| `production/docs/operator-install.md` | config | file-I/O | `production/README.md` 蓝图 + `projects/p01/.../docs/DEGRADE-CHECKLIST.md` 清单体 | role-match |
| `production/docs/bluegreen-sop.md` | config | file-I/O | `projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md` | role-match |
| `production/docs/gitops-cicd.md` | config | file-I/O | `production/README.md` §CI/CD 范围句 | partial |
| `production/charts/p03-vehicle-alert/**` | config | event-driven | RESEARCH Pattern 2 YAML（仓库无既有 chart） | no-analog → RESEARCH |
| `production/argocd/application-p03.yaml` | config | event-driven | RESEARCH Argo Application 示意 | no-analog → RESEARCH |
| `production/README.md` | config | file-I/O | 现有蓝图 → 回填为可复现总入口（对齐 p03 README 八段纪律） | role-match |
| `monitoring/platform-overview.json` | config | request-response | `projects/p03-vehicle-monitoring/monitoring/dashboards/p03-vehicle-overview.json` | exact |
| `monitoring/job-deepdive.json` | config | request-response | 同上（Prom 五指标面板） | exact |
| `monitoring/ai-cost.json` | config | request-response | p03 dashboard JSON 结构 + p01 `guardrail_blocks` / BudgetGate 指标 | role-match |
| `docker/config/grafana/provisioning/dashboards/dashboards.yml` | config | file-I/O | 现有 `dashboards.yml`（p03 provider） | exact |
| `docker/docker-compose.yml`（grafana volumes） | config | file-I/O | 现有 p03 dashboards volume 挂载 | exact |
| `monitoring/README.md` | config | file-I/O | 现有值班五指标表 | role-match |
| `best-practice/*.md` | config | file-I/O | `best-practice/README.md`（12 条军规体） | exact |
| `interview/**` | config | file-I/O | `interview/README.md`（L1–L8+ 分层 + 教材锚点） | exact |
| `scripts/count_interview.py` | utility | file-I/O | `scripts/qa_check.sh` 案例计数段 | role-match |
| `.github/workflows/ci.yml` | config | request-response | `scripts/qa_check.sh`（门禁语义；无 GHA 样板） | no-analog → RESEARCH |
| `docs/README.md`（模块 13/14） | config | file-I/O | `docs/README.md` 模块 11/12/15 完成态登记句式 | exact |
| `docs/13-performance/` / `docs/14-production/` | config | file-I/O | `docs/11-ecosystem/README.md` 模块 README | exact |
| `README.md`（版本矩阵 / 目录状态） | config | file-I/O | 根 `README.md` §版本矩阵 + 目录表 | exact |

## Pattern Assignments

### `benchmark/scripts/run_matrix.sh` (utility, batch/streaming)

**Analog:** `projects/p03-vehicle-monitoring/scripts/loadtest.sh`

**Imports / shell preamble** (lines 1–20):
```bash
#!/usr/bin/env bash
# p03 VEH-06 压测入口：gen → 刮 Prometheus → 写 docs/baseline.md
# 禁止 k6/JMeter；依赖不可达 exit 1。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
PROM_URL="${PROM_URL:-http://localhost:9090}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9094}"
RATE="${RATE:-100}"
WARMUP_SEC="${WARMUP_SEC:-45}"
DURATION_SEC="${DURATION_SEC:-120}"
```

**Core pattern — deps → warmup discard → measure → scrape → write report** (lines 75–106, 161–185, 228–290):
```bash
require_deps() {
  if ! curl -sf "${PROM_URL}/-/ready" >/dev/null 2>&1 \
    && ! curl -sf "${PROM_URL}/api/v1/status/config" >/dev/null 2>&1; then
    echo "FAIL: Prometheus 不可达（${PROM_URL}）" >&2
    exit 1
  fi
  # Flink REST / Kafka / CH 同理；失败非 0
}

# 主流程：热身丢弃 → 计量 → sleep 落地 → scrape → cat > baseline.md
echo "==> warmup ${WARMUP_SEC}s（指标丢弃，不计入 baseline）"
run_gen "${WARMUP_SEC}" "warmup"
# … measure + scrape_metrics + HEREDOC 写四段结构 …
```

**矩阵扩展（planner 注意）:** 外层按 D-01 单元格循环（job × eps × backend × ckpt）；每轮只改一个旋钮；调用既有 `uv run scripts/gen_events.py --eps` / p03 `gen_vehicle_events.py --rate`；默认 WARMUP 30–60s 并在 `benchmark/baseline.md` 声明相对「理想 3 分钟」偏差。

**Error handling:** 一律 `echo "FAIL: …" >&2; exit 1`；禁止假数占位。

---

### `benchmark/scripts/collect_metrics.py` (utility, request-response)

**Analog:** `projects/p03-vehicle-monitoring/scripts/loadtest.sh` PromQL helpers

**Core PromQL scrape pattern** (lines 22–66, 135–159):
```bash
prom_query() {
  local q="$1"
  curl -sfG "${PROM_URL}/api/v1/query" --data-urlencode "query=${q}" 2>/dev/null \
    || return 1
}

prom_scalar_max() {
  local q="$1"
  local json
  json="$(prom_query "${q}")" || return 1
  python3 -c '
import json,sys
d=json.load(sys.stdin)
res=d.get("data",{}).get("result",[])
vals=[]
for r in res:
    try: vals.append(float(r["value"][1]))
    except (KeyError, IndexError, TypeError, ValueError): pass
print(max(vals) if vals else "")
' <<<"${json}"
}

# 固定值班指标名（抄 monitoring/README）：
# flink_jobmanager_job_lastCheckpointDuration
# flink_jobmanager_job_numRestarts
# flink_taskmanager_job_task_operator_currentEmitEventTimeLag
```

**Python 形态建议:** 将上述 `python3 -c` 抽成 `collect_metrics.py` CLI（stdin JSON 或直查 Prom）；输出 `key=value` 行供 `run_matrix.sh` 拼 markdown 表行。指标全名禁止臆造——以 `:9249` / Prom 实际 series 为准。

---

### `benchmark/baseline.md` (config, file-I/O)

**Analog:** `projects/p03-vehicle-monitoring/docs/baseline.md`（仓库级权威；项目级仅交叉引用）

**四段结构** (lines 1–57 文档形态；生成器见 loadtest L228–287):
```markdown
# … baseline（OrbStack arm64 实测）

> 方法论对齐 benchmark/README.md：环境快照 → 负载定义 → 指标表 → 结论。
> 数字仅来自本次实测，禁止当作生产 SLA。

## 1. 环境
| 项 | 值 |
| 采集时间 (UTC) | … |
| 主机 / 架构 | Darwin arm64 |
| Flink / TM / slots | … |

## 2. 负载
| 驱动 | gen_*.py --rate/--eps（无 k6/JMeter） |
| 热身 | Ns（**丢弃**）|

## 3. 指标
| 指标 | 值 | 来源 |
| … PromQL / 墙钟 eps … |

## 4. 结论
- 复跑命令 + SKIPPED 单元格须写原因（20k / ForSt）
```

**仓库级差异:** 表头增加矩阵列（Job / eps / State Backend / Checkpoint）；每单元格一行或分表；声明热身偏差。

---

### `benchmark/Makefile` (config, batch)

**Analog:** `projects/p03-vehicle-monitoring/Makefile`

**Target pattern** (lines 1–53):
```makefile
.PHONY: package verify loadtest drill-watermark

loadtest:
	bash scripts/loadtest.sh

# 映射到仓库级：
# matrix:   bash scripts/run_matrix.sh
# baseline: 依赖 matrix；断言 -s baseline.md
# dry-run:  低 eps 冒烟（Wave 0）
```

**约定:** 环境变量覆盖（`RATE`/`WARMUP_SEC`/`DURATION_SEC`/`EPS`）；失败非 0；不引入 k6 目标。

---

### `production/scripts/run-bluegreen-drill.sh` (utility, event-driven)

**Analog:** `projects/p03-vehicle-monitoring/scripts/drill_watermark_stall.sh`（两阶段演练 + 失败非 0 + 证据采集）

**Shell preamble + require_deps** (lines 1–27, 86–100):
```bash
#!/usr/bin/env bash
# 两阶段演练；失败非 0。可选附录不挡 exit 0。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

require_deps() {
  if ! curl -sf "${FLINK_URL}/overview" >/dev/null 2>&1; then
    echo "FAIL: Flink REST 不可达" >&2
    exit 1
  fi
  # BG 版：改为 kubectl get crd flinkbluegreendeployments… / nodes Ready
}
```

**Evidence write pattern（对齐 RESEARCH D-06 骨架 + loadtest 写文件）:**
```bash
OUT="${1:-production/docs/bluegreen-timeline.md}"
{
  echo "# Blue/Green Timeline"
  echo "UTC: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo '## CR status'
  kubectl get flinkbluegreendeployment -A -o yaml
  echo '## Child FlinkDeployments'
  kubectl get flinkdeployment -A -o wide
  echo '## Events'
  kubectl get events -A --sort-by='.lastTimestamp' | tail -n 80
} | tee "$OUT"
# 触发 TRANSITION → 轮询 ACTIVE_* → 超时 exit 1
```

**从 drill 抄的纪律:** 明确阶段标签（`==> stall` / `==> recover` 风格）；禁止「仅截图散文」；可选附录（Autoscaler）不挡硬门禁。

---

### `production/scripts/verify-argocd-sync.sh` (utility, request-response)

**Analog:** `projects/p03-vehicle-monitoring/scripts/verify.sh`

**Fail-closed + whitelist validation** (lines 1–35, 74–80):
```bash
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"

# 白名单 / 固定判据；非法输入 exit 1
case "${PATTERN_ID}" in
  HARSH_THEN_FAULT|TRIPLE_HARSH|DTC_PAIR) ;;
  *) echo "FAIL: …" >&2; exit 1 ;;
esac

if [[ "${MATCH_COUNT}" -lt "${MIN_COUNT}" ]]; then
  echo "FAIL: expected … got=${MATCH_COUNT}" >&2
  exit 1
fi
echo "ok …"
```

**Argo 映射:** 判据改为 `kubectl get application -n argocd …` 的 Sync/Health 字段（或 `argocd app get`）；成功打印 `ok sync=… health=…`；未 Synced / 无 Application CR → FAIL 非 0。

---

### `production/scripts/install-operator.sh` (utility, request-response)

**Analog（部分）:** `docker/init.sh`（`set -euo pipefail` 初始化脚本）+ RESEARCH Helm 安装块

**从 RESEARCH 抄安装坐标（无仓库内 Helm 样板）:**
```bash
set -euo pipefail
command -v helm >/dev/null || { echo "FAIL: helm 未安装（brew install helm）" >&2; exit 1; }
kubectl get nodes   # Ready 门禁

helm repo add flink-operator-repo \
  https://downloads.apache.org/flink/flink-kubernetes-operator-1.15.0/
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator \
  --namespace flink-operator --create-namespace \
  --set webhook.create=false \
  --set image.tag=1.15.0

kubectl get crd | grep flink
```

**版本纪律:** chart/镜像 tag 须先出现在根 README 版本矩阵（ENG-01）。

---

### `production/charts/p03-vehicle-alert/**` + `production/argocd/application-p03.yaml` (config)

**Analog:** 仓库内 **无** Helm chart / Argo CR — 使用 `06-RESEARCH.md` Pattern 2 / Argo 示意。

**Must copy from RESEARCH (FlinkBlueGreenDeployment):**
```yaml
apiVersion: flink.apache.org/v1beta1
kind: FlinkBlueGreenDeployment
metadata:
  name: p03-vehicle-alert-bg
spec:
  template:
    spec:
      image: flinklab/p03-vehicle-alert:dev   # 非 :latest
      imagePullPolicy: IfNotPresent
      flinkVersion: v2_2
      job:
        jarURI: local:///opt/flink/usrlib/p03-vehicle-monitoring.jar
        entryClass: com.flywhl.flinklab.p03.VehicleAlertJob
        upgradeMode: savepoint
        state: running
```

**作业提交类比（compose 侧，用于 JAR 主类/路径一致性）:** `projects/p03-vehicle-monitoring/Makefile` L17–26：
```makefile
submit: package
	cd ../../docker && docker compose exec jobmanager flink run -d \
		-c com.flywhl.flinklab.p03.VehicleAlertJob \
		"/opt/flink/usrlib/$$JAR_NAME"
```

**Kafka bootstrap 陷阱:** K8s Job 禁止用 compose DNS `kafka:9092`；宿主机可达地址（见 RESEARCH Pattern 3 / A1）。

---

### `monitoring/{platform-overview,job-deepdive,ai-cost}.json` (config, request-response)

**Analog:** `projects/p03-vehicle-monitoring/monitoring/dashboards/p03-vehicle-overview.json`

**Dashboard document shape** (lines 1–21, 176–215):
```json
{
  "id": null,
  "editable": true,
  "panels": [
    {
      "datasource": { "type": "prometheus", "uid": "PBFA97CFB590B2093" },
      "targets": [
        {
          "datasource": { "type": "prometheus", "uid": "PBFA97CFB590B2093" },
          "editorMode": "code",
          "expr": "flink_jobmanager_job_lastCheckpointDuration{job_name=~\"p03.*\"}",
          "refId": "A"
        }
      ],
      "title": "Flink 健康 · checkpoint 耗时 / 重启次数",
      "type": "timeseries"
    }
  ]
}
```

**Datasource UIDs to reuse:**
- Prometheus: `PBFA97CFB590B2093`（p03 JSON 已用；或 provisioning 默认 Prometheus）
- ClickHouse: `p03-clickhouse`（见 `docker/config/grafana/provisioning/datasources/clickhouse.yml`）

**PromQL 名抄自** `monitoring/README.md` 值班五指标（反压 / busy / ckpt / restarts / emit lag）。

**AI 看板:** 面板结构抄 p03 JSON；查询先 `:9249` 核实 p01 `guardrail_blocks` / BudgetGate counter 实际名（`GuardrailFunction.java` 注册 `guardrail_blocks`），禁止臆造 token 计费 series。

---

### Grafana provisioning + compose mount (config)

**Analog:** `docker/config/grafana/provisioning/dashboards/dashboards.yml` + `docker/docker-compose.yml` L208–212

**Provider pattern:**
```yaml
apiVersion: 1
providers:
  - name: p03-vehicle
    orgId: 1
    folder: p03
    type: file
    updateIntervalSeconds: 30
    options:
      path: /var/lib/grafana/dashboards/p03
```

**Volume pattern:**
```yaml
volumes:
  - ./config/grafana/provisioning:/etc/grafana/provisioning:ro
  - ../projects/p03-vehicle-monitoring/monitoring/dashboards:/var/lib/grafana/dashboards/p03:ro
  # 新增：../monitoring:/var/lib/grafana/dashboards/repo:ro + 对应 provider
```

**验证抄自:** `projects/p03-vehicle-monitoring/scripts/verify_dashboard.sh`（非空 JSON、uid 引用、`/api/datasources`、`/api/search`）。

---

### `best-practice/*.md` (config, file-I/O)

**Analog:** `best-practice/README.md`

**Rule body pattern** (lines 1–16):
```markdown
每条军规 = 规则 + 理由 + 反例事故。

1. **有状态算子必须显式 `.uid()` 与 `.name()`**——savepoint 按 uid 匹配；…
12. **AI 链路必须有降级路径**:…（展开见 ai/ 第 III 部）
```

**扩展纪律（D-12）:** 分章可拆文件，但每条保持「规则 + 理由 + 反例」；与 `production/` **互链**——规范正文在此，落地清单在 `production/docs/*`。

**AI 降级交叉引用:** `projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md` 检查表体例。

---

### `interview/**` (config, file-I/O)

**Analog:** `interview/README.md`

**Level 分层 + 教材锚点** (lines 1–46):
```markdown
## L1–L2(模型与时间)
1. …?(01-02)
…
## L8+(AI 专项)
27. …(ai/ 第 0 节)
```

**P5 升级（D-11）:** 保留 L1–L8+；每题从「考点骨架」升级为**完整参考答案或等价考点推导**；可链回 `docs/` / `ai/` 章节；目标 ≥150。计数脚本对齐 `qa_check.sh` 风格。

---

### `scripts/count_interview.py` (utility, file-I/O)

**Analog:** `scripts/qa_check.sh` 案例计数段 (lines 40–43):
```bash
MAINS=$(grep -rl --include='*.java' 'public static void main' examples | wc -l | tr -d ' ')
note "info  可运行作业数(main 计数)= $MAINS"
[ "$MAINS" -ge 67 ] && ok "案例数 ≥ …" || bad "案例数不足:$MAINS < 67"
```

**映射:** 扫描 `interview/**/*.md` 题号或 `^\d+\.`；`< 150` 非 0；可供 Makefile / CI 调用。

---

### `.github/workflows/ci.yml` (config, request-response)

**Analog:** 仓库无 workflow — **语义**对齐 `scripts/qa_check.sh`（compose config、违禁词、断链、案例计数、可选 mvn compile）。

**门禁语义摘录** (qa_check L1–56):
```bash
# ① compose YAML 可解析
# ② 违禁词扫描（TODO|FIXME|自行实现|请参考官网）
# ③ Markdown 相对链接存在性
# ④ 可运行案例计数
# ⑤ Maven 编译（可选）
bash scripts/qa_check.sh   # CI 主步骤
```

**Discretion:** workflow 文件名；不以多架构 push 为硬门禁（D-08）。

---

### `docs/README.md` 模块 13/14 + `docs/13-*` / `docs/14-*` (config)

**Analog:** `docs/README.md` 模块 11/12/15 完成态登记

**完成态句式** (lines 53–75):
```markdown
## 模块 11 · 生态协同 ✅([全文](./11-ecosystem/README.md))
## 模块 12 · AI 专项 ✅(全书独立成册:[ai/](../ai/README.md),…)
## 模块 15 · … ✅ 完成：… + [baseline.md](…) …
```

**模块 README 体例:** 抄 `docs/11-ecosystem/README.md`（背景→章节索引→参考）；或明确指向权威路径 `benchmark/` / `production/` / `monitoring/`（D-13 禁止空编号）。

---

### `README.md` SSOT / 目录状态 (config)

**Analog:** 根 `README.md` §版本矩阵 + 目录表

**SSOT 规则** (约 L17–28, L88–101):
- 新增 Helm/Argo/cert-manager 坐标先写版本矩阵再使用
- 目录表：`production/` 从「📐 蓝图」改为完成态表述；`benchmark/` / `monitoring/` 同步

## Shared Patterns

### Bash 脚本纪律
**Source:** `projects/p03-vehicle-monitoring/scripts/{loadtest,verify,drill_watermark_stall,verify_dashboard}.sh`
**Apply to:** `benchmark/scripts/*`、`production/scripts/*`
```bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
# require_deps → 业务步骤 → echo "ok …" / FAIL+exit 1
# 禁止 TODO/请参考官网；可选附录不挡硬门禁 exit 0
```

### PromQL / 值班五指标
**Source:** `monitoring/README.md` + `loadtest.sh` scrape
**Apply to:** `collect_metrics.py`、三块 Grafana JSON、baseline 指标列
```text
flink_taskmanager_job_task_isBackPressuredTimeMsPerSecond
flink_taskmanager_job_task_busyTimeMsPerSecond
flink_jobmanager_job_lastCheckpointDuration
flink_jobmanager_job_numRestarts
…_currentEmitEventTimeLag (source)
```

### Baseline 四段报告
**Source:** `projects/*/docs/baseline.md` + loadtest HEREDOC
**Apply to:** `benchmark/baseline.md`（权威）；项目级仅交叉引用（D-03）

### Grafana JSON + provisioning
**Source:** `p03-vehicle-overview.json` + `dashboards.yml` + compose volume + `verify_dashboard.sh`
**Apply to:** `monitoring/*.json`（恰好 3 个）+ provider/挂载；导入路径可文档化 API 一键导入作备选

### 造数驱动（禁止 k6）
**Source:** `scripts/gen_events.py`（`--eps`）+ `projects/p03/.../gen_vehicle_events.py`（`--rate/--duration`）
**Apply to:** 矩阵作业轴 e01-J2 / e10 / p03
```python
# scripts/gen_events.py
p.add_argument("--bootstrap", default="localhost:9094")
p.add_argument("--eps", type=int, default=200)
# 宿主机 bootstrap=localhost:9094；容器内 Flink 用 kafka:9092，勿混用
```

### 演练证据脚本化
**Source:** `drill_watermark_stall.sh` / `drill_ai_degrade.sh` + RESEARCH timeline 骨架
**Apply to:** `run-bluegreen-drill.sh` → `production/docs/bluegreen-timeline.md`

### 文档完成态登记
**Source:** `docs/README.md` 模块 11/15
**Apply to:** 模块 13/14、根 README 目录表、`production/README` 从蓝图→可复现入口

### QA / CI 门禁语义
**Source:** `scripts/qa_check.sh`
**Apply to:** GHA workflow、Phase 结束门禁、interview 计数脚本

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `production/charts/p03-vehicle-alert/**` | config | event-driven | 仓库无 Helm chart；用 RESEARCH Pattern 2 + p03 Makefile 主类/JAR 约定 |
| `production/argocd/application-p03.yaml` | config | event-driven | 无 Argo CR；用 RESEARCH Application 示意 + verify.sh 失败非 0 纪律 |
| `.github/workflows/ci.yml` | config | request-response | 无 GHA workflow；语义对齐 `qa_check.sh`，YAML 结构按 RESEARCH/Discretion |
| `production/docs/gitops-cicd.md`（完整 SOP） | config | file-I/O | 仅有蓝图段落；体例可抄 DEGRADE-CHECKLIST，步骤内容来自 RESEARCH |
| Operator/Argo 安装脚本细节 | utility | request-response | 无既有 kubectl/helm 脚本；Wave 0 须 `brew install helm` |
| e10 CEP 矩阵造数专用脚本 | utility | streaming | 若现有 e10 造数不足，Discretion 最小补丁；模式仍抄 `gen_events.py` |

## Metadata

**Analog search scope:** `benchmark/`、`production/`、`monitoring/`、`best-practice/`、`interview/`、`projects/p0{1,2,3}-*/`、`scripts/`、`docker/`、`docs/`、`.github/`、根 `README.md`
**Files scanned:** ~45（含骨架 README + 项目脚本/看板/Makefile）
**Strong analogs used:** 5（p03 loadtest、p03 baseline、p03 dashboard JSON、p03 drill、qa_check）
**Pattern extraction date:** 2026-07-18
