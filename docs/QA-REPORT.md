# QA 报告 · P6 总装门禁摘要

> 人读摘要（不挡门禁）。权威口径以 `scripts/qa_check.sh` 与 `scripts/eng_audit.sh` 在 OrbStack arm64 本机实测为准。
> 违禁词扫描按 D-08 词表整词匹配；对单独汉字「略」不做裸匹配，避免误杀「策略」等合法用词。
> **2026-07-19**：撤销文档行数 ≥30000 硬门禁（注水刷数导致质量降级）；详见 [`.planning/MEMORY.md`](../.planning/MEMORY.md)。

## 环境

| 项 | 值 |
|---|---|
| 架构 | arm64（`uname -m`） |
| Docker context | orbstack |
| 日期 | 2026-07-19 |

## 命令与期望

```bash
bash scripts/qa_check.sh   # 期望 == QA PASS == ，exit 0
bash scripts/eng_audit.sh  # 期望 == ENG AUDIT PASS == ，exit 0
```

## 计量阈值

| 指标 | 阈值 | 说明 |
|---|---|---|
| `examples/` 含 `public static void main` 的作业数 | ≥ 100 | 仅 examples；不含 projects / 测试类 |
| 全仓 `*.md` 行数（排除 `.planning/`、`.git/`） | 诊断 only | `count_docs.py` 打印分目录；**不以行数硬失败** |

## 文档质量纪律（取代行数硬指标）

- 禁止为凑行数复制粘贴同构段落（编号循环、模板情景、模板检查清单）
- 扩写须增加可执行信息：命令、路径、失败模式、与 examples/projects 交叉引用
- Wave 2 注水提交（`5b427d4` / `0435b7b` / `3cc6882`）已回退至扩写前实质正文

## 硬门禁摘录

| 门禁 | 期望 |
|---|---|
| ① compose | ok |
| ② 违禁词 | ok |
| ③ Markdown 相对链接 | ok |
| ④ 案例 ≥ 100 | ok |
| ⑤ mvn compile | ok |
| ENG-01…04 | PASS |
| 文档行数 | 仅 `info` 诊断，不挡门禁 |

证明命令：

```bash
uname -m                                          # arm64
docker context show                               # orbstack
bash scripts/qa_check.sh                          # == QA PASS ==
bash scripts/eng_audit.sh                         # == ENG AUDIT PASS ==
grep -rl --include='*.java' 'public static void main' examples | wc -l   # ≥100
python3 scripts/count_docs.py                     # 诊断打印，exit 0
```

## 相关路径

- [PHASES.md](../PHASES.md) · P6 可验证完成态
- [scripts/qa_check.sh](../scripts/qa_check.sh)
- [scripts/eng_audit.sh](../scripts/eng_audit.sh)
- [`.planning/MEMORY.md`](../.planning/MEMORY.md) · 注水整改记忆
- [projects/p01-log-ai-platform/README.md](../projects/p01-log-ai-platform/README.md)
- [projects/p02-realtime-reco/README.md](../projects/p02-realtime-reco/README.md)
- [projects/p03-vehicle-monitoring/README.md](../projects/p03-vehicle-monitoring/README.md)
- [production/README.md](../production/README.md)
