# QA 报告 · P6 总装门禁摘要

> 人读摘要（不挡门禁）。权威口径以 `scripts/qa_check.sh` 与 `scripts/eng_audit.sh` 在 OrbStack arm64 本机实测为准。
> 违禁词扫描按 D-08 词表整词匹配；对单独汉字「略」不做裸匹配，避免误杀「策略」等合法用词。

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

## 计量阈值（D-01 / D-04）

| 指标 | 阈值 | 说明 |
|---|---|---|
| `examples/` 含 `public static void main` 的作业数 | ≥ 100 | 仅 examples；不含 projects / 测试类 |
| 全仓 `*.md` 行数（排除 `.planning/`、`.git/`） | ≥ 30000 | 与 `python3 scripts/count_docs.py` 一致 |

## Wave 3 终态摘录（ENG-04 严格 PHASES 已启用）

| 门禁 | 结果 |
|---|---|
| ① compose | ok |
| ② 违禁词 | ok |
| ③ Markdown 相对链接 | ok |
| ④ 案例 ≥ 100 | ok（mains=100） |
| ⑤ 文档 ≥ 30000 | ok（doc_lines≥30000） |
| ⑥ mvn compile | ok |
| ENG-01…03 | PASS |
| ENG-04 | PASS（CHANGELOG Unreleased + PHASES P6 可验证完成态） |

终验以 Task 3 本机复跑输出为准；确认未创建 git tag（D-12，留给 `/gsd-complete-milestone`）。

## 相关路径

- [PHASES.md](../PHASES.md) · P6 可验证完成态
- [scripts/qa_check.sh](../scripts/qa_check.sh)
- [scripts/eng_audit.sh](../scripts/eng_audit.sh)
- [projects/p01-log-ai-platform/README.md](../projects/p01-log-ai-platform/README.md)
- [projects/p02-realtime-reco/README.md](../projects/p02-realtime-reco/README.md)
- [projects/p03-vehicle-monitoring/README.md](../projects/p03-vehicle-monitoring/README.md)
- [production/README.md](../production/README.md)
