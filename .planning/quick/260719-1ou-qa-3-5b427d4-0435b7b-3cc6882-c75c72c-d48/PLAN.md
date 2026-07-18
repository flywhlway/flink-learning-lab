---
status: complete
slug: qa-3-5b427d4-0435b7b-3cc6882-c75c72c-d48
created: 2026-07-19
---

# PLAN · 撤销文档行数硬指标并整改 Wave 2 注水

## Goal

去除 QA「文档 ≥30000 行」硬指标；核查并回退提交 `5b427d4` / `0435b7b` / `3cc6882`（及元数据 `c75c72c` / `d480eb4`）引入的重复注水内容；将决策写入 `.planning/MEMORY.md`。

## Tasks

1. 审计注水模式（模板循环 / Wave 2 段）并量化影响
2. 回退三批内容提交涉及的全部 `*.md` 至扩写前父版本
3. 修订 `qa_check.sh` / `count_docs.py` / `eng_audit.sh` 拆除行数硬门
4. 同步 PHASES / README / CHANGELOG / REQUIREMENTS / PROJECT / CONTEXT / QA-REPORT
5. 写入 `.planning/MEMORY.md`（D-14…D-16）并更新 STATE
6. 复跑 `qa_check` / `eng_audit` / `count_docs`

## Done criteria

- [x] 无「文档行数 ≥30000」硬失败路径
- [x] Wave 2 注水段从产品文档消失
- [x] MEMORY + STATE 记录决策与复发信号
- [x] qa_check / eng_audit 可绿（案例≥100 仍硬门）
