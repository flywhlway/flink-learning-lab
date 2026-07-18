---
status: complete
slug: qa-3-5b427d4-0435b7b-3cc6882-c75c72c-d48
completed: 2026-07-19
---

# SUMMARY · 撤销文档行数硬指标并整改 Wave 2 注水

## Result

- 产品文档已从 `5b427d4` / `0435b7b` / `3cc6882` 注水态回退至扩写前实质正文（约 −20k 注水行）
- `qa_check` 改为五硬门；文档行数仅诊断（`doc_lines=10765`）
- 决策写入 `.planning/MEMORY.md`（D-14…D-16）并回填 CONTEXT / STATE / PHASES / README / CHANGELOG

## Verification

```
bash scripts/qa_check.sh   # == QA PASS ==
bash scripts/eng_audit.sh  # == ENG AUDIT PASS ==
python3 scripts/count_docs.py  # ok doc_lines=10765 (no min gate)
```

## Key files

- `.planning/MEMORY.md`
- `scripts/qa_check.sh` / `scripts/count_docs.py` / `scripts/eng_audit.sh`
- 回退的 docs/ / interview/ / ai/ / best-practice/ / examples/**/README.md 等
