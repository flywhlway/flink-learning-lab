# interview/ · 面试题库（Level 分层，≥150）

> **使用方式：** 先自答，再看各文件中的 **参考答案**（完整考点推导）。可链回教材章节做延伸阅读，但每题答案正文已自洽（D-11 / PROD-04）。

## 门禁

```bash
python3 scripts/count_interview.py
# 期望：interview_questions≥150 且 exit 0
```

计数规则：所有 `interview/**/*.md` 中行首 `数字. ` 视为一题。

## 分层索引

| Level | 主题 | 题号 | 题量 | 文件 |
|---|---|---|---|---|
| L1 | 运行时与拓扑 | 1–31 | 31 | [`L1.md`](L1.md) |
| L2 | 时间与窗口 | 32–61 | 30 | [`L2.md`](L2.md) |
| L3 | 状态管理 | 62–91 | 30 | [`L3.md`](L3.md) |
| L4 | Checkpoint / Savepoint / EOS | 92–122 | 31 | [`L4.md`](L4.md) |
| L5 | Table/SQL | 123–151 | 29 | [`L5.md`](L5.md) |
| L6 | 连接器 / CDC / 湖仓 | 152–175 | 24 | [`L6.md`](L6.md) |
| L7 | 性能、K8s 与生产化 | 176–200 | 25 | [`L7.md`](L7.md) |
| L8 | AI 专项（p01 / Agents / RAG） | 201–230 | 30 | [`L8.md`](L8.md) |

**合计：230 题**

## 原首批 30 题

已并入各 Level 文件并升级为完整参考答案（不再使用「仅考点骨架」）。

## 与教材 / 工程互链

- 教材目录：[`docs/README.md`](../docs/README.md)
- AI 教材：[`ai/`](../ai/)（若存在）
- 生产化：[`production/`](../production/)、[`monitoring/`](../monitoring/)、[`benchmark/`](../benchmark/)
- 规范：[`best-practice/`](../best-practice/)
