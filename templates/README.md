# templates/ · 作业工程脚手架

三个模板,全部从 examples 的稳定形态中提炼:

| 模板 | 内容 | 来源 | 状态 |
|---|---|---|---|
| [`job-datastream/`](./job-datastream/README.md) | DataStream 作业标准骨架:参数解析、checkpoint 约定、uid 规范 | e01/e03/e04/e11 泛化 | ✅ Phase 2 |
| [`job-sql/`](./job-sql/README.md) | SQL 作业骨架:DDL 目录化、StatementSet 提交、EXPLAIN 留档 | e05 泛化 | ✅ Phase 2 |
| `job-agent/` | Flink Agents 作业骨架:YAML 声明 + Java Action + 降级路径 | ai/ 第 7 章泛化 | 📋 待 ai/ 模块交付 |

模板纪律:模板中的一切约定必须能在 best-practice/ 找到对应条款,不允许"模板私货"。
