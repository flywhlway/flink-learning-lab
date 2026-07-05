# job-sql 模板

SQL 作业标准骨架:DDL 目录化 + StatementSet 集中提交 + EXPLAIN 留档,固化 best-practice/ 中与 SQL 作业相关的全部纪律。

## 目录结构

```
job-sql/
├── ddl/              # 逐表一个 .sql 文件,便于 code review 与版本追踪
│   ├── source.sql
│   └── sink.sql
├── statements/
│   └── main.sql      # 业务查询 + INSERT,通过 StatementSet 语义组织
├── explain/          # EXPLAIN 输出存档目录(提交前生成,评审附件)
└── submit.sh         # 标准提交脚本(SQL Client -f 组合执行)
```

## 使用方式

1. 在 `ddl/` 下为每张表建一个独立 `.sql` 文件(source 一个、sink 一个,更多按需拆分)。
2. 在 `statements/main.sql` 中编写业务逻辑,**多个 INSERT 用 `BEGIN STATEMENT SET; ... END;` 包裹**成单作业提交(对应 e05-C2 的 StatementSet)。
3. 提交前生成 EXPLAIN 存档到 `explain/`(见 submit.sh),作为评审附件之一。
4. 完成"上线交付三件套"(EXPLAIN 存档 / TTL 声明与论证 / 回撤去向说明,docs/05 与 e05/README)。

## 固化的纪律来源

| 纪律 | 依据 |
|---|---|
| DDL 逐表拆分独立文件 | 便于 code review/diff,工程通用纪律 |
| StatementSet 集中提交 | e05-C2,共享源与优化,单作业交付 |
| EXPLAIN 存档 | e05-C10,evidence for review |
| TTL 声明与论证(全局或 STATE_TTL hint) | 军规 11,e05-C6/C10 |
| 回撤流去向必须显式说明 | e05/README §7,docs/05-01 |
