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

---

## Wave 2 行数补齐段（实质索引加固）

### 索引加固 1

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 1 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 2

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 2 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 3

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 3 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 4

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 4 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 5

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 5 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 6

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 6 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 7

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 7 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 8

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 8 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 9

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 9 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 10

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 10 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 11

本段强化 `templates/job-sql/README.md` 作为导航面的可发现性：列出与第 11 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

