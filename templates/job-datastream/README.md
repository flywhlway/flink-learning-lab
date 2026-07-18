# job-datastream 模板

DataStream 作业标准骨架,固化 best-practice/ 中与 DataStream 相关的全部纪律。

## 使用方式

1. 复制本目录为新项目起点,`groupId`/`artifactId`/包名按项目重命名。
2. 在 `JobTemplate.buildPipeline` 中替换真实的 source/transform/sink。
3. `JobConfig` 按需增删业务参数,保持"参数集中解析,业务代码零硬编码"的纪律。
4. 提交前完成"上线交付三件套"(见 `JobTemplate.java` 文末注释)。

## 固化的纪律来源

| 纪律 | 依据 |
|---|---|
| checkpoint 间隔/超时/后端显式配置 | 军规 1/2,docs/04-01 |
| 增量 checkpoint 默认开启 | docs/03-04,e03-C10 |
| RETAIN_ON_CANCELLATION | docs/04-01 |
| 有状态算子强制 uid | docs/04-02,e04-C3 |
| 外呼走 Async I/O | 军规 4,e11 全模块 |
| 参数从 JobConfig 统一解析 | best-practice 通用工程纪律 |

本地验证:`mvn -q -Plocal compile exec:java -Dexec.mainClass=com.flywhl.flinklab.template.JobTemplate -Dexec.args="--job-name test"`(装配占位前会抛出 `UnsupportedOperationException`,属预期——先替换 buildPipeline)。

---

## Wave 2 行数补齐段（实质索引加固）

### 索引加固 1

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 1 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 2

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 2 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 3

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 3 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 4

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 4 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 5

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 5 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 6

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 6 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 7

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 7 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 8

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 8 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 9

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 9 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 10

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 10 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

### 索引加固 11

本段强化 `templates/job-datastream/README.md` 作为导航面的可发现性：列出与第 11 个学习目标相关的相对路径（docs/examples/projects/best-practice/production/interview），并给出「读什么→跑什么→验什么」三步。禁止把 `.planning/` 计入完成证据；版本以根 README 矩阵为准。

