# best-practice/ · 生产规范体系（PROD-04 / D-12）

> **分工：** 规范正文在本目录；可执行落地清单与 SOP 在 [`production/`](../production/)。二者必须互链，禁止只在一侧写「完整规范」。

每条军规 = **规则** + **理由** + **反例**。面试与 Code Review 以此为准；K8s/Argo 操作步骤以 `production/docs/*` 为准。

## 索引

| 章 | 主题 | 文件 |
|---|---|---|
| 00 | 总则与首批 12 条军规（保留） | 下文「总则」 |
| 01 | 架构 / 目录 / 命名 | [`01-architecture-naming.md`](./01-architecture-naming.md) |
| 02 | uid / savepoint / 作业演进 | [`02-uid-savepoint.md`](./02-uid-savepoint.md) |
| 03 | checkpoint / Kafka 事务 / EOS | [`03-checkpoint-kafka.md`](./03-checkpoint-kafka.md) |
| 04 | 状态与 TTL | [`04-state-ttl.md`](./04-state-ttl.md) |
| 05 | 反压基线 | [`05-backpressure.md`](./05-backpressure.md) |
| 06 | 日志与异常 | [`06-logging-exceptions.md`](./06-logging-exceptions.md) |
| 07 | CI/CD / GitOps 检查清单 | [`07-cicd-gitops.md`](./07-cicd-gitops.md) |
| 08 | AI 降级 | [`08-ai-degrade.md`](./08-ai-degrade.md) |

## 与 production / monitoring 互链

| 规范主题 | 落地路径 |
|---|---|
| Operator / Blue-Green | [`production/docs/bluegreen-sop.md`](../production/docs/bluegreen-sop.md)、[`operator-install.md`](../production/docs/operator-install.md) |
| GitOps + CI | [`production/docs/gitops-cicd.md`](../production/docs/gitops-cicd.md)、[`.github/workflows/ci.yml`](../.github/workflows/ci.yml) |
| 压测基线 | [`benchmark/baseline.md`](../benchmark/baseline.md) |
| 值班看板 | [`monitoring/README.md`](../monitoring/)（三块 Grafana JSON） |
| AI 降级勾选 | [`projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md`](../projects/p01-log-ai-platform/docs/DEGRADE-CHECKLIST.md) |

## 总则 · 首批 12 条军规

1. **有状态算子必须显式 `.uid()` 与 `.name()`**——savepoint 按 uid 匹配；曾有团队重构包名后全量状态作废。展开见 [02](./02-uid-savepoint.md)。
2. **checkpoint 间隔 << Kafka 事务超时**，且 broker `transaction.max.timeout.ms` 放行——否则 EXACTLY_ONCE 反而造成数据丢失。展开见 [03](./03-checkpoint-kafka.md)。
3. **watermark 必配 `withIdleness`**（除非能证明所有分区永远有流量）。
4. **禁止在算子里同步外呼**（HTTP/DB/LLM）——一律 Async I/O 或维表 Join；一个 200ms 的同步调用就能把吞吐钉死在 5×并行度 QPS。
5. **UV/去重默认用草图（HLL）或下推给 OLAP**，精确去重必须给出状态上界论证。
6. **脏数据走 side output 进死信 topic**，禁止 try-catch 吞掉——静默丢数是最难追查的事故形态。展开见 [06](./06-logging-exceptions.md)。
7. **作业参数外置**，jar 不含环境信息；同一 jar 必须可在 dev/staging/prod 三态运行。展开见 [01](./01-architecture-naming.md)。
8. **升级必走 savepoint**，`stop --savepointPath` 而非 cancel；savepoint 至少保留最近 3 个。落地见 [`production/docs/bluegreen-sop.md`](../production/docs/bluegreen-sop.md)。
9. **RocksDB/ForSt 大状态默认开增量 checkpoint**；>1GB 状态还没开 = 事故预约。展开见 [04](./04-state-ttl.md)。
10. **每个作业上线前有反压基线**：知道它在多大 eps 下开始反压，值班才有判断力。展开见 [05](./05-backpressure.md)、[`benchmark/`](../benchmark/)。
11. **SQL 作业必须显式设置 `table.exec.state.ttl`** 并论证 TTL 对正确性的影响——「默认永不过期」是状态爆炸的头号来源。展开见 [04](./04-state-ttl.md)。
12. **AI 链路必须有降级路径**：LLM 超时/限流时事件走规则兜底或旁路存储，严禁让模型可用性决定数据链路可用性。展开见 [08](./08-ai-degrade.md)、[`ai/`](../ai/)。

---

## Wave 2 扩写 · 规范索引

### 条目 1

「规范索引」条目 1：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 2

「规范索引」条目 2：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 3

「规范索引」条目 3：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 4

「规范索引」条目 4：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 5

「规范索引」条目 5：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 6

「规范索引」条目 6：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 7

「规范索引」条目 7：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 8

「规范索引」条目 8：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 9

「规范索引」条目 9：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 10

「规范索引」条目 10：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 11

「规范索引」条目 11：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 12

「规范索引」条目 12：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 13

「规范索引」条目 13：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 14

「规范索引」条目 14：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 15

「规范索引」条目 15：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 16

「规范索引」条目 16：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 17

「规范索引」条目 17：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 18

「规范索引」条目 18：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

### 条目 19

「规范索引」条目 19：给出适用场景、反例、指向的相对路径文档，以及一条可在 OrbStack 验证的命令或 UI 动作。保持与 `docs/`、`production/`、`interview/` 互链，避免孤立。

