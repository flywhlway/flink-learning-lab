# 第 24 章 · 架构收官:事件驱动 AI 平台参考架构

> 本章综合前 23 章内容,产出可直接用于企业汇报的参考架构 · Level:L7

## 1. 全景架构图

```mermaid
flowchart TB
    subgraph SRC[事件源]
        K1[业务系统事件] --- K2[IoT/传感器信号] --- K3[日志/指标]
    end
    subgraph BUS[事件总线 · 第2章]
        BUS1[(Kafka\n分层 topic + 事件契约)]
    end
    subgraph CORE[Flink Agents 运行时 · 第7-14章]
        AG1[分诊 Agent] --> AG2[专家 Agent 集群]
        AG2 --> AG3[汇总/决策 Agent]
        AG2 <-->|三层记忆| MEM[(Short/Long-Term Memory\nMem0)]
        AG2 <-->|工具调用| TOOL[FunctionTool/MCP\nDurable Execution]
    end
    subgraph AI[AI 能力层 · 第3-5章]
        INFER[ML_PREDICT 推理]
        VEC[(Milvus 向量库)]
        RAG[Streaming RAG]
    end
    subgraph GOV[治理层 · 第17-22章]
        GUARD[护栏引擎] --- COST[成本熔断] --- PROMPT[Prompt灰度]
    end
    subgraph OBS[可观测性 · 第15-16章]
        MET[Metrics/Grafana] --- TRACE[全链路 Trace] --- LOG[结构化EventLog]
    end
    subgraph FEED[反馈闭环 · 第21-23章]
        EVAL[在线评测] --> SAMPLE[样本管线] --> RAY[Ray Train]
        RAY --> REGISTRY[(模型注册表)] --> INFER
    end

    SRC --> BUS --> CORE
    CORE <--> AI
    CORE <--> GOV
    CORE --> OBS
    CORE --> FEED
```

## 2. 分层职责一览表

| 层 | 核心职责 | 对应章节 |
|---|---|---|
| 事件总线 | 事件契约、Schema 治理、命令/事实语义分离 | 02 |
| AI 能力层 | 流式推理、向量检索、RAG、知识图谱 | 03-05、14 |
| Agent 运行时 | Action 编排、记忆、工具调用、多 Agent 协作 | 07-13 |
| 治理层 | 护栏、成本控制、Prompt 灰度、模型路由 | 17-20、22 |
| 可观测性 | 指标、Trace、结构化日志 | 15-16 |
| 反馈闭环 | 在线评测、样本管线、训练协同(Ray) | 06、21、23 |

## 3. 与企业既有 AIOS/AIDO 蓝图的映射指南

本参考架构与你的 AITS(智能分诊)/SVDM(智能车辆数据诊断)融合蓝图 AIDO 存在直接的概念映射:

| 本书概念 | AIDO/AITS 对应概念 | 映射说明 |
|---|---|---|
| 分诊-专家-汇总多 Agent 协作(第12章) | AITS 两层诊断设计 | 同一"先分诊、后专家处理"的架构模式 |
| 护栏引擎(第17章) | AITS 告警会话生命周期耦合协议 | 都是"决策与执行之间插入的安全阀" |
| AI Gateway(第19章) | AITS 自定义 AI 网关 | 概念完全对应,本书提供了流式化的补充视角 |
| 三层记忆(第08章) | mem0 2.0.x 长期记忆工程经验 | Flink Agents 的 Mem0 收敛验证了你此前的技术选型方向 |
| Streaming Feature(第06章) | 车联网监控案例三的实时特征需求 | 直接技术基础 |

**给企业汇报使用的核心论点**(可直接摘录):"我们过去在 AITS/SVDM 上验证的分诊-专家-汇总架构、动态规则热更新、AI 网关等设计模式,与 Apache 官方最新推出的 Flink Agents 项目在架构理念上高度一致——这既验证了我们此前架构决策的前瞻性,也意味着可以用官方标准化组件替代部分自建能力,降低长期维护成本。"

## 4. 分阶段落地建议

```mermaid
flowchart LR
    P1[阶段1\n事件总线+可观测性打底\n第2/15/16章] --> P2[阶段2\n单 Agent 场景验证\n第7章+一个具体业务场景]
    P2 --> P3[阶段3\n治理层补齐\n第17/18/22章护栏成本Prompt]
    P3 --> P4[阶段4\n多 Agent 协作+反馈闭环\n第12/21/23章]
```

不建议一次性铺开全部 24 章的能力——按此四阶段路线,每阶段验证一批能力再推进下一阶段,与 Flink Agents 本身"Preview→逐步收敛稳定"的成熟度曲线保持同步。

## 5. 全书核心工程判据回顾

贯穿全书的判断框架,收敛为可直接用于方案评审的检查单:

1. 触发源是人还是事件?(第1章)
2. 正确性要求动作不重不漏吗?(第1、9章)
3. 上下文是会话级还是跨事件累积?(第1、8章)
4. 这个环节该用 Flink 拓扑还是 Agent 工作流还是外呼交互式服务?(第11、13章)
5. 这个决策的成本、护栏、可追溯性是否已经设计到位?(第15-20章)

## 6. 面试题(全书综合)

① 如果只能向一个从未接触过事件驱动 AI 的团队讲一件事,你会讲什么?② 本参考架构里,哪个组件最有可能在生产中先崩溃,为什么?③ 如何向非技术管理层解释"为什么 AI Agent 需要流处理引擎"这件事(用一个类比)?

## 7. 参考资料

全书第 1-23 章;docs/00-landscape(技术底座总览);docs/11(生态协同,架构选型的外部参照系)。

---

## Wave 2 扩写 · 24-reference-architecture

### 背景加固

本章对应 AI 学习路径中的「24-reference-architecture」。流式 AI 工程的约束与批式离线不同：延迟预算、成本封顶、降级路径、可观测追踪必须在作业图内一等公民对待。本仓库 e12 系列用零依赖 DataStream 演示机制；p01 提供可降级生产路径。

### 架构对照

```mermaid
flowchart LR
  Bus[Kafka/事件总线] --> Feat[特征/护栏/路由]
  Feat --> Infer[推理/工具/RAG]
  Infer --> Out[下游 topic / CH / 旁路]
  Feat --> Deg[降级标签]
```

控制面：预算、熔断、开关（Broadcast/侧输出）。数据面：embedding、提示、工具调用结果。
降级决策树：外部依赖超时 → 规则路径；成本超软顶 → 降采样；护栏命中 → 旁路。

### 与仓库 Demo 对照

- 优先查找 `examples/e12-24-*/README.md` 与同模块第二 Job；若编号为独立成册章节，见 `ai/README.md` 映射表。
- 生产对照：`projects/p01-log-ai-platform/`（AI off 默认可跑）。
- 规范：`best-practice/08-ai-degrade.md`。

### 踩坑实证

1. 坑 1：把同步外呼放在 map 线程；或无预算的工具调用；或无 trace 无法定位延迟。实证方向：用 e11/e12 作业制造超时，观察旁路与指标。

2. 坑 2：把同步外呼放在 map 线程；或无预算的工具调用；或无 trace 无法定位延迟。实证方向：用 e11/e12 作业制造超时，观察旁路与指标。

3. 坑 3：把同步外呼放在 map 线程；或无预算的工具调用；或无 trace 无法定位延迟。实证方向：用 e11/e12 作业制造超时，观察旁路与指标。

4. 坑 4：把同步外呼放在 map 线程；或无预算的工具调用；或无 trace 无法定位延迟。实证方向：用 e11/e12 作业制造超时，观察旁路与指标。

5. 坑 5：把同步外呼放在 map 线程；或无预算的工具调用；或无 trace 无法定位延迟。实证方向：用 e11/e12 作业制造超时，观察旁路与指标。

6. 坑 6：把同步外呼放在 map 线程；或无预算的工具调用；或无 trace 无法定位延迟。实证方向：用 e11/e12 作业制造超时，观察旁路与指标。

7. 坑 7：把同步外呼放在 map 线程；或无预算的工具调用；或无 trace 无法定位延迟。实证方向：用 e11/e12 作业制造超时，观察旁路与指标。

### 降级决策树

1. 依赖健康？否 → 规则/缓存路径。
2. 成本软顶？超 → 降采样/关昂贵模型。
3. 护栏分数？拒 → side output。
4. 全部通过 → 主输出。

### 验证步骤

1. 启动对应 e12 作业；注入正常/超时/超预算流量；检查主流与旁路；确认无违禁词文档；记录到个人 baseline 笔记。

2. 启动对应 e12 作业；注入正常/超时/超预算流量；检查主流与旁路；确认无违禁词文档；记录到个人 baseline 笔记。

3. 启动对应 e12 作业；注入正常/超时/超预算流量；检查主流与旁路；确认无违禁词文档；记录到个人 baseline 笔记。

4. 启动对应 e12 作业；注入正常/超时/超预算流量；检查主流与旁路；确认无违禁词文档；记录到个人 baseline 笔记。

5. 启动对应 e12 作业；注入正常/超时/超预算流量；检查主流与旁路；确认无违禁词文档；记录到个人 baseline 笔记。

### 面试钩子

用 90 秒讲清「24-reference-architecture」：定义、流式约束、降级、仓库路径（e12/p01）、一个指标。题库见 `interview/L8.md`。

### 模式卡片

#### 卡片 24-reference-architecture-1

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-2

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-3

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-4

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-5

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-6

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-7

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-8

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-9

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-10

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

#### 卡片 24-reference-architecture-11

问题：在流式场景下如何保证「24-reference-architecture」相关能力可降级且可观测？
方案：作业内开关 + 旁路 + 预算；外呼 Async；缓存 TTL；追踪字段贯通。
验证：OrbStack 跑 e12；断依赖仍有输出契约。
反例：无开关硬依赖 Ollama/Milvus 导致主路径不可用。

