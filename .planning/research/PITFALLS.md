# Pitfalls Research

**Domain:** Flink 学习工程生产项目 / OrbStack arm64
**Researched:** 2026-07-17
**Confidence:** HIGH

## Critical Pitfalls

### 1. 「文档写全、本机未跑」——假完成

**Warning signs:** CHANGELOG Notes 出现「沙箱未验证」「留本机」却把状态标 ✅；验证脚本只有 echo  
**Prevention:** 合入前强制 OrbStack 实测；验证脚本必须断言可观察输出（topic/CH 行数/Dashboard RUNNING）  
**Phase:** 全程；P6 扫尾加重

### 2. 破坏 `make up` 基座

**Warning signs:** default compose 拉起失败；端口冲突；无 profile 却依赖 p03 服务  
**Prevention:** 项目只用 `--profile`；PR 跑 `docker compose config`；主干禁止半成品  
**Phase:** p03 起建立样板

### 3. CEP 无 within / 宽松量词爆炸

**Warning signs:** TM 内存爬升；部分匹配数无上界；模式缺少五元组登记  
**Prevention:** 无 within 禁止合入；followedByAny/oneOrMore 需状态上界论证；复用 e10 红线  
**Phase:** p03

### 4. 事件时间/watermark 演示失败却怪 CEP

**Warning signs:** 超时从不触发；告警「偶发」；造数用处理时间混事件时间  
**Prevention:** 造数带事件时间字段 + 显式 watermark；演练剧本含「停 watermark」  
**Phase:** p03 告警会话

### 5. AI 路径无降级，环境一缺就整章不可用

**Warning signs:** README 要求必须 Ollama+Milvus；无 Agents 就编译失败进主构建  
**Prevention:** 主构建零硬依赖 Preview；AI 路径 profile 化；降级核对清单（P3 纪律）  
**Phase:** p01

### 6. 版本号散落，SSOT 漂移

**Warning signs:** 文档写死 2.3；pom 与 README 矩阵不一致；新组件未登记  
**Prevention:** 新增先改矩阵+属性区；qa_check 可后续加重版本一致性  
**Phase:** 全程

### 7. 违禁词与「请参考官网」回潮

**Warning signs:** 半成品用 TODO/省略/略/自行实现  
**Prevention:** qa_check 违禁词扫描；会话结束前自扫  
**Phase:** P6 最终清零；每 Phase 预扫

### 8. 压测/故障演练变成散文

**Warning signs:** 只有方法论无命令；无基线数字；演练不可重复  
**Prevention:** 每个项目 `drills/` + `bench/` 可执行；记录 baseline 数字进文档  
**Phase:** p03 样板 → p01/p02 复制 → P5 矩阵化

### 9. OrbStack K8s Blue/Green 纸上谈兵

**Warning signs:** 只贴 Operator YAML；无实际 rollout 观察  
**Prevention:** P5 验收必须本机执行并记录截图替代物（日志/事件时间线）  
**Phase:** P5

### 10. 会话范围过大导致主干半成品

**Warning signs:** 单 commit 含三个项目骨架；wip 直接进 main  
**Prevention:** 会话 ≤ 一模块；半成品 `wip/` 分支；先告警后大盘  
**Phase:** 全程（用户已锁定）

### 11. 复制 P2/P3 的「编译验证须在本机」债务

**Warning signs:** 再次用「沙箱离线」豁免硬验收  
**Prevention:** 本里程碑 Core Value 明确禁止；P4 起不再接受该豁免作为 ✅  
**Phase:** P4 起

## Pitfall → Phase Map

| Pitfall | p03 | p01 | p02 | P5 | P6 |
|---------|-----|-----|-----|----|-----|
| 假完成 | ● | ● | ● | ● | ● |
| 破坏 make up | ● | ○ | ○ | ○ | ● |
| CEP 状态爆炸 | ● | | | | |
| watermark 踩坑 | ● | ○ | ○ | | |
| AI 无降级 | | ● | | | |
| SSOT 漂移 | ● | ● | ● | ● | ● |
| 违禁词 | ○ | ○ | ○ | ○ | ● |
| 演练散文 | ● | ● | ● | ● | |
| K8s 纸面 | | | | ● | |
| 会话过大 | ● | ● | ● | ○ | |
| 沙箱豁免债务 | ● | ● | ● | ● | ● |

## Research Flags for Planning

- p03 Phase：强制「验证脚本断言」任务，不可仅文档
- p01 Phase：强制「降级路径」作为 must-have，非附录
- P5 Phase：Blue/Green 列为 success criteria 可观察行为
- P6 Phase：案例数/行数/违禁词计量进 success criteria

---
*Research date: 2026-07-17 · Sources: CHANGELOG P2/P3 Notes, PROJECT.md, e10 红线*
