# scripts/ · 工具脚本(uv 单文件脚本规范)

所有脚本采用 PEP 723 内联元数据 + `uv run` 直跑,零虚拟环境管理成本。

| 脚本 | 用途 | 典型用法 |
|---|---|---|
| `gen_events.py` | 点击流生成器(事件时间打点、可控乱序、可注入倾斜) | `uv run scripts/gen_events.py --topic clicks --eps 200` |
| `qa_check.sh` | 六硬门：compose / 违禁词(含「省略」) / 断链 / mains≥100 / md≥30000 / mvn compile 硬失败；末尾调用 eng_audit | `bash scripts/qa_check.sh` |
| `eng_audit.sh` | ENG-01…04 终检（版本抽样 / docs 编号 / baseline 证据 / CHANGELOG Unreleased；PHASES 严格断言见 07-03） | `bash scripts/eng_audit.sh` |
| `count_docs.py` | 文档行数诊断（excl `.planning`/`.git`；不足 30000 则 exit 1） | `python3 scripts/count_docs.py` |
| `count_interview.py` | interview 题量 ≥150 | `python3 scripts/count_interview.py` |
| `gen_vehicles.py`(Phase 4) | 车联网 10 万车辆 GPS/CAN/DTC 模拟器 | 案例三专用 |
| `gen_logs.py`(Phase 4) | TB 级日志回放器(限速/突发两种模式) | 案例一专用 |

规范:脚本必须幂等、可 Ctrl+C 安全退出、参数有默认值且默认值与 docker 环境自洽。
