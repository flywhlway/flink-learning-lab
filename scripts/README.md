# scripts/ · 工具脚本(uv 单文件脚本规范)

所有脚本采用 PEP 723 内联元数据 + `uv run` 直跑,零虚拟环境管理成本。

| 脚本 | 用途 | 典型用法 |
|---|---|---|
| `gen_events.py` | 点击流生成器(事件时间打点、可控乱序、可注入倾斜) | `uv run scripts/gen_events.py --topic clicks --eps 200` |
| `qa_check.sh`(Phase 1) | 仓库 QA:编译全部模块、校验 compose、扫描断链与"TODO/省略"违禁词 | `bash scripts/qa_check.sh` |
| `gen_vehicles.py`(Phase 4) | 车联网 10 万车辆 GPS/CAN/DTC 模拟器 | 案例三专用 |
| `gen_logs.py`(Phase 4) | TB 级日志回放器(限速/突发两种模式) | 案例一专用 |

规范:脚本必须幂等、可 Ctrl+C 安全退出、参数有默认值且默认值与 docker 环境自洽。
