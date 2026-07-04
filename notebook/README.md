# notebook/ · PyFlink / Jupyter 环境

**版本要点**:PyFlink 对 Python 版本有上限约束(通常滞后于最新 CPython)。本机 Python 3.13 作为默认工具链没问题,但 **PyFlink 实验一律用 uv 固定 3.12**:

```bash
cd notebook
uv init --python 3.12 pyflink-lab && cd pyflink-lab
uv add apache-flink jupyterlab
uv run jupyter lab
```

> 若 `apache-flink` 在 3.12 安装受阻(依赖轮子滞后),降到 3.11:`uv python pin 3.11`。arm64 mac 上 PyFlink 自带的 JVM 桥接正常工作,无需 Rosetta。

定位:PyFlink 在本仓库是**辅助视角**(交互探索、Table API 快速验证、AI 生态胶水),生产主线仍是 Java 21。首批 notebook(P2):Table API 交互式教程、与 e05 SQL 案例的对照实验。
