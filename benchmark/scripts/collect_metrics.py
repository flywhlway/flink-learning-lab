#!/usr/bin/env python3
"""从 Prometheus 刮取 Flink 值班五指标，输出 key=value 行供 run_matrix.sh 拼 baseline 表。

指标名对齐 monitoring/README，禁止臆造 series。
不引入未登记压测镜像（D-02）。
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

# 值班五指标（与 monitoring/README 对齐）
DEFAULT_QUERIES = {
    "backpressure_ms_per_s": "flink_taskmanager_job_task_isBackPressuredTimeMsPerSecond",
    "busy_ms_per_s": "flink_taskmanager_job_task_busyTimeMsPerSecond",
    "last_checkpoint_duration": "flink_jobmanager_job_lastCheckpointDuration",
    "num_restarts": "flink_jobmanager_job_numRestarts",
    "emit_event_time_lag": "flink_taskmanager_job_task_operator_currentEmitEventTimeLag",
}


def prom_query(prom_url: str, query: str) -> dict:
    qs = urllib.parse.urlencode({"query": query})
    url = f"{prom_url.rstrip('/')}/api/v1/query?{qs}"
    with urllib.request.urlopen(url, timeout=10) as resp:
        return json.loads(resp.read().decode())


def scalar_max(payload: dict) -> str:
    results = payload.get("data", {}).get("result", [])
    vals: list[float] = []
    for r in results:
        try:
            vals.append(float(r["value"][1]))
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    return str(max(vals)) if vals else ""


def scalar_avg(payload: dict) -> str:
    results = payload.get("data", {}).get("result", [])
    vals: list[float] = []
    for r in results:
        try:
            vals.append(float(r["value"][1]))
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    if not vals:
        return ""
    return str(sum(vals) / len(vals))


def with_job_filter(query: str, job_name_regex: str | None) -> str:
    if not job_name_regex:
        return query
    # 在 metric 后插入 job_name=~"..."；若已有 `{` 则追加标签
    if "{" in query:
        return query.replace("{", '{job_name=~"' + job_name_regex + '",', 1)
    return f'{query}{{job_name=~"{job_name_regex}"}}'


def from_stdin() -> int:
    raw = sys.stdin.read()
    if not raw.strip():
        print("FAIL: stdin 为空（期望 Prometheus query JSON）", file=sys.stderr)
        return 1
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        print(f"FAIL: stdin 非 JSON: {e}", file=sys.stderr)
        return 1
    print(f"scalar_max={scalar_max(payload)}")
    return 0


def scrape(prom_url: str, job_name_regex: str | None) -> int:
    for key, query in DEFAULT_QUERIES.items():
        q = with_job_filter(query, job_name_regex)
        try:
            payload = prom_query(prom_url, q)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            print(f"FAIL: Prometheus 查询失败 key={key}: {e}", file=sys.stderr)
            return 1
        # lag / backpressure / busy 取 max；其余同
        print(f"{key}={scalar_max(payload)}")
        if key in ("backpressure_ms_per_s", "busy_ms_per_s", "emit_event_time_lag"):
            print(f"{key}_avg={scalar_avg(payload)}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Flink Prometheus 指标采集（baseline 拼表）")
    parser.add_argument(
        "--stdin",
        action="store_true",
        help="从 stdin 读 Prometheus /api/v1/query JSON，输出 scalar_max=",
    )
    parser.add_argument(
        "--prom-url",
        default=os.environ.get("PROM_URL", "http://localhost:9090"),
        help="Prometheus 基址",
    )
    parser.add_argument(
        "--job-name",
        default=os.environ.get("JOB_NAME_REGEX", ""),
        help='可选 job_name=~"regex" 过滤（如 e01.*|e10.*|p03.*）',
    )
    args = parser.parse_args()
    if args.stdin:
        return from_stdin()
    job = args.job_name.strip() or None
    return scrape(args.prom_url, job)


if __name__ == "__main__":
    sys.exit(main())
