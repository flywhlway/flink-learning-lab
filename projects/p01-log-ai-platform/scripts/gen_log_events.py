#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
"""
p01 结构化日志可判定造数（D-09 / D-13）。

用法:
    uv run scripts/gen_log_events.py --scenario rule-auth-fail
    uv run scripts/gen_log_events.py --scenario rule-error-burst
    uv run scripts/gen_log_events.py --rate 100 --duration 30
    make gen

宿主机 bootstrap 默认 localhost:9094（容器内 Flink 用 kafka:9092，勿混用）。
速率/时长上限：--rate≤5000、--duration≤600。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import uuid

from confluent_kafka import Producer

SCENARIOS = (
    "rule-auth-fail",
    "rule-error-burst",
)
MAX_RATE = 5000
MAX_DURATION = 600


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="p01 log event generator")
    p.add_argument("--bootstrap", default="localhost:9094")
    p.add_argument("--topic", default="logs.events")
    p.add_argument(
        "--scenario",
        choices=list(SCENARIOS),
        default=None,
        help="rule-auth-fail（AUTH_FAIL）/ rule-error-burst（ERROR_BURST）",
    )
    p.add_argument(
        "--rate",
        "--eps",
        dest="rate",
        type=int,
        default=None,
        metavar="N",
        help="每秒事件数（--eps 别名）；与 --duration 组成速率模式（压测钩子）",
    )
    p.add_argument(
        "--duration",
        type=int,
        default=None,
        metavar="SEC",
        help="速率模式持续时间（秒）",
    )
    p.add_argument("--service", default="auth-svc")
    return p.parse_args()


def emit(producer: Producer, topic: str, event: dict) -> None:
    producer.produce(
        topic,
        json.dumps(event, separators=(",", ":")).encode(),
        key=event["service"].encode(),
    )
    producer.poll(0)


def make_event(
    service: str,
    level: str,
    message: str,
    event_time: int,
    trace_id: str | None = None,
) -> dict:
    return {
        "service": service,
        "level": level,
        "message": message,
        "traceId": trace_id or f"tr-{uuid.uuid4().hex[:12]}",
        "eventTime": event_time,
    }


def scenario_rule_auth_fail(producer: Producer, topic: str, service: str) -> int:
    """可判定 AUTH_FAIL：ERROR + auth 服务 + authentication failed 文案。"""
    base = int(time.time() * 1000)
    events = [
        make_event(
            service,
            "INFO",
            "login attempt started",
            base,
            "tr-auth-setup",
        ),
        make_event(
            service,
            "ERROR",
            "authentication failed for user alice",
            base + 1_000,
            "tr-auth-fail-1",
        ),
        make_event(
            service,
            "ERROR",
            "authentication failed for user bob",
            base + 2_000,
            "tr-auth-fail-2",
        ),
        # 尾事件推进 watermark（ooo=5s）
        make_event(
            "billing-svc",
            "INFO",
            "heartbeat ping",
            base + 12_000,
            "tr-wm-tail",
        ),
    ]
    for ev in events:
        emit(producer, topic, ev)
    producer.flush()
    return len(events)


def scenario_rule_error_burst(producer: Producer, topic: str, service: str) -> int:
    """连发 ERROR 推高 Keyed 特征计数，供 ERROR_BURST（阈值 5）。"""
    base = int(time.time() * 1000)
    svc = service if service != "auth-svc" else "payments-svc"
    n = 0
    for i in range(6):
        emit(
            producer,
            topic,
            make_event(
                svc,
                "ERROR",
                f"upstream timeout batch={i}",
                base + i * 500,
                f"tr-burst-{i}",
            ),
        )
        n += 1
    emit(
        producer,
        topic,
        make_event("billing-svc", "INFO", "heartbeat ping", base + 12_000, "tr-wm-tail"),
    )
    n += 1
    producer.flush()
    return n


def run_rate(producer: Producer, topic: str, service: str, rate: int, duration: int) -> int:
    """压测钩子：恒定速率 INFO 事件（后续 loadtest 复用）。"""
    if rate < 1 or rate > MAX_RATE:
        raise SystemExit(f"FAIL: --rate 须在 1..{MAX_RATE}，got={rate}")
    if duration < 1 or duration > MAX_DURATION:
        raise SystemExit(f"FAIL: --duration 须在 1..{MAX_DURATION}，got={duration}")
    interval = 1.0 / rate
    deadline = time.time() + duration
    n = 0
    while time.time() < deadline:
        t0 = time.time()
        emit(
            producer,
            topic,
            make_event(
                service,
                "INFO",
                f"loadtest tick n={n}",
                int(t0 * 1000),
            ),
        )
        n += 1
        slept = time.time() - t0
        if slept < interval:
            time.sleep(interval - slept)
    producer.flush()
    return n


def main() -> int:
    args = build_args()
    if args.scenario and (args.rate is not None or args.duration is not None):
        print("FAIL: --scenario 与 --rate/--duration 互斥", file=sys.stderr)
        return 1
    if args.scenario is None and (args.rate is None or args.duration is None):
        print(
            "FAIL: 请指定 --scenario rule-auth-fail|rule-error-burst "
            "或同时指定 --rate 与 --duration",
            file=sys.stderr,
        )
        return 1
    if args.rate is not None and args.duration is None:
        print("FAIL: --rate 须与 --duration 同时给出", file=sys.stderr)
        return 1
    if args.duration is not None and args.rate is None:
        print("FAIL: --duration 须与 --rate 同时给出", file=sys.stderr)
        return 1

    producer = Producer({"bootstrap.servers": args.bootstrap})
    if args.scenario == "rule-auth-fail":
        n = scenario_rule_auth_fail(producer, args.topic, args.service)
        print(f"ok scenario=rule-auth-fail events={n} topic={args.topic}")
        return 0
    if args.scenario == "rule-error-burst":
        n = scenario_rule_error_burst(producer, args.topic, args.service)
        print(f"ok scenario=rule-error-burst events={n} topic={args.topic}")
        return 0
    n = run_rate(producer, args.topic, args.service, args.rate, args.duration)
    print(f"ok rate={args.rate} duration={args.duration} events={n} topic={args.topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
