#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
"""
p03 车联网可判定造数：注入 HARSH_ACCEL → DTC（30s 内）供 CEP MATCH。

用法（项目根或仓库任意位置）:
    uv run scripts/gen_vehicle_events.py --scenario match
    make gen

宿主机 bootstrap 默认 localhost:9094（容器内 Flink 用 kafka:9092，勿混用）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from confluent_kafka import Producer


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="p03 vehicle event generator")
    p.add_argument("--bootstrap", default="localhost:9094")
    p.add_argument("--topic", default="vehicle.events")
    p.add_argument(
        "--scenario",
        choices=["match"],
        default="match",
        help="match: 同 vin 的 HARSH_ACCEL(500)→DTC(500) 夹 HEARTBEAT",
    )
    p.add_argument("--vin", default="VIN-P03-E2E-001")
    return p.parse_args()


def emit(producer: Producer, topic: str, event: dict) -> None:
    producer.produce(
        topic,
        json.dumps(event, separators=(",", ":")).encode(),
        key=event["vin"].encode(),
    )
    producer.poll(0)


def scenario_match(producer: Producer, topic: str, vin: str) -> int:
    """同 vin：HEARTBEAT → HARSH_ACCEL(500) → HEARTBEAT → DTC(500)，再追加推进 watermark 的尾心跳。

    作业 watermark = maxEventTime - 5s（BoundedOutOfOrderness）；尾心跳须把水位推过 DTC
    事件时间，否则 CEP MATCH 可能迟迟不落库（空闲分区 idleness=30s 亦不足以单独放行）。
    """
    base = int(time.time() * 1000)
    sequence = [
        {"vin": vin, "signalType": "HEARTBEAT", "value": 1.0, "eventTime": base},
        {
            "vin": vin,
            "signalType": "HARSH_ACCEL",
            "value": 500.0,
            "eventTime": base + 2_000,
        },
        {
            "vin": vin,
            "signalType": "HEARTBEAT",
            "value": 1.0,
            "eventTime": base + 4_000,
        },
        {
            "vin": vin,
            "signalType": "DTC",
            "value": 500.0,
            "eventTime": base + 6_000,
        },
        # DTC@+6s；+5s ooo 下需 eventTime>=DTC+5s 才能让 watermark 越过 DTC
        {
            "vin": vin,
            "signalType": "HEARTBEAT",
            "value": 1.0,
            "eventTime": base + 12_000,
        },
        {
            "vin": vin,
            "signalType": "HEARTBEAT",
            "value": 1.0,
            "eventTime": base + 18_000,
        },
    ]
    for ev in sequence:
        emit(producer, topic, ev)
        print(f"  sent {ev['signalType']} value={ev['value']} ts={ev['eventTime']}")
    return len(sequence)


def main() -> None:
    args = build_args()
    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 5,
            "acks": "all",
        }
    )
    print(
        f"producing → {args.bootstrap} topic={args.topic} "
        f"scenario={args.scenario} vin={args.vin}"
    )
    if args.scenario == "match":
        sent = scenario_match(producer, args.topic, args.vin)
    else:
        print(f"未知 scenario: {args.scenario}", file=sys.stderr)
        sys.exit(1)

    remaining = producer.flush(10)
    if remaining:
        print(f"FAIL: {remaining} messages still in queue", file=sys.stderr)
        sys.exit(1)
    print(f"done, total sent={sent}")
    sys.exit(0)


if __name__ == "__main__":
    main()
