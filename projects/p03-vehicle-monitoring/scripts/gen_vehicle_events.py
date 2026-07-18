#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
"""
p03 车联网可判定造数：三场景 + 可选控制面发布（D-12）。

用法:
    uv run scripts/gen_vehicle_events.py --scenario match-harsh-fault
    uv run scripts/gen_vehicle_events.py --scenario match-triple-harsh
    uv run scripts/gen_vehicle_events.py --scenario match-dtc-pair
    uv run scripts/gen_vehicle_events.py --publish-control '{"activePatterns":["TRIPLE_HARSH"],"version":2}'
    make gen

宿主机 bootstrap 默认 localhost:9094（容器内 Flink 用 kafka:9092，勿混用）。
控制 topic 默认 vehicle.pattern.control（可用 --control-topic 覆盖）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from confluent_kafka import Producer

# match 为 Phase 1 别名 → match-harsh-fault（D-12）
SCENARIO_ALIASES = {
    "match": "match-harsh-fault",
}
SCENARIOS = (
    "match",
    "match-harsh-fault",
    "match-triple-harsh",
    "match-dtc-pair",
)


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="p03 vehicle event generator")
    p.add_argument("--bootstrap", default="localhost:9094")
    p.add_argument("--topic", default="vehicle.events")
    p.add_argument(
        "--control-topic",
        default="vehicle.pattern.control",
        help="Broadcast 控制面 topic（D-04）",
    )
    p.add_argument(
        "--scenario",
        choices=list(SCENARIOS),
        default=None,
        help=(
            "match(=match-harsh-fault) / match-harsh-fault / "
            "match-triple-harsh / match-dtc-pair"
        ),
    )
    p.add_argument(
        "--publish-control",
        default=None,
        metavar="JSON_OR_PATH",
        help=(
            "向 control topic 发送确定性 JSON（字符串或文件路径），"
            '例: \'{"activePatterns":["TRIPLE_HARSH"],"version":2}\''
        ),
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


def emit_control(producer: Producer, topic: str, payload: dict) -> None:
    """控制消息无 vin key；整包 JSON 作为 value（D-04）。"""
    body = json.dumps(payload, separators=(",", ":")).encode()
    producer.produce(topic, body)
    producer.poll(0)


def load_control_payload(raw: str) -> dict:
    """接受 JSON 字符串或指向 JSON 文件的路径。"""
    path = Path(raw)
    if path.is_file():
        text = path.read_text(encoding="utf-8")
    else:
        text = raw
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"FAIL: --publish-control 不是合法 JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise SystemExit("FAIL: --publish-control 顶层必须是 JSON object")
    if "activePatterns" not in payload or "version" not in payload:
        raise SystemExit(
            "FAIL: --publish-control 必须含 activePatterns 与 version（D-04）"
        )
    if not isinstance(payload["activePatterns"], list):
        raise SystemExit("FAIL: activePatterns 必须是数组")
    return payload


def _tail_heartbeats(vin: str, base: int, offsets: tuple[int, ...] = (12_000, 18_000)) -> list[dict]:
    """尾心跳推进 watermark：须把水位推过末业务事件（ooo=5s）。"""
    return [
        {
            "vin": vin,
            "signalType": "HEARTBEAT",
            "value": 1.0,
            "eventTime": base + off,
        }
        for off in offsets
    ]


def scenario_match_harsh_fault(producer: Producer, topic: str, vin: str) -> int:
    """同 vin：HEARTBEAT → HARSH_ACCEL(500) → HEARTBEAT → DTC(500) + 尾心跳。

    作业 watermark = maxEventTime - 5s（BoundedOutOfOrderness）；尾心跳须把水位推过 DTC
    事件时间，否则 CEP MATCH 可能迟迟不落库。
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
        *_tail_heartbeats(vin, base),
    ]
    for ev in sequence:
        emit(producer, topic, ev)
        print(f"  sent {ev['signalType']} value={ev['value']} ts={ev['eventTime']}")
    return len(sequence)


def scenario_match_triple_harsh(producer: Producer, topic: str, vin: str) -> int:
    """连续 3× HARSH_ACCEL(value>450)；中间禁止 HEARTBEAT（consecutive）；+ 尾心跳。

    Pitfall 2：times(3).consecutive() 被 HEARTBEAT 打断则永不 MATCH。
    """
    base = int(time.time() * 1000)
    sequence = [
        {"vin": vin, "signalType": "HEARTBEAT", "value": 1.0, "eventTime": base},
        {
            "vin": vin,
            "signalType": "HARSH_ACCEL",
            "value": 500.0,
            "eventTime": base + 1_000,
        },
        {
            "vin": vin,
            "signalType": "HARSH_ACCEL",
            "value": 510.0,
            "eventTime": base + 2_000,
        },
        {
            "vin": vin,
            "signalType": "HARSH_ACCEL",
            "value": 520.0,
            "eventTime": base + 3_000,
        },
        # 第三次 HARSH@+3s；尾心跳推过末事件 + ooo
        *_tail_heartbeats(vin, base),
    ]
    # 断言：三次 HARSH 之间无 HEARTBEAT
    harsh_idx = [
        i for i, e in enumerate(sequence) if e["signalType"] == "HARSH_ACCEL"
    ]
    if len(harsh_idx) != 3:
        raise SystemExit("internal: expected exactly 3 HARSH_ACCEL")
    for i in range(harsh_idx[0], harsh_idx[-1] + 1):
        if sequence[i]["signalType"] == "HEARTBEAT":
            raise SystemExit("internal: HEARTBEAT between consecutive HARSH")
    for ev in sequence:
        emit(producer, topic, ev)
        print(f"  sent {ev['signalType']} value={ev['value']} ts={ev['eventTime']}")
    return len(sequence)


def scenario_match_dtc_pair(producer: Producer, topic: str, vin: str) -> int:
    """两 DTC(value>480) 落在 15s 窗内 + 尾心跳。"""
    base = int(time.time() * 1000)
    sequence = [
        {"vin": vin, "signalType": "HEARTBEAT", "value": 1.0, "eventTime": base},
        {
            "vin": vin,
            "signalType": "DTC",
            "value": 500.0,
            "eventTime": base + 2_000,
        },
        {
            "vin": vin,
            "signalType": "DTC",
            "value": 510.0,
            "eventTime": base + 5_000,
        },
        *_tail_heartbeats(vin, base),
    ]
    for ev in sequence:
        emit(producer, topic, ev)
        print(f"  sent {ev['signalType']} value={ev['value']} ts={ev['eventTime']}")
    return len(sequence)


def resolve_scenario(name: str) -> str:
    return SCENARIO_ALIASES.get(name, name)


def main() -> None:
    args = build_args()
    if args.publish_control is None and args.scenario is None:
        print(
            "FAIL: 需指定 --scenario 和/或 --publish-control",
            file=sys.stderr,
        )
        sys.exit(2)

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap,
            "linger.ms": 5,
            "acks": "all",
        }
    )

    sent = 0
    if args.publish_control is not None:
        payload = load_control_payload(args.publish_control)
        print(
            f"publishing control → {args.bootstrap} "
            f"topic={args.control_topic} payload={payload}"
        )
        emit_control(producer, args.control_topic, payload)
        print(
            f"  sent control activePatterns={payload['activePatterns']} "
            f"version={payload['version']}"
        )
        sent += 1

    if args.scenario is not None:
        scenario = resolve_scenario(args.scenario)
        print(
            f"producing → {args.bootstrap} topic={args.topic} "
            f"scenario={scenario} (arg={args.scenario}) vin={args.vin}"
        )
        if scenario == "match-harsh-fault":
            sent += scenario_match_harsh_fault(producer, args.topic, args.vin)
        elif scenario == "match-triple-harsh":
            sent += scenario_match_triple_harsh(producer, args.topic, args.vin)
        elif scenario == "match-dtc-pair":
            sent += scenario_match_dtc_pair(producer, args.topic, args.vin)
        else:
            print(f"未知 scenario: {scenario}", file=sys.stderr)
            sys.exit(1)

    remaining = producer.flush(10)
    if remaining:
        print(f"FAIL: {remaining} messages still in queue", file=sys.stderr)
        sys.exit(1)
    print(f"done, total sent={sent}")
    sys.exit(0)


if __name__ == "__main__":
    main()
