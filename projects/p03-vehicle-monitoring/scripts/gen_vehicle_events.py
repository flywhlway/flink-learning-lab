#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
"""
p03 车联网可判定造数：三场景 + 速率压测 + 冻结 eventTime + 可选控制面发布（D-07/D-10/D-12）。

用法:
    uv run scripts/gen_vehicle_events.py --scenario match-harsh-fault
    uv run scripts/gen_vehicle_events.py --scenario match-triple-harsh
    uv run scripts/gen_vehicle_events.py --scenario match-dtc-pair
    uv run scripts/gen_vehicle_events.py --publish-control '{"activePatterns":["TRIPLE_HARSH"],"version":2}'
    uv run scripts/gen_vehicle_events.py --rate 100 --duration 120
    uv run scripts/gen_vehicle_events.py --eps 100 --duration 45   # --eps 为 --rate 别名
    uv run scripts/gen_vehicle_events.py --partial harsh-open --vin VIN-STALL-001
    uv run scripts/gen_vehicle_events.py --frozen-event-time --duration 50 --rate 2 --vin VIN-STALL-001
    make gen

模式互斥规则（--help 同源）:
  - --scenario / --partial：离散剧本（一次发完）
  - --rate|--eps + --duration：恒定速率，eventTime 随墙钟推进（压测）
  - --frozen-event-time + --duration：HEARTBEAT 涓流，eventTime 固定为 T0（watermark 停滞演练）
  - --publish-control 可与上述任一模式组合
  - 纯 --scenario 与纯速率/冻结模式互斥；勿同时指定 --scenario 与 --rate/--frozen-event-time

宿主机 bootstrap 默认 localhost:9094（容器内 Flink 用 kafka:9092，勿混用）。
控制 topic 默认 vehicle.pattern.control（可用 --control-topic 覆盖）。
速率/时长上限：--rate≤5000、--duration≤600（防误打云端；默认仍指向 localhost:9094）。
"""
from __future__ import annotations

import argparse
import json
import random
import signal
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
PARTIALS = ("harsh-open",)
MAX_RATE = 5000
MAX_DURATION = 600


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="p03 vehicle event generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "模式互斥:\n"
            "  --scenario / --partial  离散剧本（一次发完）\n"
            "  --rate|--eps + --duration  恒定速率，advancing eventTime（压测）\n"
            "  --frozen-event-time + --duration  冻结 eventTime=T0 的 HEARTBEAT 涓流（停滞演练）\n"
            "  --publish-control 可与上述组合；勿同时指定 --scenario 与 --rate/--frozen-event-time\n"
        ),
    )
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
        "--partial",
        choices=list(PARTIALS),
        default=None,
        help="部分 CEP 序列：harsh-open = HEARTBEAT+HARSH（无 DTC/尾心跳），供停滞演练",
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
    p.add_argument(
        "--rate",
        "--eps",
        dest="rate",
        type=int,
        default=None,
        metavar="N",
        help="每秒事件数（--eps 为别名）；与 --duration 组成速率模式",
    )
    p.add_argument(
        "--duration",
        type=int,
        default=None,
        metavar="SEC",
        help="速率/冻结模式持续时间（秒）",
    )
    p.add_argument(
        "--frozen-event-time",
        action="store_true",
        help="冻结 eventTime=T0 的 HEARTBEAT 涓流（需 --duration；可选 --rate 默认 2）",
    )
    p.add_argument(
        "--freeze-at",
        type=int,
        default=None,
        metavar="EPOCH_MS",
        help="冻结的 eventTime（毫秒）；默认取启动时刻；仅 --frozen-event-time 有效",
    )
    p.add_argument(
        "--vin-count",
        type=int,
        default=1,
        help="速率模式下轮换 vin 数量（默认 1；压测可调大分散 key）",
    )
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


def partial_harsh_open(producer: Producer, topic: str, vin: str) -> tuple[int, int]:
    """部分序列：HEARTBEAT + HARSH，无 DTC/尾心跳。返回 (sent, freeze_at_ms=HARSH.eventTime)。"""
    base = int(time.time() * 1000)
    sequence = [
        {"vin": vin, "signalType": "HEARTBEAT", "value": 1.0, "eventTime": base},
        {
            "vin": vin,
            "signalType": "HARSH_ACCEL",
            "value": 500.0,
            "eventTime": base + 2_000,
        },
    ]
    for ev in sequence:
        emit(producer, topic, ev)
        print(f"  sent {ev['signalType']} value={ev['value']} ts={ev['eventTime']}")
    freeze_at = sequence[-1]["eventTime"]
    print(f"  partial harsh-open freeze_hint={freeze_at}")
    return len(sequence), freeze_at


def run_rate_mode(
    producer: Producer,
    topic: str,
    vin_base: str,
    rate: int,
    duration: int,
    vin_count: int,
) -> int:
    """恒定速率：HEARTBEAT/HARSH 混合，eventTime 随墙钟推进（对齐 gen_events.py 循环）。"""
    if rate < 1 or rate > MAX_RATE:
        raise SystemExit(f"FAIL: --rate 须在 1..{MAX_RATE}，got={rate}")
    if duration < 1 or duration > MAX_DURATION:
        raise SystemExit(f"FAIL: --duration 须在 1..{MAX_DURATION}，got={duration}")
    if vin_count < 1:
        raise SystemExit("FAIL: --vin-count 须 ≥1")

    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent = 0
    deadline = time.monotonic() + duration
    tick = time.monotonic()
    print(
        f"rate-mode → {topic} rate={rate} duration={duration}s "
        f"vin_count={vin_count} advancing eventTime"
    )
    while running and time.monotonic() < deadline:
        batch_deadline = tick + 1.0
        for i in range(rate):
            if time.monotonic() >= deadline or not running:
                break
            vin = vin_base if vin_count == 1 else f"{vin_base}-{i % vin_count:04d}"
            # ~80% HEARTBEAT / ~20% HARSH，模拟遥测混合
            if random.random() < 0.2:
                signal_type, value = "HARSH_ACCEL", 480.0 + random.random() * 40.0
            else:
                signal_type, value = "HEARTBEAT", 1.0
            event = {
                "vin": vin,
                "signalType": signal_type,
                "value": value,
                "eventTime": int(time.time() * 1000),
            }
            emit(producer, topic, event)
            sent += 1
        producer.poll(0)
        if sent > 0 and sent % max(rate * 10, 1) < rate:
            print(f"  sent={sent}")
        sleep = batch_deadline - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)
        tick = batch_deadline
    return sent


def run_frozen_mode(
    producer: Producer,
    topic: str,
    vin: str,
    rate: int,
    duration: int,
    freeze_at: int | None,
    vin_count: int = 1,
) -> int:
    """冻结 eventTime=T0 的 HEARTBEAT 涓流：源不 idle，但 watermark 卡在 T0-ooo。

    vin_count>1 时轮换 key，尽量覆盖多 Kafka 分区，避免仅靠空闲分区演示停滞。
    """
    if rate < 1 or rate > MAX_RATE:
        raise SystemExit(f"FAIL: --rate 须在 1..{MAX_RATE}，got={rate}")
    if duration < 1 or duration > MAX_DURATION:
        raise SystemExit(f"FAIL: --duration 须在 1..{MAX_DURATION}，got={duration}")
    if vin_count < 1:
        raise SystemExit("FAIL: --vin-count 须 ≥1")

    t0 = freeze_at if freeze_at is not None else int(time.time() * 1000)
    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent = 0
    deadline = time.monotonic() + duration
    tick = time.monotonic()
    print(
        f"frozen-mode → {topic} rate={rate} duration={duration}s "
        f"vin={vin} vin_count={vin_count} frozen_eventTime={t0}"
    )
    while running and time.monotonic() < deadline:
        batch_deadline = tick + 1.0
        for i in range(rate):
            if time.monotonic() >= deadline or not running:
                break
            key = vin if vin_count == 1 else f"{vin}-{i % vin_count:04d}"
            event = {
                "vin": key,
                "signalType": "HEARTBEAT",
                "value": 1.0,
                "eventTime": t0,
            }
            emit(producer, topic, event)
            sent += 1
        producer.poll(0)
        if sent > 0 and sent % max(rate * 10, 1) < rate:
            print(f"  sent={sent} frozen_ts={t0}")
        sleep = batch_deadline - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)
        tick = batch_deadline
    return sent


def resolve_scenario(name: str) -> str:
    return SCENARIO_ALIASES.get(name, name)


def validate_mode(args: argparse.Namespace) -> None:
    discrete = args.scenario is not None or args.partial is not None
    rate_mode = args.rate is not None and not args.frozen_event_time
    frozen_mode = args.frozen_event_time

    if args.publish_control is None and not discrete and not rate_mode and not frozen_mode:
        print(
            "FAIL: 需指定 --scenario / --partial / (--rate + --duration) / "
            "--frozen-event-time / --publish-control 之一",
            file=sys.stderr,
        )
        sys.exit(2)

    if discrete and (rate_mode or frozen_mode):
        print(
            "FAIL: --scenario/--partial 与 --rate/--frozen-event-time 互斥",
            file=sys.stderr,
        )
        sys.exit(2)

    if rate_mode and frozen_mode:
        print("FAIL: 速率模式与 --frozen-event-time 互斥", file=sys.stderr)
        sys.exit(2)

    if args.scenario is not None and args.partial is not None:
        print("FAIL: --scenario 与 --partial 互斥", file=sys.stderr)
        sys.exit(2)

    if rate_mode and args.duration is None:
        print("FAIL: 速率模式需要 --duration", file=sys.stderr)
        sys.exit(2)

    if frozen_mode and args.duration is None:
        print("FAIL: --frozen-event-time 需要 --duration", file=sys.stderr)
        sys.exit(2)

    if args.duration is not None and not rate_mode and not frozen_mode and not discrete:
        print("FAIL: --duration 须配合 --rate 或 --frozen-event-time", file=sys.stderr)
        sys.exit(2)


def main() -> None:
    args = build_args()
    validate_mode(args)

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

    if args.partial is not None:
        print(
            f"producing → {args.bootstrap} topic={args.topic} "
            f"partial={args.partial} vin={args.vin}"
        )
        if args.partial == "harsh-open":
            n, _ = partial_harsh_open(producer, args.topic, args.vin)
            sent += n
        else:
            print(f"未知 partial: {args.partial}", file=sys.stderr)
            sys.exit(1)

    if args.frozen_event_time:
        rate = args.rate if args.rate is not None else 2
        sent += run_frozen_mode(
            producer,
            args.topic,
            args.vin,
            rate,
            args.duration,
            args.freeze_at,
            args.vin_count,
        )

    if args.rate is not None and not args.frozen_event_time:
        print(
            f"producing → {args.bootstrap} topic={args.topic} "
            f"rate={args.rate} duration={args.duration}s vin={args.vin}"
        )
        sent += run_rate_mode(
            producer,
            args.topic,
            args.vin,
            args.rate,
            args.duration,
            args.vin_count,
        )

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
