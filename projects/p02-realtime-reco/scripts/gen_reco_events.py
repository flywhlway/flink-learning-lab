#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
"""
p02 用户行为可判定造数（D-09 / RECO-01）。

用法:
    uv run scripts/gen_reco_events.py --scenario feature-score
    uv run scripts/gen_reco_events.py --rate 100 --duration 30
    make gen

宿主机 bootstrap 默认 localhost:9094（容器内 Flink 用 kafka:9092，勿混用）。
速率/时长上限：--rate≤5000、--duration≤600（T-05-04）。
"""
from __future__ import annotations

import argparse
import json
import sys
import time

from confluent_kafka import Producer

SCENARIOS = ("feature-score",)
MAX_RATE = 5000
MAX_DURATION = 600

# 与 sql/postgres_reco_items.sql 种子对齐的可判定 item
KNOWN_ITEMS = (
    ("i-001", "VIEW"),
    ("i-001", "CLICK"),
    ("i-006", "VIEW"),
    ("i-011", "CART"),
    ("i-001", "BUY"),
    ("i-016", "CLICK"),
    ("i-021", "VIEW"),
)


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="p02 reco behavior event generator")
    p.add_argument("--bootstrap", default="localhost:9094")
    p.add_argument("--topic", default="reco.events")
    p.add_argument(
        "--scenario",
        choices=list(SCENARIOS),
        default=None,
        help="feature-score：同 user 多 eventType + 已知 itemId（特征/打分可判定）",
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
    p.add_argument("--user", default="u-score-1", help="scenario/压测默认 userId")
    return p.parse_args()


def emit(producer: Producer, topic: str, event: dict) -> None:
    producer.produce(
        topic,
        json.dumps(event, separators=(",", ":")).encode(),
        key=event["userId"].encode(),
    )
    producer.poll(0)


def make_event(
    user_id: str,
    item_id: str,
    event_type: str,
    event_time: int,
) -> dict:
    return {
        "userId": user_id,
        "itemId": item_id,
        "eventType": event_type,
        "eventTime": event_time,
    }


def scenario_feature_score(producer: Producer, topic: str, user_id: str) -> int:
    """可判定特征/打分序列：同 user 多 eventType + PG 种子内已知 itemId。"""
    base = int(time.time() * 1000)
    n = 0
    for i, (item_id, event_type) in enumerate(KNOWN_ITEMS):
        emit(
            producer,
            topic,
            make_event(user_id, item_id, event_type, base + i * 1_000),
        )
        n += 1
    # 尾事件推进 watermark（ooo=5s）
    emit(
        producer,
        topic,
        make_event("u-wm-tail", "i-050", "VIEW", base + 12_000),
    )
    n += 1
    producer.flush()
    return n


def run_rate(producer: Producer, topic: str, user_id: str, rate: int, duration: int) -> int:
    """压测钩子：恒定速率 VIEW 事件（后续 loadtest 复用）。"""
    if rate < 1 or rate > MAX_RATE:
        raise SystemExit(f"FAIL: --rate 须在 1..{MAX_RATE}，got={rate}")
    if duration < 1 or duration > MAX_DURATION:
        raise SystemExit(f"FAIL: --duration 须在 1..{MAX_DURATION}，got={duration}")
    interval = 1.0 / rate
    deadline = time.time() + duration
    n = 0
    items = [f"i-{i:03d}" for i in range(1, 51)]
    types = ("VIEW", "CLICK", "CART", "BUY")
    while time.time() < deadline:
        t0 = time.time()
        item_id = items[n % len(items)]
        event_type = types[n % len(types)]
        emit(
            producer,
            topic,
            make_event(user_id, item_id, event_type, int(t0 * 1000)),
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
            "FAIL: 请指定 --scenario feature-score 或同时指定 --rate 与 --duration",
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
    if args.scenario == "feature-score":
        n = scenario_feature_score(producer, args.topic, args.user)
        print(f"ok scenario=feature-score events={n} topic={args.topic}")
        return 0
    n = run_rate(producer, args.topic, args.user, args.rate, args.duration)
    print(f"ok rate={args.rate} duration={args.duration} events={n} topic={args.topic}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
