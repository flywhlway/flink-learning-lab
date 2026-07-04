#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["confluent-kafka>=2.5"]
# ///
"""
点击流数据生成器(e01 及后续模块通用)。

用法(仓库任意位置):
    uv run scripts/gen_events.py --topic clicks --eps 200
    uv run scripts/gen_events.py --topic clicks --eps 2000 --skew-page /home --skew-ratio 0.7

特性:
- 事件时间打点 + 可控乱序(默认 ≤5s,与 e01 作业的 watermark 上界一致)
- 可注入页面维度倾斜(为 Level 6 数据倾斜实验预留)
- 宿主机默认 bootstrap = localhost:9094(容器内 Flink 用 kafka:9092,勿混用)
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time

from confluent_kafka import Producer

PAGES = ["/home", "/search", "/item", "/cart", "/pay", "/profile"]


def build_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="clickstream generator")
    p.add_argument("--bootstrap", default="localhost:9094")
    p.add_argument("--topic", default="clicks")
    p.add_argument("--eps", type=int, default=200, help="events per second")
    p.add_argument("--users", type=int, default=1000, help="user id 基数")
    p.add_argument("--max-lag-ms", type=int, default=5000, help="事件时间最大乱序")
    p.add_argument("--skew-page", default=None, help="倾斜页面,如 /home")
    p.add_argument("--skew-ratio", type=float, default=0.0, help="倾斜比例 0~1")
    p.add_argument("--total", type=int, default=0, help="总条数,0=无限")
    return p.parse_args()


def main() -> None:
    args = build_args()
    producer = Producer({
        "bootstrap.servers": args.bootstrap,
        "linger.ms": 20,
        "batch.num.messages": 10_000,
    })

    running = True

    def stop(*_: object) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    sent = 0
    tick = time.monotonic()
    print(f"producing → {args.bootstrap} topic={args.topic} eps={args.eps} (Ctrl+C 停止)")

    while running and (args.total == 0 or sent < args.total):
        batch_deadline = tick + 1.0
        for _ in range(args.eps):
            if args.skew_page and random.random() < args.skew_ratio:
                page = args.skew_page
            else:
                page = random.choice(PAGES)
            event = {
                "userId": f"u{random.randint(1, args.users)}",
                "page": page,
                # 事件时间:当前时刻回拨一个随机乱序量(模拟客户端上报延迟)
                "ts": int(time.time() * 1000) - random.randint(0, args.max_lag_ms),
            }
            producer.produce(args.topic, json.dumps(event).encode(),
                             key=event["userId"].encode())
            sent += 1
        producer.poll(0)
        if sent % (args.eps * 10) < args.eps:
            print(f"  sent={sent}")
        sleep = batch_deadline - time.monotonic()
        if sleep > 0:
            time.sleep(sleep)
        tick = batch_deadline

    producer.flush(10)
    print(f"done, total sent={sent}")
    sys.exit(0)


if __name__ == "__main__":
    main()
