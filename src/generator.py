"""Synthetic experiment event generator.

Emits an assignment event (with a pre-experiment covariate) and an engagement event per user.
Optional faults: a bucketing bug that skews assignment (to trigger SRM), late events, and very
late events (to fall behind the watermark).
"""
import argparse
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import numpy as np


def make_user_events(user_id, rng, now, bug_active=False, lift=0.6,
                     late_rate=0.02, very_late_rate=0.005):
    pre = float(rng.gamma(2.0, 5.0))                       # pre-experiment engagement, mean 10
    p_treat = 0.62 if bug_active else 0.50                 # bug: treatment over-assigned
    variant = "treatment" if rng.random() < p_treat else "control"
    y = max(0.0, 0.8 * pre + float(rng.normal(0, 4)) + (lift if variant == "treatment" else 0.0))

    r = rng.random()
    if r < very_late_rate:
        ts = now - timedelta(seconds=300)                  # beyond the 2 minute watermark
    elif r < very_late_rate + late_rate:
        ts = now - timedelta(seconds=90)                   # late but within the watermark
    else:
        ts = now
    base = {"user_id": user_id, "variant": variant, "ts": ts.isoformat()}
    return [
        {**base, "event_id": str(uuid.uuid4()), "type": "assignment", "value": 0.0, "pre_metric": pre},
        {**base, "event_id": str(uuid.uuid4()), "type": "engagement", "value": y, "pre_metric": pre},
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", type=int, default=6000)
    ap.add_argument("--rate", type=float, default=200, help="users per second")
    ap.add_argument("--bug-after", type=int, default=-1, help="user index after which bucketing skews")
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="events-live")
    ap.add_argument("--out", default="data/events.jsonl")
    ap.add_argument("--no-kafka", action="store_true")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    rng = np.random.default_rng(args.seed)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    producer = None
    if not args.no_kafka:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True})

    start = time.time()
    with open(args.out, "w") as f:
        for i in range(args.users):
            bug = args.bug_after >= 0 and i >= args.bug_after
            for e in make_user_events(f"u{i}", rng, datetime.now(timezone.utc), bug_active=bug):
                line = json.dumps(e)
                f.write(line + "\n")
                if producer:
                    producer.produce(args.topic, key=e["user_id"], value=line)
            if producer:
                producer.poll(0)
            if not args.no_kafka:
                delay = start + (i + 1) / args.rate - time.time()
                if delay > 0:
                    time.sleep(delay)
    if producer:
        producer.flush()
    print(f"wrote {args.users} users to {args.out}" + ("" if args.no_kafka else f" and topic '{args.topic}'"))


if __name__ == "__main__":
    main()
