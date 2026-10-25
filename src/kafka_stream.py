"""Live Kafka consumer: per-variant counts, engagement means, and a cumulative SRM check.

The aggregation logic lives in LiveAggregator so it can be unit tested without Kafka.
Run from anywhere; the consumer needs a running broker (see README).
"""
import argparse
import json
import os
import sys
import time
import uuid
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from stats import srm_check  # noqa: E402

VARIANTS = ("control", "treatment")


class LiveAggregator:
    """Cumulative per-variant state. update() returns a report dict at each reporting boundary."""

    def __init__(self, report_every=500):
        self.report_every = report_every
        self.assignments = defaultdict(int)
        self.engagement_sum = defaultdict(float)
        self.engagement_n = defaultdict(int)
        self.processed = 0
        self._last_bucket = 0

    def update(self, event):
        variant = event.get("variant")
        if variant not in VARIANTS:
            return None
        kind = event.get("type")
        if kind == "assignment":
            self.assignments[variant] += 1
        elif kind == "engagement":
            self.engagement_sum[variant] += float(event.get("value", 0.0))
            self.engagement_n[variant] += 1
        else:
            return None
        self.processed += 1

        # Report on engagement events, so the means include the user that triggered the report.
        if kind != "engagement":
            return None
        bucket = sum(self.engagement_n.values()) // self.report_every
        if bucket <= self._last_bucket or not all(self.assignments[v] for v in VARIANTS):
            return None
        self._last_bucket = bucket
        return self.report()

    def report(self):
        counts = {v: self.assignments[v] for v in VARIANTS}
        srm = srm_check(counts, {v: 1.0 for v in VARIANTS})
        means = {v: (self.engagement_sum[v] / self.engagement_n[v] if self.engagement_n[v] else 0.0)
                 for v in VARIANTS}
        return {"events": self.processed, "users": sum(counts.values()), "counts": counts,
                "srm": srm, "means": means}


def format_report(r):
    p = r["srm"]["p_value"]
    p_text = "n/a (n too small)" if p is None else f"{p:.3g}"
    status = "ALERT: SRM detected" if r["srm"]["alert"] else "OK"
    return (f"events={r['events']} users={r['users']} control={r['counts']['control']} "
            f"treatment={r['counts']['treatment']} SRM_p={p_text} status={status} "
            f"mean_engagement_control={r['means']['control']:.3f} "
            f"mean_engagement_treatment={r['means']['treatment']:.3f}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="events-live")
    ap.add_argument("--report-every", type=int, default=500, help="report every N engagement events")
    ap.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = no limit)")
    ap.add_argument("--idle-timeout", type=float, default=15.0,
                    help="stop after this many seconds without messages, once data has arrived")
    args = ap.parse_args()

    from confluent_kafka import Consumer  # imported here so tests do not need the Kafka client

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"exp-streaming-{uuid.uuid4().hex[:10]}",   # fresh group: replays from earliest
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])
    agg = LiveAggregator(args.report_every)
    last_msg = None
    print(f"Listening to Kafka topic '{args.topic}'...", flush=True)

    try:
        while not args.max_events or agg.processed < args.max_events:
            msg = consumer.poll(1.0)
            if msg is None:
                if last_msg is not None and time.time() - last_msg > args.idle_timeout:
                    print(f"No messages for {args.idle_timeout:.0f}s, stopping.", flush=True)
                    break
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}", flush=True)
                continue
            last_msg = time.time()
            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                print(f"Skipping invalid event: {exc}", flush=True)
                continue
            report = agg.update(event)
            if report:
                print(format_report(report), flush=True)
    except KeyboardInterrupt:
        print("\nStopping consumer...", flush=True)
    finally:
        consumer.close()

    print(f"Finished: {agg.processed} events, {sum(agg.assignments.values())} assignments.", flush=True)


if __name__ == "__main__":
    main()
