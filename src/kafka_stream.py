"""Windows-compatible Kafka streaming runner for live experiment monitoring."""
import argparse
import json
import sys
import time
import uuid
from collections import defaultdict

from confluent_kafka import Consumer

sys.path.insert(0, "src")
from stats import srm_check


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="events-live")
    parser.add_argument("--report-every", type=int, default=500)
    parser.add_argument("--max-events", type=int, default=12000)
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": args.bootstrap,
        "group.id": f"exp-streaming-{uuid.uuid4().hex[:10]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([args.topic])

    assignments = defaultdict(int)
    engagement_sum = defaultdict(float)
    engagement_count = defaultdict(int)
    processed = 0
    last_reported_users = 0
    last_report_time = time.time()

    print(f"Listening to Kafka topic '{args.topic}'...", flush=True)

    try:
        while processed < args.max_events:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}", flush=True)
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                print(f"Skipping invalid event: {exc}", flush=True)
                continue

            variant = event.get("variant")
            event_type = event.get("type")

            if variant not in ("control", "treatment"):
                continue

            if event_type == "assignment":
                assignments[variant] += 1
            elif event_type == "engagement":
                engagement_sum[variant] += float(event.get("value", 0.0))
                engagement_count[variant] += 1

            processed += 1
            total_users = sum(assignments.values())

            if (
                total_users >= args.report_every
                and total_users // args.report_every
                > last_reported_users // args.report_every
                and assignments["control"] > 0
                and assignments["treatment"] > 0
            ):
                result = srm_check(
                    dict(assignments),
                    {"control": 1.0, "treatment": 1.0},
                )

                means = {
                    v: (
                        engagement_sum[v] / engagement_count[v]
                        if engagement_count[v] else 0.0
                    )
                    for v in ("control", "treatment")
                }

                status = "ALERT: SRM detected" if result["alert"] else "OK"
                print(
                    f"events={processed} users={total_users} "
                    f"control={assignments['control']} "
                    f"treatment={assignments['treatment']} "
                    f"SRM_p={result['p_value']:.3g} "
                    f"status={status} "
                    f"mean_engagement_control={means['control']:.3f} "
                    f"mean_engagement_treatment={means['treatment']:.3f}",
                    flush=True,
                )

                last_reported_users = total_users
                last_report_time = time.time()

    except KeyboardInterrupt:
        print("\nStopping consumer...", flush=True)
    finally:
        consumer.close()

    print(
        f"Finished: {processed} events, "
        f"{assignments['control'] + assignments['treatment']} assignments.",
        flush=True,
    )


if __name__ == "__main__":
    main()
