"""Spark Structured Streaming job: Kafka -> per-variant windowed metrics + live SRM alerts."""
import argparse
import json
import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

sys.path.insert(0, os.path.dirname(__file__))
from stats import srm_check  # noqa: E402
from windows import WindowState  # noqa: E402

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("user_id", StringType()),
    StructField("variant", StringType()),
    StructField("type", StringType()),
    StructField("value", DoubleType()),
    StructField("pre_metric", DoubleType()),
    StructField("ts", StringType()),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="events-live")
    ap.add_argument("--metrics-out", default="data/metrics.jsonl")
    ap.add_argument("--checkpoint", default="/tmp/exp-checkpoint")
    ap.add_argument("--watermark", default="2 minutes")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("experiment-metrics")
             .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", args.bootstrap)
           .option("subscribe", args.topic)
           .option("startingOffsets", "earliest").load())

    events = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
              .withColumn("event_time", F.to_timestamp("ts")))

    # Duplicates from producer retries are prevented upstream (idempotent producer), because chaining
    # dropDuplicates before a windowed aggregation is not supported in update output mode.
    agg = (events.withWatermark("event_time", args.watermark).groupBy(F.window("event_time", "1 minute").alias("w"), "variant")
           .agg(F.count(F.when(F.col("type") == "assignment", 1)).alias("users"),
                F.sum(F.when(F.col("type") == "engagement", F.col("value")).otherwise(0.0))
                .alias("engagement_sum")))

    state = WindowState()
    flags = {"alerted": False}
    os.makedirs(os.path.dirname(args.metrics_out) or ".", exist_ok=True)

    def process_batch(df, batch_id):
        rows = [(r["w"]["start"], r["variant"], r["users"], r["engagement_sum"]) for r in df.collect()]
        state.apply(rows)
        totals = state.totals()
        if len(totals) < 2:
            return
        counts = {v: t["users"] for v, t in totals.items()}
        srm = srm_check(counts, {v: 1.0 for v in totals})        # expected 50/50 split
        with open(args.metrics_out, "a") as f:
            f.write(json.dumps({"batch": batch_id, "totals": totals, "srm": srm}) + "\n")
        if srm["alert"] and not flags["alerted"]:
            flags["alerted"] = True
            print(f"ALERT batch={batch_id}: sample ratio mismatch, n={srm['n']}, "
                  f"p={srm['p_value']:.2e}, counts={counts}", flush=True)
        else:
            print(f"batch={batch_id} n={srm['n']} srm_p={srm['p_value']}", flush=True)

    (agg.writeStream.outputMode("update").foreachBatch(process_batch)
     .option("checkpointLocation", args.checkpoint).start().awaitTermination())


if __name__ == "__main__":
    main()
