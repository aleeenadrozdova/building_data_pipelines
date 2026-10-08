#!/usr/bin/env python3
"""
Генератор событий продаж для hw6 (сравнение DataFrame vs RDD).
Пишет Parquet: события (user_id, session_id, category_id, amount, qty, ts)
и отдельный датасет с вложенным полем items (array<struct>) для кейсов nested-типов.

Запуск:
    spark-submit gen_data.py --rows 5000000 --out ../data
    # быстрая проверка:
    spark-submit gen_data.py --rows 200000 --out ../data_small
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark():
    return (SparkSession.builder
            .appName("retailflow-hw6-gen")
            .config("spark.driver.memory", "6g")
            .getOrCreate())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    spark = build_spark()
    rnd = F.rand(123)

    base = spark.range(0, args.rows)
    events = (base
              .withColumn("event_id", F.concat(F.lit("e-"), F.col("id")))
              .withColumn("user_id", (rnd * 200_000).cast("int"))
              .withColumn("session_id", (rnd * 1_000_000).cast("long"))
              .withColumn("category_id", (rnd * 50).cast("int"))
              .withColumn("amount", F.round(rnd * 5000 + 10, 2))
              .withColumn("qty", (rnd * 5 + 1).cast("int"))
              .withColumn("ts", F.lit("2026-09-01 00:00:00") +
                          F.expr("cast(rand() * 8640000000000 as long) * interval 1 microsecond"))
              .drop("id"))
    events.write.mode("overwrite").partitionBy("category_id").parquet(f"{args.out}/events")

    # вложенный датасет: items = array<struct> (до 3 строк) + размер в отдельной колонке
    n_items = (F.col("qty") % 3) + 1
    nested = (events.filter(F.col("amount") > 0)
              .withColumn("n", n_items)
              .withColumn("items",
                          F.array([F.struct(
                              F.lit(i).alias("line"),
                              F.round(F.col("amount") / (F.col("qty") + 0.0), 2).alias("unit_price"),
                              F.col("qty").alias("qty"),
                          ) for i in range(1, 4)])))
    nested = nested.withColumn("items", F.slice(F.col("items"), 1, F.col("n")))
    nested.drop("n").write.mode("overwrite").parquet(f"{args.out}/events_nested")
    print(f"[gen] events + events_nested -> {args.out}")


if __name__ == "__main__":
    main()
