#!/usr/bin/env python3
"""
Генератор фактовых записей продаж (для hw7: запись в Delta/Hudi/Iceberg).
Каждая запись уникальна по `id` и имеет время + версию — удобно для тестов
одновременной записи (upserts) и сравнения производительности.

Запуск:
    spark-submit gen_data.py --rows 3000000 --out ../data/source.parquet
    # быстрая проверка:
    spark-submit gen_data.py --rows 200000 --out ../data/source_small.parquet
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark():
    return (SparkSession.builder
            .appName("retailflow-hw7-gen")
            .config("spark.driver.memory", "6g")
            .getOrCreate())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    spark = build_spark()
    rnd = F.rand(2026)
    df = (spark.range(0, args.rows)
          .withColumn("id", F.concat(F.lit("f-"), F.col("id")))     # уникальный ключ факта
          .withColumn("dt", F.to_date(F.lit("2026-09-01") + F.expr("cast(rand()*29 as int) * interval 1 day")))
          .withColumn("category_id", (rnd * 100).cast("int"))
          .withColumn("seller_id", (rnd * 1000).cast("int"))
          .withColumn("amount", F.round(rnd * 5000 + 10, 2))
          .withColumn("qty", (rnd * 5 + 1).cast("int"))
          .withColumn("version", F.lit(1)))
    df.write.mode("overwrite").parquet(args.out)
    print(f"[gen] rows={args.rows} -> {args.out}")


if __name__ == "__main__":
    main()
