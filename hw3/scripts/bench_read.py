#!/usr/bin/env python3
"""
Бенчмарк ЧТЕНИЯ: для каждого формата (из bench_write) измеряем 3 операции:
  1. full scan  — прочитать все строки, посчитать count
  2. выборка    — фильтр по event_type='purchase' + отбор колонок
  3. агрегация  — сумма выручки (price*quantity) по категориям

Использование:
    spark-submit bench_read.py --indir data/out --n 5
    # n — сколько повторов, берём минимум/медиану

Параметры:
    --indir  каталог с подпапками parquet/avro/csv/json (результат bench_write)
    --n      число повторов каждого замера
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark():
    return (
        SparkSession.builder
        .appName("retailflow-bench-read")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")   # AQE
        .getOrCreate()
    )


def timed(fn, n):
    """Запустить fn n раз, вернуть минимум времени (сек)."""
    best = float("inf")
    for _ in range(n):
        t0 = time.time()
        fn()
        best = min(best, time.time() - t0)
    return best


def load(spark, fmt, path):
    if fmt == "csv":
        return spark.read.format("csv").option("header", "true").option("sep", "\t").load(path)
    return spark.read.format(fmt).load(path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--indir", required=True)
    ap.add_argument("--n", type=int, default=5)
    args = ap.parse_args()

    spark = build_spark()

    formats = ["parquet", "avro", "csv", "json"]
    header = "format\tfull_scan\tselect\taggregate"
    print(header)
    rows = []

    for fmt in formats:
        path = f"{args.indir}/{fmt}"
        # если формат ещё не записан (например, прогон упал на середине) — пропускаем
        if not os.path.isdir(path) or not os.listdir(path):
            print(f"{fmt}\tSKIP (нет данных в {path})")
            rows.append(f"{fmt}\tSKIP")
            continue
        df = load(spark, fmt, path)

        # типовая нормализация для csv/json (все строки) — приведём типы как в исходнике
        if fmt in ("csv", "json"):
            df = (
                df.withColumn("price", F.col("price").cast("decimal(12,2)"))
                  .withColumn("quantity", F.col("quantity").cast("int"))
                  .withColumn("category_id", F.col("category_id").cast("int"))
            )

        t_scan = timed(lambda: df.count(), args.n)

        filtered = df.filter(F.col("event_type") == "purchase")
        t_select = timed(lambda: filtered.select("event_id", "user_id", "price", "ts").count(), args.n)

        agg = (df.groupBy("category_id")
                 .agg(F.coalesce(F.sum(F.col("price") * F.col("quantity")), F.lit(0)).alias("revenue")))
        t_agg = timed(lambda: agg.count(), args.n)

        line = f"{fmt}\t{t_scan:.3f}\t{t_select:.3f}\t{t_agg:.3f}"
        print(line)
        rows.append(line)

    print("\nВремя — в секундах; используется минимум из N повторов.")

    # сводку дублируем в файл, чтобы не искать её среди логов Spark
    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "bench_read_RESULTS.txt")
    with open(res_path, "w") as fh:
        fh.write(header + "\n")
        fh.write("\n".join(rows) + "\n")
    print(f">>> сводка сохранена: cat {res_path}")


if __name__ == "__main__":
    main()
