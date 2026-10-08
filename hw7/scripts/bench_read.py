#!/usr/bin/env python3
"""
Тест ПРОИЗВОДИТЕЛЬНОСТИ ЧТЕНИЯ: Delta / Hudi / Iceberg.
Операции: full scan, фильтр по дате (partitioned read), агрегация.

ВАЖНО: у каждого формата свой SparkSession — см. bench_write.py (у форматов разные
spark.sql.extensions / spark_catalog).

Использование:
    spark-submit --packages <3 пакета> bench_read.py --outdir ../data/tbl --n 3
Параметры:
    --outdir  каталог с таблицами (delta/, hudi/, iceberg/fact)
    --n       повторов (берём минимум)
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FORMAT_CONF = {
    "delta": {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    },
    # Hudi: на Spark 4.2 с hudi-spark4.0-bundle extension+catalog ломают даже чтение plain
    # parquet (HoodieSpark33DataSourceV2ToV1Fallback из чужой ветки Spark). Для path-based
    # операций format("hudi") сессионные настройки не нужны — поэтому пустой конфиг.
    "hudi": {},
    "iceberg": {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.spark_catalog": "org.apache.iceberg.spark.SparkSessionCatalog",
        "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.local.type": "hadoop",
        "spark.sql.catalog.local.warehouse": "/tmp/iceberg_wh",
    },
}


def build_spark(name):
    b = (SparkSession.builder
         .appName(f"retailflow-hw7-read-{name}")
         .config("spark.sql.shuffle.partitions", "200"))
    for k, v in FORMAT_CONF[name].items():
        b = b.config(k, v)
    return b.getOrCreate()


def timed(fn, n):
    best = float("inf")
    for _ in range(n):
        t0 = time.time()
        fn()
        best = min(best, time.time() - t0)
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--n", type=int, default=3)
    ap.add_argument("--format", choices=["delta", "hudi", "iceberg", "all"], default="all",
                    help="формат для прогона: delta|hudi|iceberg (по умолчанию — все)")
    args = ap.parse_args()

    readers = {
        "delta":   f"{args.outdir}/delta",
        "hudi":    f"{args.outdir}/hudi",
        "iceberg": f"{args.outdir}/iceberg/fact",
    }
    # при выборочном прогоне (--format iceberg на другой версии Spark) СВОДКУ ДОЗАПИСЫВАЕМ,
    # не затирая уже полученные строки delta/hudi
    partial = args.format != "all"
    if partial:
        readers = {args.format: readers[args.format]}

    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "bench_read_RESULTS.txt")
    print("format\tfull_scan\tfilter_by_dt\taggregate")
    with open(res_path, "a" if partial else "w") as fh:
        fh.write("" if partial else "format\tfull_scan_s\tfilter_by_dt_s\taggregate_s\n")

    for name, path in readers.items():  # своя сессия на формат
        try:
            spark = build_spark(name)
            df = spark.read.format(name).load(path)
            t_scan = timed(lambda: df.count(), args.n)
            t_filter = timed(lambda: (df.filter(F.col("dt") == "2026-09-10")
                                       .select("id", "amount").count()), args.n)
            t_agg = timed(lambda: (df.groupBy("category_id")
                                    .agg(F.sum("amount").alias("a")).count()), args.n)
            line = f"{name}\t{t_scan:.3f}\t{t_filter:.3f}\t{t_agg:.3f}"
            print(line)
            with open(res_path, "a") as fh:
                fh.write(line + "\n")
            spark.stop()
        except Exception as e:
            print(f"{name}\tПРОПУЩЕН: {type(e).__name__}: {str(e)[:200]}")

    print(f">>> сводка: cat {res_path}")


if __name__ == "__main__":
    main()
