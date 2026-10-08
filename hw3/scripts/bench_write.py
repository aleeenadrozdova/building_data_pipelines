#!/usr/bin/env python3
"""
Бенчмарк ЗАПИСИ: конвертирует исходный датасет (Parquet "source") в 4 формата
(Parquet, Avro, CSV, JSON) и замеряет время записи + размер на диске.

Использование:
    spark-submit bench_write.py --src data/source --outdir data/out
    # быстрый предварительный прогон (меньше данных):
    spark-submit bench_write.py --src data/source_small --outdir data/out_small --limit 1000000

Параметры:
    --src       каталог Parquet, сгенерённый gen_data.py
    --outdir    каталог для результатов по форматам
    --limit     ограничить число строк (для предварительного прогона; 0 = всё)
"""
import argparse
import os
import time

from pyspark.sql import SparkSession


def build_spark():
    return (
        SparkSession.builder
        .appName("retailflow-bench-write")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "200")
        # Avro-модуль идёт в комплекте со Spark 3.x
        .getOrCreate()
    )


def dir_size(path: str) -> float:
    """Размер каталога в МБ (рекурсивно)."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / 1024 / 1024


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    spark = build_spark()
    df = spark.read.format("parquet").load(args.src)
    if args.limit:
        df = df.limit(args.limit)

    # одинаковое число партиций для честного сравнения записи.
    # НЕ кэшируем весь датасет (10 ГБ не влезут в память) — читаем с диска на каждый формат.
    df = df.coalesce(8)

    formats = {
        "parquet": {"fmt": "parquet", "opts": {"compression": "snappy"}},
        "avro":    {"fmt": "avro",    "opts": {}},
        "csv":     {"fmt": "csv",     "opts": {"header": "true", "sep": "\t"}},
        "json":    {"fmt": "json",    "opts": {}},
    }

    results = []
    for name, spec in formats.items():
        out = os.path.join(args.outdir, name)
        t0 = time.time()
        # ПЛОСКАЯ запись (без partitionBy) — просто и устойчиво; для сравнения форматов
        # партиционирование не критично, а avro+partitionBy в Spark 4.x капризен.
        df.write.mode("overwrite").format(spec["fmt"]).options(**spec["opts"]).save(out)
        dt = time.time() - t0
        sz = dir_size(out)
        results.append((name, round(sz, 2), round(dt, 2)))
        print(f"[write] {name:8s} size={sz:9.2f} MB  time={dt:7.2f} s")

    print("\n=== СВОДКА ЗАПИСЬ (формат; размер_МБ; время_сек) ===")
    for name, sz, dt in results:
        print(f"{name}\t{sz}\t{dt}")

    # сводку дублируем в файл, чтобы не искать её среди логов Spark
    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "bench_write_RESULTS.txt")
    with open(res_path, "w") as fh:
        fh.write("format\tsize_MB\ttime_s\n")
        for name, sz, dt in results:
            fh.write(f"{name}\t{sz}\t{dt}\n")
    print(f">>> сводка сохранена: cat {res_path}")


if __name__ == "__main__":
    main()
