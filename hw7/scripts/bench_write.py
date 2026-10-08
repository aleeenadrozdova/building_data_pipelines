#!/usr/bin/env python3
"""
Тест ПРОИЗВОДИТЕЛЬНОСТИ ЗАПИСИ: в Delta / Hudi / Iceberg при разных объёмах (10/20/50/100%).
Меряем время записи и размер на диске (и сколько «весит» таблица с метаданными).

ВАЖНО: у каждого формата свой SparkSession — Delta/Hudi/Iceberg на Spark 4 требуют РАЗНЫЕ
spark.sql.extensions и spark_catalog (смешивать их в одной сессии нельзя).

Использование:
    spark-submit --packages <3 пакета, см. RUN_ME> bench_write.py --src ../data/source.parquet --outdir ../data/tbl

Параметры:
    --src      паратет-датасет (gen_data.py)
    --outdir   куда писать таблицы (%/формат)
"""
import argparse
import os
import time

from pyspark.sql import SparkSession

FRACTIONS = [0.10, 0.20, 0.50, 1.00]

# конфиги, обязательные для каждого формата на Spark 4.x
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
         .appName(f"retailflow-hw7-write-{name}")
         .config("spark.sql.shuffle.partitions", "200"))
    for k, v in FORMAT_CONF[name].items():
        b = b.config(k, v)
    return b.getOrCreate()


def dir_size(path):
    total = 0
    for root, _d, files in os.walk(path):
        for f in files:
            total += os.path.getsize(os.path.join(root, f))
    return total / 1024 / 1024


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--format", choices=list(FORMAT_CONF) + ["all"], default="all",
                    help="формат для прогона: delta|hudi|iceberg (по умолчанию — все)")
    args = ap.parse_args()

    # при выборочном прогоне (--format iceberg на другой версии Spark) СВОДКУ ДОЗАПИСЫВАЕМ,
    # не затирая уже полученные строки delta/hudi
    partial = args.format != "all"
    formats = list(FORMAT_CONF) if not partial else [args.format]

    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "bench_write_RESULTS.txt")
    # пишем по мере прохождения формата — если один формат упадёт, остальные результаты
    # уже не потеряются
    with open(res_path, "a" if partial else "w") as fh:
        fh.write("" if partial else "frac_pct\tformat\trows\tsize_MB\ttime_s\n")

    for name in formats:  # своя сессия на формат
        try:
            spark = build_spark(name)
            all_data = spark.read.parquet(args.src).repartition(16)
            total = all_data.count()

            for frac in FRACTIONS:
                take = int(total * frac)
                chunk = all_data.limit(take)
                out = f"{args.outdir}/{name}"
                t0 = time.time()
                if name == "delta":
                    chunk.write.format("delta").mode("overwrite").save(out)
                elif name == "hudi":
                    chunk.write.format("hudi").mode("overwrite") \
                        .option("primaryKey", "id").option("preCombineField", "version") \
                        .option("hoodie.table.name", f"fact_{name}") \
                        .save(out)
                else:  # iceberg
                    chunk.write.format("iceberg").mode("overwrite") \
                        .save(f"{out}/fact")
                dt = time.time() - t0
                sz = dir_size(out)
                line = f"[write] frac={int(frac*100):3d}%  {name:8s} rows={take:>9,}  size={sz:9.2f}MB  time={dt:7.2f}s"
                print(line)
                with open(res_path, "a") as fh:
                    fh.write(f"{int(frac*100)}\t{name}\t{take}\t{sz:.2f}\t{dt:.2f}\n")

            spark.stop()
        except Exception as e:
            print(f"[write] {name}: ПРОПУЩЕН (ошибка): {type(e).__name__}: {str(e)[:300]}")
        print("-" * 60)

    print(f">>> сводка: cat {res_path}")


if __name__ == "__main__":
    main()
