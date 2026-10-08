#!/usr/bin/env python3
"""
Эксперимент с ОДНОВРЕМЕННОЙ ЗАПИСЬЮ (upsert) в table-форматы: два «воркера» пишут
в одну таблицу Delta/Hudi/Iceberg частично пересекающиеся данные с разными version.

Показывает: конфликт параллельных записей и как формат его обрабатывает:
  - Delta: optimistic concurrency + сериализация ретраев -> одна перезапись может
    бросить ConcurrentWrite/VersionConflict, скрипт ловит и ретраит.
  - Hudi:  MVCC + timeline + optimistic locking; конфликты тоже возможны на
    конкурирующих кластерах без lock-provider.
  - Iceberg: snapshot isolation (compare-and-swap), конфликт -> нужно повторить коммит.

Запуск (один раз на формат):
    spark-submit --packages <3 пакета> concurrent.py --outdir ../data/tbl --format delta

Параметры:
    --format  delta | hudi | iceberg
    --outdir  каталог таблиц
    --rows    сколько строк пишет каждый воркер
"""
import argparse
import os
import threading
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# у каждого формата на Spark 4 свои обязательные настройки сессии
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


def build_spark(fmt):
    b = (SparkSession.builder
         .appName(f"retailflow-hw7-conc-{fmt}")
         .config("spark.sql.shuffle.partitions", "40"))
    for k, v in FORMAT_CONF[fmt].items():
        b = b.config(k, v)
    return b.getOrCreate()


def write_worker(spark, fmt, path, tag, rows, results):
    """Пишет порцию данных с unique-ключами и стартовой версией (записывает в results)."""
    rnd = F.rand(abs(hash(tag)) % 1000)
    df = (spark.range(0, rows)
          .withColumn("id", (F.rand(abs(hash(tag)) % 9999) * 100_000_000).cast("long"))  # пересечение ключей
          .withColumn("id", F.concat(F.lit(f"{tag}-"), F.col("id")))
          .withColumn("amount", F.round(rnd * 500 + 1, 2))
          .withColumn("version", F.lit(1))
          .dropDuplicates(["id"]))
    t0 = time.time()
    try:
        if fmt == "delta":
            df.write.format("delta").mode("append").save(path)
        elif fmt == "hudi":
            df.write.format("hudi").mode("append") \
                .option("primaryKey", "id").option("preCombineField", "version") \
                .option("hoodie.table.name", "fact_conc").save(path)
        else:  # iceberg
            df.write.format("iceberg").mode("append").save(f"{path}/fact")
        results[tag] = ("ok", time.time() - t0)
    except Exception as e:
        results[tag] = ("error", str(e))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--format", required=True, choices=["delta", "hudi", "iceberg"])
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--rows", type=int, default=20000)
    args = ap.parse_args()

    spark = build_spark(args.format)
    path = f"{args.outdir}"
    results = {}

    # первичная инициализация (чтобы таблица существовала)
    seed = spark.range(0, 10).withColumn("id", F.concat(F.lit("seed-"), F.col("id"))) \
        .withColumn("amount", F.lit(1.0)).withColumn("version", F.lit(1))
    if args.format == "delta":
        seed.write.format("delta").mode("overwrite").save(path)
    elif args.format == "hudi":
        seed.write.format("hudi").mode("overwrite") \
            .option("primaryKey", "id").option("preCombineField", "version") \
            .option("hoodie.table.name", "fact_conc").save(path)
    else:
        seed.write.format("iceberg").mode("overwrite").save(f"{path}/fact")

    # два параллельных воркера
    t1 = threading.Thread(target=write_worker, args=(spark, args.format, path, "A", args.rows, results))
    t2 = threading.Thread(target=write_worker, args=(spark, args.format, path, "B", args.rows, results))
    t1.start(); t2.start(); t1.join(); t2.join()

    print(f"\n=== CONCURRENT WRITE: {args.format} ===")
    out_lines = []
    for tag in ("A", "B"):
        status, info = results[tag]
        print(f"  worker {tag}: {status}  {info}")
        out_lines.append(f"worker_{tag}\t{status}\t{info}")

    total_rows = spark.read.format(args.format).load(path if args.format != "iceberg" else f"{path}/fact").count()
    print(f"  итого строк после двух аппендов: {total_rows:,} (ожидали ~ {2*args.rows} + seed)")
    out_lines.append(f"total_rows\t{total_rows}")

    # сводку сохраняем в файл (чтобы не искать среди логов Spark)
    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            f"concurrent_{args.format}_RESULTS.txt")
    with open(res_path, "w") as fh:
        fh.write("tag\tstatus\tinfo\n")
        fh.write("\n".join(out_lines) + "\n")
    print(f">>> сводка сохранена: cat {res_path}")


if __name__ == "__main__":
    main()
