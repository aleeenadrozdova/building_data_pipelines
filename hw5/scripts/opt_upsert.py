#!/usr/bin/env python3
"""
(* hw5) ПРОБЛЕМА, НЕ ОПИСАННАЯ В КЛАССИЧЕСКОМ СПИСКЕ ОПТИМИЗАЦИЙ:
**Инкрементальные обновления витрины без табличных форматов (merge-on-read).**

Ситуация (реалистичная для RetailFlow): ежедневно меняется лишь небольшой процент строк
витрины (например, цена/остаток у 1–5% товаров), а чтобы «обновить» данные, пайплайн
переписывает всю витрину целиком (full rewrite). Это дорого по записи и I/O.

Решение — ДВУХСЛОЙНЫЙ merge-on-read:
  - базовый слой: стабильная партиция, пишется редко;
  - слой изменений (updates): пишем ТОЛЬКО изменённые строки (небольшая запись);
  - при чтении: base JOIN updates + coalesce(price) — дешёвый маленький join;
  - периодически делаем compaction (переписываем объединённое в новый base).

Меряем: время полного переписывания vs (время записи слоя изменений + время чтения
с подмешиванием) при долях обновления 1%, 5%, 20%.

Запуск:
    spark-submit --driver-memory 8g --executor-memory 8g \
      opt_upsert.py --rows 5000000 --out ../data/mor --n 2
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark():
    return (SparkSession.builder
            .appName("retailflow-hw5-upsert")
            .config("spark.sql.shuffle.partitions", "200")
            .getOrCreate())


def timed(fn, n):
    best = float("inf")
    for _ in range(n):
        t0 = time.time()
        fn()
        best = min(best, time.time() - t0)
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=5_000_000)
    ap.add_argument("--out", required=True)
    ap.add_argument("--n", type=int, default=2)
    args = ap.parse_args()

    spark = build_spark()
    out = args.out
    rnd = F.rand(7)

    # ---- базовый слой витрины (пишем один раз) ----
    base = (spark.range(0, args.rows)
            .withColumn("id", F.concat(F.lit("f-"), F.col("id")))
            .withColumn("category_id", (rnd * 100).cast("int"))
            .withColumn("price", F.round(rnd * 5000 + 100, 2))
            .withColumn("dt", F.lit("2026-09-01")))
    base.write.mode("overwrite").parquet(f"{out}/base")
    base_rows = args.rows

    print("frac\tfull_rewrite_s\tupdates_write_s\tmerge_read_s")
    results = []
    for frac in (0.01, 0.05, 0.20):
        n_upd = int(base_rows * frac)
        # слой изменений: подмножество строк с новыми ценами
        updates = (base.sample(withReplacement=False, fraction=frac, seed=11)
                   .select("id", "category_id",
                           F.round(F.col("price") * 1.1, 2).alias("new_price"),
                           F.lit("2026-09-02").alias("eff_dt")))
        # для join берём только ключ и новое значение (иначе дубль category_id)
        upd_join = updates.select("id", "new_price")

        # ---- 1) ПОЛНОЕ переписывание витрины (как делают обычно) ----
        def full_rewrite():
            b = spark.read.parquet(f"{out}/base")
            (b.alias("b")
             .join(upd_join.alias("u"), "id", "left")
             .withColumn("price", F.coalesce(F.col("u.new_price"), F.col("b.price")))
             .write.mode("overwrite")
             .partitionBy("dt")  # после обновления приходится перепартиционировать заново
             .parquet(f"{out}/full_{int(frac*100)}"))

        t_full = timed(full_rewrite, args.n)

        # ---- 2) MERGE-ON-READ: пишем ТОЛЬКО изменения ----
        def write_updates():
            updates.write.mode("overwrite").parquet(f"{out}/upd_{int(frac*100)}")

        t_upd = timed(write_updates, args.n)

        # чтение: base LEFT JOIN updates + coalesce (дешёвый маленький join)
        def merge_read():
            b = spark.read.parquet(f"{out}/base")
            u = spark.read.parquet(f"{out}/upd_{int(frac*100)}").select("id", "new_price")
            (b.alias("b").join(u.alias("u"), "id", "left")
             .withColumn("price", F.coalesce(F.col("u.new_price"), F.col("b.price")))
             .agg(F.count("*"), F.avg("price")).collect())

        t_read = timed(merge_read, args.n)

        line = f"{frac*100:4.0f}%\t{t_full:.3f}\t{t_upd:.3f}\t{t_read:.3f}"
        print(line)
        results.append(line)

    # компактция (переписываем объединённое в новый базовый слой) — разово, отдельно
    # не обязательна для бенча; упоминается как обвязка слоя изменений в отчёте.

    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "opt_upsert_RESULTS.txt")
    with open(res_path, "w") as fh:
        fh.write("frac\tfull_rewrite_s\tupdates_write_s\tmerge_read_s\n")
        fh.write("\n".join(results) + "\n")
    print(f">>> сводка сохранена: cat {res_path}")

if __name__ == "__main__":
    main()
