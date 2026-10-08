#!/usr/bin/env python3
"""
Генератор датасета с РЕАЛИСТИЧНЫМИ проблемами Spark-обработки:
  1. ПЕРЕКОС КЛЮЧЕЙ (skew) — несколько «горячих» продавцов дают огромную долю заказов.
  2. Много маленьких файлов / избыточное число партиций (мелкие задачи).
  3. Таблица каталога небольшая (для кейса broadcast join).

Выход:
  --out/orders    большой датасет заказов (Parquet, партиционирован по category)
  --out/catalog   маленькая таблица каталога (Parquet)

Использование:
    spark-submit gen_data.py --rows 20000000 --out ../data
    # быстрая проверка:
    spark-submit gen_data.py --rows 300000 --out ../data_small
"""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark():
    return (SparkSession.builder
            .appName("retailflow-hw5-gen")
            .config("spark.driver.memory", "6g")
            .getOrCreate())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    spark = build_spark()
    rnd = F.rand(args.seed)

    # --- CATALOG (маленькая) ---
    catalog = (spark.range(0, 100)
               .select(F.col("id").cast("int").alias("category_id"),
                       F.concat(F.lit("cat-"), F.col("id")).alias("category_name"),
                       F.round(F.rand() * 100, 1).alias("margin_pct")))
    catalog.write.mode("overwrite").parquet(f"{args.out}/catalog")

    # --- ORDERS (большая) с перекосом по seller_id ---
    # 3 «горячих» продавца получают непропорционально много заказов (~65%).
    # ВАЖНО: продавец считается по ОДНОМУ значению u (несколько вызовов rand() в одном
    # when дали бы независимые значения и «съели» перекос).
    n_sellers = 1000
    base = spark.range(0, args.rows).withColumn("u", F.rand(args.seed))
    skew = (F.when(F.col("u") < 0.30, F.lit(1))     # продавец #1 — 30%
             .when(F.col("u") < 0.50, F.lit(2))     # #2 — 20%
             .when(F.col("u") < 0.65, F.lit(3))     # #3 — 15%
             .otherwise((F.col("u") * n_sellers).cast("int")))  # остальные равномерно
    orders = (base
              .withColumn("order_id", F.concat(F.lit("ord-"), F.col("id")))
              .withColumn("user_id", (rnd * 2_000_000).cast("int"))
              .withColumn("seller_id", skew)
              .withColumn("category_id", (rnd * 100).cast("int"))
              .withColumn("amount", (rnd * 5000 + 100).cast("decimal(12,2)"))
              .withColumn("qty", (rnd * 5 + 1).cast("int"))
              .withColumn("ts", F.lit("2026-09-01 00:00:00") +
                          F.expr(f"cast(rand({args.seed}) * 8640000000000 as long) * interval 1 microsecond"))
              .drop("u", "id"))

    t0 = time.time()
    # пишем БЕЗ переразбиения — как есть, это даст разные (часто мелкие) файлы
    orders.write.mode("overwrite").partitionBy("category_id").format("parquet").save(f"{args.out}/orders")
    print(f"[gen] orders={args.rows} -> {args.out}/orders in {time.time()-t0:.1f}s")
    print(f"[gen] catalog -> {args.out}/catalog")

    # справка по перекосу (для отчёта)
    skew_check = (orders.groupBy("seller_id").count().withColumnRenamed("count", "cnt")
                  .orderBy(F.desc("cnt")).limit(5)).collect()
    print("[gen] top-5 seller by orders:", [(r["seller_id"], r["cnt"]) for r in skew_check])
    total = args.rows
    print("[gen] доля топ-3 продавца от общего числа заказов:",
          round(sum(r["cnt"] for r in skew_check) / total * 100, 1), "%")


if __name__ == "__main__":
    main()
