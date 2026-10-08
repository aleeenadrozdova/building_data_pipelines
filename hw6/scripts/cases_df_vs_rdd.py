#!/usr/bin/env python3
"""
3 кейса, где DataFrame ГАРАНТИРОВАННО быстрее RDD, + какая оптимизация Catalyst/Tungsten
даёт выигрыш в каждом. Датасет e-commerce.

Кейсы:
  C1. Множественные агрегации — df.groupBy().agg(sum,avg,min,max,count) за ОДИН проход
      vs RDD, где то же самое приходится делать сравнимой логикой на уровне значений
      (Tungsten whole-stage codegen + векторные агрегации без сериализации).
  C2. Оконные функции — топ-N в группе (window row_number) vs RDD: сортировка + groupBy + take
      (Catalyst: одно окно без ручной сортировки/повторных операций, codegen).
  C3. Вложенные типы (array<struct>) — df explode/agg без сериализации
      vs RDD, где приходится вручную парсить вложенные структуры (Tungsten работает
      с нативными типами без pickle).

Запуск:
    spark-submit cases_df_vs_rdd.py --data ../data
    spark-submit cases_df_vs_rdd.py --data ../data_small --n 2
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark():
    return (SparkSession.builder
            .appName("retailflow-hw6-cases")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.adaptive.enabled", "true")
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
    ap.add_argument("--data", required=True)
    ap.add_argument("--n", type=int, default=3)
    args = ap.parse_args()

    spark = build_spark()
    src = f"{args.data}/events"
    nested_src = f"{args.data}/events_nested"

    # ============ C1: множественные агрегации ============
    df1 = spark.read.parquet(src)

    def df_agg():
        df1.groupBy("user_id").agg(
            F.sum("amount").alias("s"), F.avg("amount").alias("a"),
            F.min("amount").alias("mi"), F.max("amount").alias("ma"),
            F.count("qty").alias("c")).count()

    def rdd_agg():
        # на RDD агрегацию придётся реализовать встроенной логикой (seq/comb) — медленнее
        (df1.rdd.map(lambda r: (r.user_id, (r.amount, r.amount, r.amount, 1)))
                .reduceByKey(lambda x, y: (x[0]+y[0], min(x[1], y[1]), max(x[2], y[2]), x[3]+y[3]))
                .flatMap(lambda kv: [(kv[0], kv[1][0], kv[1][1], kv[1][2], kv[1][3])])
                .count())

    t_df1 = timed(df_agg, args.n)
    t_rdd1 = timed(rdd_agg, args.n)

    # ============ C2: оконная функция (топ-N в группе) ============
    df2 = spark.read.parquet(src)
    w = Window.partitionBy("user_id").orderBy(F.desc("amount"))

    def df_window():
        df2.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") <= 3).count()

    def rdd_window():
        # RDD: глобальная сортировка + groupBy + take(3) по каждой группе
        df2.select("user_id", "amount") \
           .rdd.sortBy(lambda r: (r.user_id, -r.amount)) \
           .map(lambda r: (r.user_id, r.amount)) \
           .groupByKey() \
           .flatMap(lambda kv: [(kv[0], x) for x in list(kv[1])[:3]]) \
           .count()

    t_df2 = timed(df_window, args.n)
    t_rdd2 = timed(rdd_window, args.n)

    # ============ C3: вложенные типы ============
    df3 = spark.read.parquet(nested_src)
    # мини-кэш, чтобы оба варианта читали то же
    df3.cache()
    df3.count()

    def df_nested():
        (df3.withColumn("item", F.explode(F.col("items")))
            .groupBy("user_id")
            .agg(F.sum(F.col("item.unit_price") * F.col("item.qty")).alias("total"))
            .count())

    def rdd_nested():
        # парсим вложенный массив вручную из сырого значения (pickle+итерация по dict)
        (df3.select("user_id", "items").rdd
            .map(lambda r: (r.user_id, sum(it["unit_price"] * it["qty"] for it in r.items)))
            .reduceByKey(lambda a, b: a + b)
            .count())

    t_df3 = timed(df_nested, args.n)
    t_rdd3 = timed(rdd_nested, args.n)

    print(f"\n=== DF vs RDD (сек, min of {args.n}) data={args.data} ===")
    print(f"C1 много-агрегаций  \tdf={t_df1:.3f}  rdd={t_rdd1:.3f}")
    print(f"C2 оконная (топ-N)  \tdf={t_df2:.3f}  rdd={t_rdd2:.3f}")
    print(f"C3 вложенные типы   \tdf={t_df3:.3f}  rdd={t_rdd3:.3f}")

    # сводка в файл (чтобы не искать среди логов Spark)
    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "cases_df_vs_rdd_RESULTS.txt")
    with open(res_path, "w") as fh:
        fh.write("case\tdf_s\trdd_s\n")
        fh.write(f"C1\t{t_df1:.3f}\t{t_rdd1:.3f}\n")
        fh.write(f"C2\t{t_df2:.3f}\t{t_rdd2:.3f}\n")
        fh.write(f"C3\t{t_df3:.3f}\t{t_rdd3:.3f}\n")
    print(f">>> сводка сохранена: cat {res_path}")


if __name__ == "__main__":
    main()
