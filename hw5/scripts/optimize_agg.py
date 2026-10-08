#!/usr/bin/env python3
"""
Проблема: ПЕРЕКОС КЛЮЧЕЙ (skew) — несколько «горячих» продавцов сосредотачивают почти
все продажи, поэтому groupBy(seller_id) «упирается» в одну-две партиции (data skew).

Оптимизация: SALTING — разбиваем горячий ключ суффиксом N даёт распараллеливание:
    (seller_id, salt) -> агрегация -> убираем salt -> финальная агрегация.
Также мерим с включённым AQE (skew join на join-ах, коалесценция партиций).

Использование:
    spark-submit optimize_agg.py --data ../data
    spark-submit optimize_agg.py --data ../data_small --n 2
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(aqe=False):
    s = (SparkSession.builder
         .appName("retailflow-hw5-agg")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())
    s.conf.set("spark.sql.adaptive.enabled", aqe)
    return s


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

    orders_src = f"{args.data}/orders"

    # 1) НАИВНАЯ агрегация по seller_id (перекос -> долго)
    spark1 = build_spark()
    o1 = spark1.read.parquet(orders_src)

    def naive_agg():
        spark1.sparkContext.setJobGroup("naive-agg", "groupBy seller")
        o1.groupBy("seller_id").agg(F.sum("amount").alias("rev")).count()

    t_naive = timed(naive_agg, args.n)

    # 2) SALTING: добавляем суффикс 0..9 к seller_id при агрегации, потом схлопываем
    spark2 = build_spark()
    o2 = spark2.read.parquet(orders_src)
    N_SALT = 10

    def salted_agg():
        spark2.sparkContext.setJobGroup("salted-agg", "salt + groupBy")
        salted = o2.withColumn("sk", F.concat_ws("_", F.col("seller_id"),
                                                 F.floor(F.rand() * N_SALT)))
        part = (salted.groupBy("sk")
                      .agg(F.sum("amount").alias("rev")))
        final = (part.withColumn("seller_id", F.split(F.col("sk"), "_")[0])
                    .groupBy("seller_id").agg(F.sum("rev").alias("rev")))
        final.count()

    t_salted = timed(salted_agg, args.n)

    # 3) AQE включён (coalesce мелких партиций + auto skew) — может помочь, может нет,
    #    это интересная инженерная оценка
    spark3 = build_spark(aqe=True)
    o3 = spark3.read.parquet(orders_src)

    def aqe_agg():
        spark3.sparkContext.setJobGroup("aqe-agg", "aqe on")
        o3.groupBy("seller_id").agg(F.sum("amount").alias("rev")).count()

    t_aqe = timed(aqe_agg, args.n)

    print(f"\n=== AGG (сек, min of {args.n}) data={args.data} ===")
    print(f"naive(groupBy)  \t{t_naive:.3f}")
    print(f"salted          \t{t_salted:.3f}")
    print(f"aqe-on          \t{t_aqe:.3f}")

    # сводку сохраняем в файл (чтобы не искать среди логов Spark)
    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "optimize_agg_RESULTS.txt")
    with open(res_path, "w") as fh:
        fh.write("variant\ttime_s\n")
        fh.write(f"naive\t{t_naive:.3f}\n")
        fh.write(f"salted\t{t_salted:.3f}\n")
        fh.write(f"aqe_on\t{t_aqe:.3f}\n")
    print(f">>> сводка сохранена: cat {res_path}")


if __name__ == "__main__":
    main()
