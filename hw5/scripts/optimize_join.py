#!/usr/bin/env python3
"""
Проблема: JOIN большой таблицы заказов с маленьким справочником каталога.
Наивный вариант -> Spark делает sort-merge join с shuffle всех данных (дорого).
Оптимизация: BROADCAST JOIN (маленький справочник шлём на все исполнители без shuffle).

Также демонстрируем включение AQE (adaptive.enabled) — Spark сам предложит broadcast.

Использование:
    spark-submit optimize_join.py --data ../data
    # быстрая проверка:
    spark-submit optimize_join.py --data ../data_small --n 2
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark(broadcast_auto=True, aqe=False):
    spark = (SparkSession.builder
             .appName("retailflow-hw5-join")
             .config("spark.sql.shuffle.partitions", "200")
             .getOrCreate())
    spark.conf.set("spark.sql.adaptive.enabled", aqe)
    # здесь НЕ ставим autoBroadcastJoinThreshold — управляем хинтом явно для честности
    return spark


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

    # 1) НАИВНО: без broadcast, spark.sql.autoBroadcastJoinThreshold=0 -> sort-merge join
    spark1 = build_spark(broadcast_auto=False)
    spark1.conf.set("spark.sql.autoBroadcastJoinThreshold", "0")  # выключаем авто-broadcast
    o1 = spark1.read.parquet(f"{args.data}/orders")
    c1 = spark1.read.parquet(f"{args.data}/catalog")

    def naive_join():
        spark1.sparkContext.setJobGroup("naive-join", "sort-merge join")
        o1.join(c1, "category_id", "left").count()

    t_naive = timed(naive_join, args.n)

    # 2) BROADCAST: маленький справочник рассылаем хинтом
    spark2 = build_spark()
    o2 = spark2.read.parquet(f"{args.data}/orders")
    c2 = spark2.read.parquet(f"{args.data}/catalog")

    def bcast_join():
        spark2.sparkContext.setJobGroup("bcast-join", "broadcast join")
        o2.join(F.broadcast(c2), "category_id", "left").count()

    t_bcast = timed(bcast_join, args.n)

    # 3) AQE: просто включили adaptive — Spark сам коалесцирует/решает
    spark3 = build_spark(aqe=True)
    o3 = spark3.read.parquet(f"{args.data}/orders")
    c3 = spark3.read.parquet(f"{args.data}/catalog")

    def aqe_join():
        spark3.sparkContext.setJobGroup("aqe-join", "adaptive join")
        o3.join(c3, "category_id", "left").count()

    t_aqe = timed(aqe_join, args.n)

    print(f"\n=== JOIN (сек, min of {args.n}) data={args.data} ===")
    print(f"naive(sort-merge)\t{t_naive:.3f}")
    print(f"broadcast-hint    \t{t_bcast:.3f}")
    print(f"aqe-on            \t{t_aqe:.3f}")

    # сводку сохраняем в файл (чтобы не искать среди логов Spark)
    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "optimize_join_RESULTS.txt")
    with open(res_path, "w") as fh:
        fh.write("variant\ttime_s\n")
        fh.write(f"naive\t{t_naive:.3f}\n")
        fh.write(f"broadcast_hint\t{t_bcast:.3f}\n")
        fh.write(f"aqe_on\t{t_aqe:.3f}\n")
    print(f">>> сводка сохранена: cat {res_path}")


if __name__ == "__main__":
    main()
