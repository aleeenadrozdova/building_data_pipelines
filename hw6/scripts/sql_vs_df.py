#!/usr/bin/env python3
"""
(*) hw6: 2 кейса, где SQL-запрос выполняется БЫСТРЕЕ эквивалентного DataFrame API.
Сравниваем планы через explain() и объясняем причину разницы.

  S1. Одно GROUP BY с несколькими агрегатами в SQL  (один HashAggregate / один проход)
      против «наивного» подхода на DF — несколько отдельных groupBy().agg(), соединённых
      join'ом (лишние Exchange = shuffle).
  S2. Несколько оконных функций в одном SELECT  (один Window-узел, один проход)
      против отдельного .withColumn(... .over()) для каждой из них на DF
      (несколько Window-узлов).

Запуск:
    spark-submit sql_vs_df.py --data ../data
    spark-submit sql_vs_df.py --data ../data_small --n 2
"""
import argparse
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_spark():
    return (SparkSession.builder
            .appName("retailflow-hw6-sql")
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
    ap.add_argument("--data", required=True)
    ap.add_argument("--n", type=int, default=3)
    args = ap.parse_args()

    spark = build_spark()
    src = f"{args.data}/events"
    spark.read.parquet(src).createOrReplaceTempView("ev")

    # ---------- S1 ----------
    sql1 = """
        SELECT user_id,
               SUM(amount) AS s, AVG(amount) AS a,
               MIN(amount) AS mi, MAX(amount) AS ma, COUNT(qty) AS c
        FROM ev GROUP BY user_id
    """

    def run_sql1():
        spark.sql(sql1).count()

    t_sql1 = timed(run_sql1, args.n)

    base = spark.table("ev")

    def run_df1():
        # «наивный» DF: два отдельных groupBy + join -> 2 shuffle
        agg1 = base.groupBy("user_id").agg(F.sum("amount").alias("s"), F.avg("amount").alias("a"))
        agg2 = base.groupBy("user_id").agg(F.min("amount").alias("mi"), F.max("amount").alias("ma"),
                                           F.count("qty").alias("c"))
        agg1.join(agg2, "user_id", "inner").count()

    t_df1 = timed(run_df1, args.n)

    print("[S1] SQL single GROUP BY vs DF multiple groupBy+join")
    print(f"  sql={t_sql1:.3f}s  df-multi={t_df1:.3f}s")
    print("  --- SQL plan (майн: один HashAggregate, один shuffle) ---")
    spark.sql(sql1).explain("extended")

    # ---------- S2 ----------
    sql2 = """
        SELECT user_id, amount,
               ROW_NUMBER()   OVER (PARTITION BY user_id ORDER BY amount DESC) AS rn,
               SUM(amount)    OVER (PARTITION BY user_id)                     AS total,
               AVG(amount)    OVER (PARTITION BY user_id)                     AS avg_amt
        FROM ev
    """

    def run_sql2():
        spark.sql(sql2).count()

    t_sql2 = timed(run_sql2, args.n)

    w = Window.partitionBy("user_id")

    def run_df2():
        # DF: три отдельных окна -> 3 Window-узла / 3 пересылки
        (base.withColumn("rn", F.row_number().over(w.orderBy(F.desc("amount"))))
             .withColumn("total", F.sum("amount").over(w))
             .withColumn("avg_amt", F.avg("amount").over(w))
             .count())

    t_df2 = timed(run_df2, args.n)

    print("\n[S2] SQL multiple windows in one SELECT vs DF separate .over()")
    print(f"  sql={t_sql2:.3f}s  df-multi-over={t_df2:.3f}s")
    print("  --- SQL plan (один Window-узел) ---")
    spark.sql(sql2).explain("extended")

    print("\n=== SQL vs DF (сек, min of N) ===")
    print(f"S1 sql={t_sql1:.3f}  df={t_df1:.3f}")
    print(f"S2 sql={t_sql2:.3f}  df={t_df2:.3f}")

    # сводку сохраняем в файл (чтобы не искать среди логов Spark)
    res_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "sql_vs_df_RESULTS.txt")
    with open(res_path, "w") as fh:
        fh.write("case\tsql_s\tdf_s\n")
        fh.write(f"S1\t{t_sql1:.3f}\t{t_df1:.3f}\n")
        fh.write(f"S2\t{t_sql2:.3f}\t{t_df2:.3f}\n")
    print(f">>> сводка сохранена: cat {res_path}")


if __name__ == "__main__":
    main()
