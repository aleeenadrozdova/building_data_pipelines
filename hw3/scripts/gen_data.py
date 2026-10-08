#!/usr/bin/env python3
"""
Генератор синтетического датасета событий покупок (~10 ГБ, e-commerce RetailFlow).

Использует Spark, чтобы сгенерировать N строк случайных «сырых» событий заказа
(разные типы: view/cart/purchase + поля каталога). Пишет исходный датасет в Parquet
(это «сырьё», из которого затем будем конвертировать в другие форматы).

Использование:
    spark-submit gen_data.py --rows 100000000 --out data/source
    # для быстрой проверки (не 10 ГБ, ~100 МБ):
    spark-submit gen_data.py --rows 1000000 --out data/source_small

Параметры:
    --rows      сколько строк событий (для ~10 ГБ Parquet обычно нужно ~80-120 млн)
    --out       путь к выходному каталогу Parquet
    --seed      сид для воспроизводимости
    --repart    число партиций при записи (по умолчанию — авто)
"""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def build_spark():
    return (
        SparkSession.builder
        .appName("retailflow-gen")
        # memory для локального запуска, подстройте под свою машину
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )


def generate(spark, rows: int, seed: int):
    """Генерируем широкую таблицу событий, похожую на сырьё из Kafka/bronze."""
    # базовый range от 0 до rows-1 (Spark сделает партиции авто)
    df = spark.range(0, rows)

    rnd = F.rand(seed)          # 0..1 с сидом
    rnd_f = F.randn(seed)       # нормальное распределение с тем же сидом

    df = (
        df
        # уникальный id события (важно: range+offset даёт уникальность)
        .withColumn("event_id", F.concat(F.lit("evt-"), F.col("id")))
        # тип события с вероятностями
        .withColumn("event_type",
                    F.when(rnd < 0.55, F.lit("view"))
                     .when(rnd < 0.80, F.lit("cart"))
                     .when(rnd < 0.97, F.lit("purchase"))
                     .otherwise(F.lit("refund")))
        # пользователь / сессия
        .withColumn("user_id", (rnd * 5_000_000).cast("int"))
        .withColumn("session_id", (rnd * 20_000_000).cast("long"))
        # каталог
        .withColumn("product_id", (rnd * 1_000_000).cast("int"))
        .withColumn("seller_id", (rnd * 50_000).cast("int"))
        .withColumn("category_id", (rnd * 500).cast("int"))
        # цена в рублях, логнормальное распределение: exp(норм) * scale
        .withColumn("price",
                    F.ceil(F.exp(4.0 + 0.6 * rnd_f) * 10).cast("decimal(12,2)"))
        .withColumn("quantity", F.ceil(rnd * 5 + 1).cast("int"))
        # гео/устройство
        .withColumn("city_id", (rnd * 300).cast("int"))
        .withColumn("device",
                    F.when(rnd < 0.5, F.lit("mobile"))
                     .when(rnd < 0.8, F.lit("web"))
                     .otherwise(F.lit("tablet")))
        .withColumn("is_promo", (rnd < 0.15))
        # время события: равномерно за 30 дней
        .withColumn("ts",
                    F.to_timestamp(
                        F.lit("2026-08-01 00:00:00") +
                        F.expr("cast(rand(%d) * 30 * 86400 as int) * interval 1 second" % seed)))
        .drop("id")
    )
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--repart", type=int, default=0)
    args = ap.parse_args()

    spark = build_spark()
    df = generate(spark, args.rows, args.seed)

    if args.repart:
        df = df.repartition(args.repart)
    # запись через partitionBy сама партиционирует по event_type (без доп. shuffle)

    t0 = time.time()
    df.write.mode("overwrite").partitionBy("event_type").format("parquet").save(args.out)
    dt = time.time() - t0

    print(f"[gen] rows={args.rows} ready in {dt:.1f}s -> {args.out}")
    print("[gen] размер проверить: du -sh ../data/source. Если <10GB — увеличьте --rows")


if __name__ == "__main__":
    main()
