# hw3 — Инструкция по запуску

Задача: бенчмарк 3+ форматов (Parquet, Avro, CSV, JSON) на датасете ~10 ГБ событий покупок.
Ниже — как сгенерировать данные и прогнать замеры локально.

## Требования

- Spark 3.x+ (`spark-submit`).
- **Avro — внешний модуль**: скрипты с `format("avro")` запускайте с
  `--packages org.apache.spark:spark-avro_2.13:<версия Spark>` (для Spark 4.x);
  для Spark 3.x суффикс `_2.12`. Без пакета падает
  `Failed to find data source: avro`.
- Память: для полного 10 ГБ прогона нужны флаги `--driver-memory`/`--executor-memory`
  (**8+ ГБ**; в скрипте `.config(...)` heap не задаёт — JVM стартует с 1 ГБ и падает OOM).
- Python 3.8+.

## Шаги

### 1. Сгенерировать данные (~10 ГБ)

Из папки `hw3/scripts`:

```bash
spark-submit --driver-memory 8g --executor-memory 8g \
  gen_data.py --rows 120000000 --out ../data/source
```

**Быстрая проверка (не 10 ГБ, ~100 МБ):**

```bash
spark-submit --driver-memory 4g gen_data.py --rows 1000000 --out ../data/source_small
```

### 2. Замер записи (время + объём)

```bash
# полный прогон (--packages нужен, иначе Avro не найдётся)
spark-submit --driver-memory 8g --executor-memory 8g \
  --packages org.apache.spark:spark-avro_2.13:4.2.0 \
  bench_write.py --src ../data/source --outdir ../data/out

# быстрая проверка: ограничить число строк
spark-submit --driver-memory 4g \
  --packages org.apache.spark:spark-avro_2.13:4.2.0 \
  bench_write.py --src ../data/source_small --outdir ../data/out_small --limit 500000
```

Скрипт выводит сводку `формат<TAB>размер_МБ<TAB>время_сек` и **сохраняет её в
`bench_write_RESULTS.txt`** 

### 3. Замер чтения (full scan / выборка / агрегация)

```bash
# полный прогон — по 3 повтора каждого замера
spark-submit --driver-memory 8g --executor-memory 8g \
  --packages org.apache.spark:spark-avro_2.13:4.2.0 \
  bench_read.py --indir ../data/out --n 3

# быстрая проверка
spark-submit --driver-memory 4g \
  --packages org.apache.spark:spark-avro_2.13:4.2.0 \
  bench_read.py --indir ../data/out_small --n 3
```

Скрипт выводит `формат<TAB>full_scan<TAB>select<TAB>aggregate` (сек, минимум из N повторов)
и **сохраняет сводку в `bench_read_RESULTS.txt`** 

## Примечания

- CSV и JSON хранят всё как текст → колонки приводятся к типам только при чтении (это тоже часть честного сравнения: «сырой» текст читается медленнее, но меньше логики на записи).
- Все форматы записываются **плоскими каталогами** (без `partitionBy`) — просто и устойчиво; партиционирование не влияет на честность сравнения форматов.
- Медиана нескольких повторов надёжнее разового замера — скрипты уже берут минимум.
