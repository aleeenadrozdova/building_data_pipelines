# hw6 — Инструкция по запуску

Задача: 3 кейса, где DataFrame гарантированно быстрее RDD, + (*) 2 кейса, где **SQL быстрее
эквивалентного DataFrame API** (сравнение планов через `explain()`).

## Требования
- Spark 3.x (`spark-submit`), Python 3.8+.

## Шаги

Из папки `hw6/scripts`:

**0. Быстрая проверка (маленький датасет):**

```bash
spark-submit gen_data.py --rows 200000 --out ../data_small
spark-submit cases_df_vs_rdd.py --data ../data_small --n 2
spark-submit sql_vs_df.py --data ../data_small --n 2
```

**1. Полный прогон (~5 млн строк):**

```bash
spark-submit gen_data.py --rows 5000000 --out ../data
spark-submit cases_df_vs_rdd.py --data ../data
spark-submit sql_vs_df.py --data ../data
```

Каждый скрипт печатает сводку времён; `sql_vs_df.py` дополнительно печатает физические планы
(explain) — по ним видно число `Exchange`/`Window`-узлов у SQL и DF.

## Что делает каждый скрипт

| Скрипт | Содержание |
|---|---|
| `gen_data.py` | датасет событий `events` + `events_nested` (array<struct>) |
| `cases_df_vs_rdd.py` | C1 много-агрегаций, C2 оконная топ-N, C3 вложенные типы; DF vs RDD |
| `sql_vs_df.py` | (*) S1 одно GROUP BY vs несколько groupBy+join; S2 одно SELECT с 3 окнами vs 3 отдельных `.over()`; печать планов |

