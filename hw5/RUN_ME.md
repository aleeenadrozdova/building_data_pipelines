# hw5 — Инструкция по запуску

Задача: эксперименты с оптимизацией Spark-приложений (проблемы → демонстрация оптимизаций,
+ свободный кейс — **проблема, не описанная в классическом списке**). Датасет e-commerce.

## Требования
- Spark 3.x+ (`spark-submit`), Python 3.8+.
- Память: флаги `--driver-memory`/`--executor-memory` (иначе JVM стартует с 1 ГБ → OOM).

## Шаги

Из папки `hw5/scripts`:

**0. Быстрая проверка на маленьком датасете (~300к строк):**

```bash
spark-submit --driver-memory 4g gen_data.py --rows 300000 --out ../data_small
spark-submit --driver-memory 4g optimize_join.py --data ../data_small --n 2
spark-submit --driver-memory 4g optimize_agg.py --data ../data_small --n 2
spark-submit --driver-memory 4g opt_upsert.py --rows 200000 --out ../data_small_mor --n 1
```

**1. Полный прогон (~20 млн строк):**

```bash
spark-submit --driver-memory 8g --executor-memory 8g gen_data.py --rows 20000000 --out ../data
spark-submit --driver-memory 8g --executor-memory 8g optimize_join.py --data ../data
spark-submit --driver-memory 8g --executor-memory 8g optimize_agg.py --data ../data
spark-submit --driver-memory 8g --executor-memory 8g opt_upsert.py --rows 5000000 --out ../data_mor
```

Сводки: `optimize_join.py`/`optimize_agg.py` печатают `вариант<TAB>сек` в stdout,
а `opt_upsert.py` пишет `opt_upsert_RESULTS.txt` (в каталоге `scripts/`).

## Что делает каждый скрипт

| Скрипт | Проблема | Оптимизации |
|---|---|---|
| `gen_data.py` | — | генерация датасета с перекосом ключей + маленьким справочником |
| `optimize_join.py` | shuffle при join с маленькой таблицей | broadcast join (hint), AQE |
| `optimize_agg.py` | data skew при groupBy (горячие продавцы) | salting (разбиение горячего ключа) |
| `opt_upsert.py` (свободный кейс, НЕ из стандартного списка) | инкрементальные обновления витрины: переписывание всей таблицы ради 1–5% изменений | двухслойный **merge-on-read** (база + слой изменений, дешёвый join при чтении, компакция) |

## Примечания
- Для честности каждый скрипт берёт минимум из N повторов (`--n`, по умолчанию 3).
- На маленьком датасете разница может быть зашумлена — выводы следует делать на полном прогоне.