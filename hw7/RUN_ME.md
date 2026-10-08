# hw7 — Инструкция по запуску

Задача: эксперименты с одновременной записью и тест производительности Delta / Hudi / Iceberg,
+ кейс «когда Hudi/Delta вместо Iceberg».

## Требования и пакеты

Установлен Spark **4.2.0** (Scala 2.13) — нужны пакеты этой ветки (не Scala 2.12, как в
черновике!). Проверенные координаты:

```
--packages io.delta:delta-spark_2.13:4.4.1,\
           org.apache.hudi:hudi-spark4.0-bundle_2.13:1.2.1,\
           org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0
```

- Delta 4.4.x / Iceberg 1.12 / Hudi 1.2.x — ветки под Spark 4.0+ (совместимы с 4.2).
- Первый запуск скачает ~300 МБ зависимостей в `~/.ivy2` — один раз.

В скриптах включён локальный каталог Iceberg (`spark.sql.catalog.local`, hadoop-типа,
warehouse `/tmp/iceberg_wh`). При необходимости поправьте пути.

## Шаги (из `hw7/scripts`)

**0. Smoke-тест (одна команда; если `DELTA OK: 5` — пакеты и конфиги работают):**
```bash
cat > /tmp/delta_smoke.py <<'EOF'
from pyspark.sql import SparkSession
s = (SparkSession.builder.appName("delta-smoke").master("local[1]")
     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
     .getOrCreate())
s.range(5).write.format("delta").mode("overwrite").save("/tmp/delta_smoke_tbl")
print("DELTA OK:", s.read.format("delta").load("/tmp/delta_smoke_tbl").count())
EOF
spark-submit --packages io.delta:delta-spark_2.13:4.4.1 --driver-memory 2g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 /tmp/delta_smoke.py
```

**VPN (macOS):** из-за VPN-интерфейса Spark не может привязаться к случайному порту —
`java.net.BindException: error(-49) Can't assign requested address: Service 'sparkDriver'`.
Поэтому ко всем командам ниже добавлены
`--conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1`
(драйвер биндится на loopback). Если VPN выключен — флаги просто привязывают к localhost,
мешать не будут.

## Iceberg на Spark 4.2 не работает — гоняем его на Spark 4.0

`iceberg-spark-runtime` (последний — 1.12.0) собран под Spark 4.0/4.1, а в Spark **4.2.0**
изменился API: `View` стал классом вместо интерфейса → `IncompatibleClassChangeError:
SparkView can not implement ...catalog.View`. Delta и Hudi на 4.2 работают (их пакеты
оказались совместимы), поэтому **прогоны delta/hudi остаются на Spark 4.2, а Iceberg
(запись + чтение + concurrent) делаем на отдельном Spark 4.0.4** — той версии, под
которую собран `iceberg-spark-runtime-4.0_2.13`.

Установка отдельного Spark 4.0.4 (один раз):
```bash
mkdir -p ~/opt && cd ~/opt
curl -O https://dlcdn.apache.org/spark/spark-4.0.4/spark-4.0.4-bin-hadoop3.tgz   # ~450 МБ
tar -xzf spark-4.0.4-bin-hadoop3.tgz
export SPARK40="$HOME/opt/spark-4.0.4-bin-hadoop3"
export PYSPARK_PYTHON=/opt/homebrew/bin/python3.12   # python для pyspark 4.0
```
(в новых терминалах `SPARK40`/`PYSPARK_PYTHON` нужно выставить заново; либо подставьте путь напрямую)

Прогоны Iceberg (из `hw7/scripts`; пакет — только Iceberg!):
```bash
# чтобы в таблице не осталось «хвостов» от неудачного прогона на 4.2:
rm -rf ../data/tbl_small/iceberg ../data/tbl/iceberg

$SPARK40/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0 \
  --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
  bench_write.py --src ../data/source_small.parquet --outdir ../data/tbl_small --format iceberg

$SPARK40/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0 \
  --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
  bench_read.py --outdir ../data/tbl_small --n 2 --format iceberg

$SPARK40/bin/spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0 \
  --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
  concurrent.py --format iceberg --outdir ../data/conc_iceberg
```
Флаг `--format iceberg` (добавлен в `bench_write.py`/`bench_read.py`) запускает только
Iceberg и **дозаписывает** сводки `bench_*_RESULTS.txt`, не затирая уже полученные
строки delta/hudi (для полного датасета `source.parquet` замените `source_small` → `source`/
`tbl_small` → `tbl`/`--n 3`). После прогонов соберите iceberg-строки из сводок.

**Про конфиги:** Delta/Hudi/Iceberg на Spark 4 требуют РАЗНЫЕ `spark.sql.extensions` и
`spark_catalog`, поэтому **скрипты hw7 теперь создают свою SparkSession под каждый формат**
(эти настройки вшиты в `bench_write.py`, `bench_read.py`, `concurrent.py` — отдельно их
задавать не нужно).

**ВАЖНО:** пакеты вписывайте прямо в каждую команду (не через переменную `$P`) — при переносе
между строками/сессиями теряется. Если опять `DATA_SOURCE_NOT_FOUND: delta` — значит `--packages`
не передалось или не скачалось; проверьте вывод (должны быть строки `fetch ... io.delta...`).

**1. Данные:**
```bash
spark-submit --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
  gen_data.py --rows 200000 --out ../data/source_small.parquet                 # проверка
spark-submit --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 \
  gen_data.py --rows 3000000 --out ../data/source.parquet                      # полный
```

**2. Запись (10/20/50/100%) — размер и время:**
```bash
PKGS="io.delta:delta-spark_2.13:4.4.1,org.apache.hudi:hudi-spark4.0-bundle_2.13:1.2.1,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.12.0"
# одну строку целиком (PKGS и spark-submit в одной строке):
spark-submit --packages "$PKGS" --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 bench_write.py --src ../data/source_small.parquet --outdir ../data/tbl_small
spark-submit --packages "$PKGS" --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 bench_write.py --src ../data/source.parquet        --outdir ../data/tbl
cat bench_write_RESULTS.txt
```

или без переменной (просто вставить пакеты в обе команды).

**3. Чтение (full scan / фильтр по дате / агрегация):**
```bash
spark-submit --packages "$PKGS" --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 bench_read.py --outdir ../data/tbl_small --n 2     # проверка
spark-submit --packages "$PKGS" --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 bench_read.py --outdir ../data/tbl      --n 3
cat bench_read_RESULTS.txt
```

**4. Одновременная запись (по одному прогону на формат):**
```bash
spark-submit --packages "$PKGS" --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 concurrent.py --format delta  --outdir ../data/conc_delta
spark-submit --packages "$PKGS" --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 concurrent.py --format hudi   --outdir ../data/conc_hudi
spark-submit --packages "$PKGS" --driver-memory 6g --executor-memory 6g \
  --conf spark.driver.host=127.0.0.1 --conf spark.driver.bindAddress=127.0.0.1 concurrent.py --format iceberg --outdir ../data/conc_iceberg
cat concurrent_*_RESULTS.txt
```

## Примечания по поведению
- **Concurrent:** на маленьких таблицах конфликты редки (быстрые коммиты последовательно).
  Чтобы «разозлить» — увеличьте `--rows` (дольше коммиты). Формат скрипта ловит исключение
  воркера и печатает его тип (ожидаемо для одного из воркеров).
- Iceberg без external catalog на локальной ФС слабее показывает конкуренцию, чем с
  HiveMetastore/lock-сервером — учтите в интерпретации.

## Куда подставлять
- В `hw7_solution.md`: таблицы «Запись (10/20/50/100%)», «Чтение», результаты concurrent-прогонов,
  и заполненный вывод «кто меньше/быстрее писать/быстрее читать».
