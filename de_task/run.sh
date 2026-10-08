#!/usr/bin/env bash
# Сборка и запуск Spark-задачи с локальным Spark (Homebrew).
set -euo pipefail

cd "$(dirname "$0")"
JAR="target/de-task-1.0.0.jar"

if [ ! -f "$JAR" ]; then
  echo ">> Сборка (mvn package)..."
  mvn -q clean package
fi

INPUT="${1:-data}"
OUT="${2:-results}"
if [ ! -d "$INPUT" ]; then
  echo "Каталог данных '$INPUT' не найден. Использование: $0 <каталог с файлами сессий> [каталог результатов]"
  exit 1
fi

exec spark-submit \
  --class ru.consultantplus.de.Main \
  --master "local[*]" \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  "$JAR" \
  --input "$INPUT" \
  --out "$OUT"
