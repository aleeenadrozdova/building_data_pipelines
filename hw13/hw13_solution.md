# HW13 — Мониторинг на базе Prometheus + Grafana для кластера RetailFlow

Кластер Kubernetes, в котором работают **долгоживущие микросервисы**, **batch-джобы Spark**.
Ниже: архитектура мониторинга, 5+ метрик с лейблами, 3+ дашборда с PromQL, 4+ правила алертинга,
решение сбора метрик короткоживущих Spark-джоб. Оформлено как архитектурная документация.

---

## 1. Архитектура сбора метрик

```
                 микросервисы   ── метрики /metrics (Prometheus client)──┐
                 kube-state-metrics ── метрики K8s-объектов ────────────┤
                 node-exporter (DaemonSet) ── метрики узлов ────────────┤
                 Spark-джобы (см. §5, через PushGateway) ───────────────┼──▶ Prometheus
                 Argo Workflows metrics ─────────────────────────────────┘      │
                                                                                ▼
                                                       Alertmanager ──▶ ротация (PagerDuty/Telegram)
                                                                                │
                          Grafana (дашборды/алерты по PromQL) ◀────── Prometheus
```

Компоненты: Prometheus (retention/TSDB), Alertmanager (ротация тегов, дедуп), Grafana
(дашборды + опционально внутренние алерты), kube-state-metrics, node-exporter, cAdvisor,
Spark: PrometheusServlet + PushGateway для batch.

---

## 2. Ключевые метрики (5+) с лейблами

**M1. Health пода микросервиса**
`up{job, namespace, pod, app}`
- Значение 1/0. `job` — тип сервиса, `namespace` — среда, `app` — имя.
- Итог по сервису: `sum(up{namespace="prod", app="api-gateway"}) by (pod)` .

**M2. SLO латентности API**
`histogram_quantile(0.95, sum(rate(retailflow_http_request_duration_seconds_bucket{
  app="api-gateway"}[5m])) by (le, route))`
- Лейблы: `app`, `route`, `method`, `status_code`.

**M3. Ошибки сервисов (rate 5xx)**
`sum(rate(retailflow_http_requests_total{status=~"5.."}[5m])) by (app)`

**M4. Производительность Spark-джобы**
`spark_executor_cores` / `spark_task_failed` / `spark_stage_failed_tasks_total{job_name, app_id, stage_id}`
- Лейблы: `job_name` (джоба), `app_id` (уникальный), `stage_id`.
- Утилизация исполнителей: `rate(spark_executor_cpuTime_seconds_total{job_name}[1m])/60`.

**M5. Данные пайплайна (бизнес-метрики)**
`retailflow_rows_processed{job_name, layer, event_date}` и `retailflow_data_freshness_seconds`
- Лейблы: `layer` (bronze/silver/gold), `job_name`, `event_date`.
- Рост: `sum(increase(retailflow_rows_processed{layer="gold"}[24h])) by (job_name)`.

**M6. Health очередей/буферов** (Kafka, например)
`kafka_consumer_lag{consumer_group, topic}` / `redis_memory_used_bytes`.

---

## 3. Дашборды (3+) с примерами PromQL

**D1. «Health кластера и приложений»**
- Здоровье нод/подов: `count(up == 0)` по namespace.
- CPU/mem: `sum by (namespace) (rate(container_cpu_usage_seconds_total[5m]))`
- SLO-панель: `100 - 100 * sum(rate(retailflow_http_requests_total{status=~"5.."}[5m]))/sum(rate(retailflow_http_requests_total[5m]))`
- Таблица «занятости» подов: `kube_pod_container_status_running`.

**D2. «Spark — производительность джоб»**
- Топ jоб по времени: `topk(10, rate(spark_task_duration_seconds_sum{job_name}[1m]))`
- Сбойные задачи: `sum(increase(spark_stage_failed_tasks_total{job_name}[10m])) by (job_name)`
- Утилизация исполнителей: `avg by (job_name) (rate(spark_executor_cpuTime_seconds_total[1m]))`.
- Продолжительность воркфло (из Argo metrics): `histogram_quantile(0.9, argo_workflow_task_duration_seconds_bucket)` .

**D3. «Бизнес-показатели (freshness/объём)»**
- Обработано строк за сутки: `sum(increase(retailflow_rows_processed{layer="gold"}[24h])) by (job_name)`
- Свежесть витрин: `retailflow_data_freshness_seconds` + пороговая линия.
- LTV/выручка из витрин (через business метрики): `sum(rate(retailflow_revenue_total[1d]))`.

---

## 4. Правила алертинга (4+) с условиями/серьёзностью/ложными срабатываниями

**A1. Под/сервис недоступен**
`up{namespace="prod"} == 0` ; `for: 2m` ; severity: `critical`.
- Ложные: рестарт при деплое, rolling-update — нужен мониторинг конкретной реплики (`kube_deployment` minAvailable).

**A2. Высокая доля 5xx**
`100 * sum(rate(retailflow_http_requests_total{status=~"5.."}[5m]))/sum(rate(retailflow_http_requests_total[5m])) > 5`
`for: 2m` ; severity: `warning` → `critical` при >15%.
- Ложные: всплеск тестового трафика — фильтровать по `namespace`/исключить load-test.

**A3. Падение свежести витрины**
`retailflow_data_freshness_seconds{layer="gold"} > 3600` ; `severity: warning`.
- Ложные: выходные/необязательные джобы — править порог/исключать по `event_date` расписанию.

**A4. Сбойные задачи Spark**
`sum(increase(spark_stage_failed_tasks_total{job_name}[10m])) > 0` ; `severity: warning`.
- Ложные: ретраи Spark (одна задача упала, потом перезапустилась) — увеличить окно/порог, смотреть `app_id`.

**A5. Высокая задержка очереди Kafka**
`kafka_consumer_lag{group="group.ingest-fin"} > 100000` ; `severity: critical`.
- Ложные: нормальный бэкап/раскатка — добавить `for: 5m`.

---

## 5. Сбор метрик с короткоживущих Spark-джоб

**Проблема:** batch-джоба Spark живёт минуты-часы и исчезает — Prometheus (pull) не успевает
опросить её эндпоинт; метрика «пропадёт» вместе с подом.

**Решение — PushGateway (push-модель для batch):**
1. В Spark включить `spark.ui.prometheus.enabled=true` + `spark.metrics.appStatusSource.enabled=true`.
2. Настроить периодический push метрик (`/metrics/json`) в PushGateway:
   - `spark.ui.prometheus.pushgateway.address=pushgateway:9091`,
   - `spark.ui.prometheus.pushgateway.jobName=<job_name>` — лейбл уникальной группы.
3. PushGateway хранит последнее значение и отдаёт его Prometheus (Prometheus pull с PG).
4. **Очистка (важно!):** в `onExit`/по завершении джобы удалять метрику из PushGateway
   (`curl -X DELETE http://pg:9091/metrics/job/<job_name>/instance/<app_id>`), иначе останутся
   «призрачные» значения закончившейся джобы. Хранить `instance=<app_id>` уникально.
5. Альтернатива (если нужна большая точность): **Prometheus remote-write / метрики в кластер**
   через Spark Operator integration (Spark Operator умеет собирать метрики как ServiceMonitor).

**Итог:** Prometheus видит и «живущих» микросервисов (pull), и короткоживущие Spark-джобы
(push через PushGateway с корректной очисткой), обеспечивая одинаковую картину для дашбордов
D2 и алертов A4.
