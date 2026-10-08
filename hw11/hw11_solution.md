# HW11 — Универсальные Argo WorkflowTemplate + сборка пайплайна

Сделано под RetailFlow: 3 переиспользуемых шаблона решают частые задачи оркестрации
(запуск Spark-джобы, проверка качества данных, уведомления) и один главный `Workflow`,
который собирает их в ночной пайплайн построения silver-витрины.

## 1. WorkflowTemplate: `spark-job`
Файл: `templates/spark-job.template.yaml`
- **inputs.parameters:** `main-app-file`, `input-path`, `output-path`, `application-name`,
  `driver-memory`, `executor-memory` — гибко под любую джобу и объём данных.
- **outputs:** `parameters.job-status` (SUCCESS/FAILED) — результат полезен последующим шагам
  и для алертов.
- Возврат: статус; ретраи (`retryStrategy: 2`) для устойчивости.
- Переиспользуется для любой Spark-обработки (строки/витрины/ML-фичи) — меняются только входы.

## 2. WorkflowTemplate: `data-quality`
Файл: `templates/data-quality.template.yaml`
- **inputs.parameters:** `table`, `checks` (файл правил), `fail-on-error` — возможность и
  «мягкого» (только отчёт), и «жёсткого» gate.
- **outputs:** `artifact.report` (отчёт SQL/HTML), `parameters.quality-status` (passed/failed).
- Возвращает отчёт-артефакт + булево решение; `fail-on-error` останавливает пайплайн при провале.

## 3. WorkflowTemplate: `notify`
Файл: `templates/notify.template.yaml`
- **inputs.parameters:** `channel` (telegram/slack/webhook), `message`, `severity`.
- **outputs:** `parameters.notify-status` (sent/failed).
- Возвращает статус отправки; универсален для уведомлений об успехе/сбое.

## 4. Главный Workflow: `retailflow-nightly-`
Файл: `workflow.yaml`
- Собирает шаблоны в единый пайплайн:
  1. `spark-job` — из сырых событий (raw/event-date) строим silver-витрину;
  2. `data-quality` — проверка качества silver (`fail-on-error=true`) — gate;
  3. `notify` — успех-ветка (если quality-status=passed), сообщает владельцу;
  4. `onExit: on-error` — fail-ветка: критичное уведомление при любой ошибке.
- Входные аргументы (event-date, пути, app, checks) — пайплайн легко параметризуется из
  CI/CD и запускается для любого дня.

## Выводы
- Шаблоны отделены от конкретики (только входы/выходы) → переиспользуются в сотне Workflow
  без дублирования.
- Возвращаемые параметры/артефакты позволяют строить конвейеры с gate-и-ветками
  (качество → публикация; сбой → уведомление).
- `retryStrategy`, артефакты отчёта и статусы — задел для наблюдаемости (hw13) и CI/CD (hw12).
