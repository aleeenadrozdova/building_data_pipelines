# hw11 — Инструкция (запуск в кластере с Argo Workflows)

Задача: 3+ универсальных WorkflowTemplate + основной Workflow. YAML готовы в `templates/`
и `workflow.yaml`. Ниже — как их проверить в реальном Argo Workflows.

## Требования
- Рабочий кластер K8s с установленным **Argo Workflows** (`argo` CLI или Argo UI).
- Образы/секреты для джоб: не требуются для линт-проверки; для фактического запуска — см. ниже.

## Шаги

**1. Синтаксис-проверка (без кластера):**
```bash
argo lint templates/spark-job.template.yaml
argo lint workflow.yaml
```

**2. Установить шаблоны (ClusterWorkflowTemplate, чтобы были во всех NS):**
```bash
kubectl apply -n argo -f templates/spark-job.template.yaml
kubectl apply -n argo -f templates/data-quality.template.yaml
kubectl apply -n argo -f templates/notify.template.yaml
```
> ВНИМАНИЕ: в примере используется `templateRef` (ссылка на шаблон по имени). Если
> развернёте как `ClusterWorkflowTemplate`, имя должно совпадать; в `workflow.yaml`
> поправьте namespace при необходимости.

**3. Пробный запуск пайплайна:**
```bash
argo submit workflow.yaml -p event-date=2026-09-22
argo watch @latest
```

**4. Просмотр результата:**
```bash
argo get @latest
# отчёты качества — в outputs/artifacts; статус success-шага notify — в parameters
```

## Что проверить
- Пайплайн проходит шаги: spark-job -> data-quality -> notify-success (или on-error).
- В случае отсутствия реальных образов (retailflow/*) шаги завершатся с ошибкой запуска —
  это ожидаемо; для «холостого» прогона замените образы на существующие (например,
  `python:3.11` + ваш код) и временно отключите retry/чтение реального S3.

## Примечания
- `templateRef` в `steps` предполагает, что шаблон существует до запуска Workflow.
- Значения путей (`s3a://...`) — пример для MinIO; подставьте реальные.
