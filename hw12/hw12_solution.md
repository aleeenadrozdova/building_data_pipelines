# hw12 — Структура репозиториев и CI/CD для разнородных приложений в Kubernetes

Контекст RetailFlow: в кластере — **Spark-приложения** (обработка данных), **сторонние продукты**
из Helm (Argo Workflows, Spark Operator, MinIO, PostgreSQL) и **самописные витрины** (Node.js/React).

---

## 1. Технологический стек

| Тип | Сборка Docker-образа | Хранилище артефактов |
|---|---|---|
| Spark-приложения | **GitLab CI** (runner с docker/buildah; образ на базе `apache/spark` + наш код) | **Harbor** (registry) |
| React-витрины | **GitLab CI** (node:18 build → nginx-образ) | **Harbor** |
| Helm-чарты | — (чарт как есть) | **Harbor** (OCI-чарты) + git как источник истины |
| Universal WorkflowTemplates (hw11) | — | git (GitOps) / конфиг-файлы |

Выбор: **GitLab CI** единый для всех (один раннер/security-сканы), **Harbor** — registry
с retention/copy и scan (Trivy). Это централизует сборку и версионирование артефактов.

---

## 2. Структура репозитория

Проектирую **монorepo** `retailflow/` с жёстким разделением по модулям (плюс допускается
разнести app-исходники при росте команды — описано ниже).

```
retailflow/
├── apps/                          # ИСХОДНИКИ приложений
│   ├── spark-jobs/                #    PySpark (bronze/silver/gold), Dockerfile, pyproject
│   │   └── src/build_silver.py
│   └── dashboard/                 #    React-витрина (Node/React), Dockerfile (nginx)
├── charts/                        # HELM-ЧАРТЫ внутренних приложений
│   ├── spark-job/                 #    обёртка: SparkApplication (Spark Operator)
│   └── dashboard/                 #    деплой React-витрины
├── deploy/
│   ├── kustomize/
│   │   ├── base/                  #    общая конфигурация (labels, images placeholder)
│   │   └── overlays/{dev,stage,prod}/   # среды: image-теги, секреты, ресурсы
│   └── third-party/               # СТОРОННИЕ продукты из Helm
│       ├── argocd-apps/           #    ArgoCD Application-манифесты (argo, minio, postgres, spark-operator)
│       └── values/{dev,prod}.yaml #    значения чартов по средам
├── workflows/                     # ARGO WORKFLOW-ФАЙЛЫ для запуска джоб
│   ├── templates/                 #    универсальные WorkflowTemplate (hw11)
│   └── nightly/                   #    конкретные Workflow (с параметрами date/image)
├── .gitlab-ci.yml
└── README.md
```

**Где что лежит (по пунктам задания):**
- Helm-чарты сторонних продуктов — `deploy/third-party/` (+ ArgoCD Application-ы).
- YAML/Helm внутренних витрин — `charts/dashboard/`, деплой через `deploy/kustomize/`.
- Kustomize-оверлеи сред — `deploy/kustomize/overlays/{dev,stage,prod}`.
- Универсальные WorkflowTemplate (из hw11) — `workflows/templates/`.
- Исходники Spark-приложений — `apps/spark-jobs/`; React-витрин — `apps/dashboard/`.
- Argo Workflow-файлы для запуска джоб — `workflows/nightly/`.

---

## 3. Пайплайн: от git push до запуска в кластере

Scenario (на примере Spark-джобы, затем витрина/сторонние):

### 3.1 Сборка образа Spark-приложения и публикация в registry
```mermaid
git push (main) --> GitLab CI:
  lint+pytest --> docker build rfu-spark --> trivy scan
  --> tag: <semver>-<sha> --> push to Harbor
```
`.gitlab-ci.yml` (фрагмент, полный — `snippets/.gitlab-ci.yml`):
```yaml
build-spark-image:
  stage: build
  script:
    # тэг: версия релиза (тег git) либо хэш коммита; ${VAR:-fallback} — если тега нет, берём SHA
    - docker build -t harbor.example.com/retailflow/spark-jobs:${CI_COMMIT_TAG:-$CI_COMMIT_SHA} apps/spark-jobs
    - docker push harbor.example.com/retailflow/spark-jobs:${CI_COMMIT_TAG:-$CI_COMMIT_SHA}
```

### 3.2 Создание/генерация Argo Workflow-файла, который запускает Spark-джобу
CI генерирует конкретный Workflow YAML (`workflows/nightly/silver-<version>.yaml`), ссылающийся
на **универсальный** `spark-job` template (hw11) и образ с тегом сборки. Версия-привязка —
через аргумент `image-tag`.

### 3.3 Где и как хранится этот workflow-файл
Хранится **в том же монорепо**, в `workflows/nightly/` (GitOps-источник истины). CI делает
коммит сгенерированного YAML (или файл уже параметризован и не перегенерируется — тогда
обновляем только аргумент `image-tag` в `kustomize overlay`/Application).

### 3.4 Как и откуда Argo Workflows его подхватывает и запускает
- **Argo CD** (GitOps) следит за `workflows/nightly/` → увидев новый/изменённый `Workflow`
  (или `Application` с ним), синкает его в кластер.
- Контроллер **Argo Workflows** подхватывает CRD `Workflow`, разворачивает шаги. Если джоба
  должна идти по расписанию — Workflow внутри сам себя перезапускает (или используется
  `CronWorkflow`), шаблоны берёт из `workflows/templates/` (ClusterWorkflowTemplate).

### 3.5 Как применяются Helm/Kustomize для вспомогательных сервисов
- **Сторонние** (Argo WF, Spark Operator, MinIO, PostgreSQL): ArgoCD `Application` c
  `source: chart` (из репозитория/наблюдаемого Helm-репа) + `values/` per env —
  ArgoCD устанавливает/обновляет Helm-релизы в кластер.
- **Внутренние витрины**: Helm-чарт (`charts/dashboard`) + **Kustomize overlay** среды
  (подставляет image-тег, секреты, limits) → `kustomize build` → аргогCD применяет.

---

## 4. Обоснование выбора

- **Переиспользование WorkflowTemplate:** джобы не дублируют код — `workflows/templates/`
  один раз на платформу, конкретные `workflows/nightly/` только параметризуют (дата, образ).
- **Версионирование артефактов:** semver-тег образа привязан к коммиту → воспроизводимость,
  откат к конкретной версии джобы (переключение `image-tag` в оверлее).
- **Изоляция конфигураций сред:** Kustomize-overlays (dev/stage/prod) держат окружения и
  секреты отдельно; один и тот же чарт, разные `values`.
- **Упрощение отката:** GitOps + Argo CD — rollback = предыдущий коммит git / `Application`
  rollback; образы immutable (тег = версия), откат джобы — указание старого `image-tag`.
- **Centralized CI/registry** (GitLab CI + Harbor) даёт единый путь сборка→артефакт→деплой
  и security-скан в цепочке.
