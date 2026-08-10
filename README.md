# Analytics Reporting Platform

ETL/DWH-платформа для автоматизированной подготовки ежемесячной web-analytics отчётности из нескольких внешних источников.

**Portfolio Edition** — восстановленная и переработанная версия аналитического MVP: вместо notebook-процесса — воспроизводимый Python-пакет, слоистый PostgreSQL DWH, оркестрация в Apache Airflow, Data Quality и управляемые режимы запуска.

Клиентские идентификаторы, реальные credentials, дампы и чувствительные KPI в публичный репозиторий не входят.

```text
Adobe Analytics API ───────┐
                           ├→ Airflow → PostgreSQL → RAW → CALC → CDM → BI
Yandex Wordstat / Cloud ───┘
```

Область текущей Portfolio Edition — **Monthly pipeline**. Weekly Organic в продуктовую архитектуру Monthly не входит.

Подробный freeze состояния: [`docs/current-state.md`](docs/current-state.md).

---

## Задача проекта

Исходный процесс ежемесячной web-analytics отчётности опирался на ручные и частично notebook-driven шаги:

* выгрузка данных из Adobe Analytics;
* подготовка нескольких аналитических разрезов;
* расчёт KPI и сравнительных показателей;
* сборка отчётных витрин;
* подготовка данных для визуализации.

Такой путь плохо масштабировался: сложно воспроизводить период, контролировать качество и безопасно сопровождать pipeline в операционном режиме.

Цель Portfolio Edition — автоматизировать этот процесс и зафиксировать воспроизводимую цепочку:

```text
источники
→ ingestion
→ RAW
→ расчётный слой (CALC)
→ CDM
→ Data Quality
→ BI
```

Платформа развивает существовавший MVP до инженерно полноценной системы:

| Направление | Смысл в Portfolio Edition |
|---|---|
| Python package | Код ETL вынесен из Jupyter notebooks в тестируемый пакет |
| PostgreSQL DWH | Явные слои RAW → CALC → CDM |
| Airflow | Операционная оркестрация Monthly pipeline |
| Режимы запуска | Incremental, rebuild, validate; планирование backfill |
| Reliability | Retries, timeouts, `max_active_runs=1`, только закрытые месяцы |
| Data Quality | Coverage, contracts, reconciliation, артефакты валидации |
| Тесты | Unit regression по source / execution / DQ / DAG |
| Secrets | Runtime configuration вне Git |
| BI | Целевой consumer-слой поверх CDM (следующий этап) |

Бизнес-метрики и семантика витрин следуют восстановленной Monthly-модели. Portfolio Edition не изобретает новую KPI-таксономию.

---

## Источники данных

### Adobe Analytics

Основной источник web-analytics трафика — **Adobe Analytics API**.

На высоком уровне pipeline:

1. формирует отчётные запросы к API;
2. последовательно извлекает Monthly-срезы;
3. сохраняет результаты в RAW максимально близко к source contract.

В Git не публикуются report suite ID, segment IDs, company ID, client credentials и реальные URL страниц. Они задаются через private runtime configuration.

Adobe RAW-марты Monthly:

* `raw_aa_monthly_metrics`
* `raw_aa_monthly_by_section`
* `raw_aa_monthly_by_url`

Извлечение Adobe выполняется **последовательно** внутри task `load_raw` (без параллельных Adobe-pull'ов).

### Yandex Wordstat (Yandex Cloud)

Второй источник — **Yandex Wordstat** через **Yandex Cloud Search API**.

Используется:

```text
Yandex Cloud Search API
Wordstat API v2
GetDynamics  (endpoint: /v2/wordstat/dynamics)
```

Подключение идёт через Yandex Cloud. Авторизация — API key service account; ключ и `folderId` передаются только через runtime configuration и **не хранятся в Git**.

Публичные placeholders:

```env
YANDEX_WORDSTAT_API_KEY=
YANDEX_WORDSTAT_FOLDER_ID=
```

Для метрики Brand Demand загружаются monthly counts по двум configured queries:

```text
samsung
самсунг
```

В RAW они хранятся отдельно (grain `month × query`). Расчётная метрика:

```text
brand_demand =
  count("samsung") +
  count("самсунг")
```

Это техническая сумма counts двух фраз. Интерпретацию Wordstat как «спроса» README не задаёт — только способ расчёта в DWH.

**Brand Demand на основе Yandex Wordstat — расширение Portfolio Edition.** В исходном Legacy MVP этого источника и этой витрины не было.

Подтверждённое historical coverage Brand Demand:

```text
2025-01 → 2026-07
```

Для **historical initialization / backfill** использовался range extract: **2** range API-вызова на весь загружаемый интервал — по одному на каждый query.

Для штатного **monthly incremental** в текущей конфигурации выполняются **2 API-вызова на отчётный месяц**: по одному на каждую configured phrase. Это свойство текущей конфигурации Brand Demand (две фразы), а не ограничение самого Wordstat API.

Task `load_wordstat_raw` независим от Adobe extraction: retry Wordstat не перезапускает Adobe.

---

## Архитектура

### Поток данных

```text
                     ┌──────────────────────┐
                     │ Adobe Analytics API  │
                     └──────────┬───────────┘
                                │
                     ┌──────────▼───────────┐
                     │      load_raw        │
                     └──────────┬───────────┘
                                │
┌──────────────────────┐        │
│ Yandex Cloud         │        │
│ Wordstat API v2      │        │
└──────────┬───────────┘        │
           │                    │
┌──────────▼───────────┐        │
│  load_wordstat_raw   │        │
└──────────┬───────────┘        │
           │                    │
           └─────────┬──────────┘
                     ▼
              PostgreSQL RAW
                     ↓
                   CALC
                     ↓
                    CDM
                     ↓
               Data Quality
                     ↓
                    BI
```

### Airflow DAG `monthly_private_pipeline`

```text
determine_periods
        ↓
 ┌──────┴─────────┐
 ↓                ↓
load_raw     load_wordstat_raw
 └──────┬─────────┘
        ↓
    build_calc
        ↓
    build_cdm
        ↓
   data_quality
        ↓
   final_status
```

| Task | Роль |
|---|---|
| `determine_periods` | Определяет закрытые месяцы по режиму и состоянию DWH |
| `load_raw` | Последовательный Adobe extract → Monthly RAW |
| `load_wordstat_raw` | Yandex Wordstat → Brand Demand RAW |
| `build_calc` | RAW → CALC; стартует только после обоих RAW tasks |
| `build_cdm` | CALC → CDM |
| `data_quality` | Monthly DQ suite + validation artifacts |
| `final_status` | Итог run; critical DQ FAIL валит DAG |

Ключевые свойства:

* Adobe и Wordstat — **независимые** source tasks;
* они могут идти **параллельно** (`max_active_tasks=2`);
* CALC ждёт завершения обоих RAW;
* retry Wordstat **не** вызывает повторный Adobe extraction;
* `max_active_runs=1` защищает Monthly pipeline от параллельного запуска;
* обрабатываются только **закрытые** календарные месяцы;
* schedule: `08:00` 1-го числа месяца, timezone `Europe/Moscow`;
* safe default: `mode=incremental`, `dry_run=True`.

---

## Слои DWH

Модель слоёв клиент-нейтральна в публичных материалах.

| Слой | Monthly marts (подтверждено) |
|---|---:|
| **RAW** | 4 (3 Adobe + Wordstat brand queries) |
| **CALC** | 6 (5 Adobe-derived + `brand_demand`) |
| **CDM** | 16 (15 Adobe-derived + `organic_brand_demand`) |

### RAW

Слой исходных данных после API ingestion. Данные сохраняются максимально близко к source contract.

Wordstat:

```text
raw_wordstat_brand_queries
```

Grain:

```text
month × query
```

Поля:

```text
month
query
count
load_dt
```

### CALC

Слой бизнес-расчётов и трансформаций поверх RAW.

Brand Demand:

```text
calc_brand_demand
```

```text
month
brand_demand
calc_dt
```

где `brand_demand` = сумма `count` по двум Wordstat queries за месяц.

### CDM

**Consumer Data Mart** — готовые витрины для отчётности и BI.

Основные группы Monthly CDM:

* Traffic KPI / Overall Traffic Trend / Traffic Source Mix
* Geo
* Organic Search (KPI, engine mix/trend)
* Explore / Buying Guides
* Sections / Top Pages / Devices
* Brand Demand

Новая витрина Brand Demand:

```text
cdm_organic_brand_demand
```

Contract:

```text
month
metric
cdm_dt
```

`metric = brand_demand`. Таблица предназначена для графика Brand Demand в блоке **Organic Search Trends**.

DWH хранит историю; BI использует rolling window последних 13 месяцев (см. ниже).

---

## 13-месячный тренд

CDM **не** ограничен 13 строками.

История Brand Demand хранится начиная с:

```text
2025-01
```

Для отчётного месяца BI выбирает окно:

```text
report_month − 12 months
…
report_month
```

то есть всегда **13 последовательных месяцев**. Хранение в DWH шире окна отображения.

---

## Технологический стек

### Data sources

* Adobe Analytics API
* Yandex Cloud Search API / Wordstat API v2 (`GetDynamics`)

### Data Engineering

* Python 3.10–3.12
* SQL / SQLAlchemy
* PostgreSQL (отдельные БД для RAW / CALC / CDM)

### Orchestration

* Apache Airflow (`monthly_private_pipeline`)
* DAG scheduling
* retries / timeouts на уровне клиентов и task execution timeout
* incremental / rebuild / validate
* backfill planning
* manual runs

### Infrastructure

* Docker / Docker Compose
* isolated PostgreSQL environments
* Apache Airflow

### Quality

* pytest
* Data Quality suite
* reconciliation RAW → CALC и CALC → CDM
* duplicate / coverage / contract checks

### Configuration & Security

* environment variables
* private runtime configuration
* `.env.example`, `config/client-config.example.yaml`
* secrets outside Git

### Runtime visibility и validation artifacts

Базовая runtime visibility (не полноценный monitoring/alerting stack):

* состояния DAG/task в Airflow;
* persistent validation artifacts (`validation.md` / `validation.json`);
* abstraction для notifier; по умолчанию **NoOp**.

### BI

BI layer — **следующий этап** после стабилизации CDM. В текущем репозитории operational BI cutover ещё не заявлен как завершённый.

---

## Режимы запуска

Неполный текущий календарный месяц никогда не обрабатывается.

| Режим | Поведение |
|---|---|
| **incremental** | Недостающие закрытые месяцы → ETL → автоматический DQ |
| **rebuild** | Выбранный период с replace-семантикой → ETL → DQ |
| **validate** | Без Adobe / API / DWH writes — только read-only DQ |
| **backfill / init** | Восстановление истории: incremental при пустой БД планирует полный init; CLI умеет печатать детерминированный backfill-plan |

### CLI (фактический синтаксис)

Entry point: `analytics-reporting`.

```text
analytics-reporting monthly run --month YYYY-MM [--env-file PATH] [--client-config PATH] [--dry-run]
analytics-reporting monthly rebuild --month YYYY-MM [--env-file PATH] [--client-config PATH] [--dry-run]
analytics-reporting monthly validate --month YYYY-MM --env-file PATH [--before PATH] [--after PATH]
analytics-reporting monthly validate-suite --env-file PATH [--reference-env-file PATH] [--report-end YYYY-MM] [--output-dir PATH]
analytics-reporting monthly backfill-plan --reporting-end YYYY-MM [--reporting-start YYYY-MM] [--raw-history-start YYYY-MM]
analytics-reporting monthly snapshot --month YYYY-MM --env-file PATH --output PATH
```

`--dry-run` планирует стадии без DB writes и без внешних API-вызовов.
`validate` / `validate-suite` / `snapshot` / `backfill-plan` не выполняют Adobe/Wordstat extraction и не пишут в DWH (кроме read-only чтения, где применимо).

Операционный путь в Apache Airflow — DAG `monthly_private_pipeline` с params `mode`, `rebuild_start` / `rebuild_end`, `dry_run`.

---

## Data Quality

После ETL Airflow запускает Monthly DQ suite. Статусы: **PASS** / **WARN** / **FAIL**. Critical **FAIL** валит task `data_quality` и весь DAG. WARN сам по себе run не роняет.

Реализованные классы проверок:

* coverage периодов и inventory marts;
* duplicate business keys;
* обязательные периоды / latest expected period;
* non-negative metrics / numeric ranges;
* RAW → CALC reconciliation;
* CALC → CDM reconciliation;
* CDM / report contracts;
* Monthly-only guard.

Отдельно для Brand Demand:

```text
2 Wordstat queries / month
RAW → CALC reconciliation
CALC → CDM reconciliation
```

### Validation artifacts

```text
<validation-root>/
  monthly_private_pipeline/
    <run-id>/
      validation.md
      validation.json
```

Корень задаётся через `ARP_VALIDATION_ARTIFACT_ROOT` (private path вне Git).

---

## Тестирование

Unit-набор покрывает:

* source clients (Adobe, Wordstat);
* RAW / CALC / CDM builders;
* execution и period planning;
* orchestration helpers;
* DQ / validation;
* DAG parse и dependency graph.

Подтверждённый локальный regression baseline:

```text
233 passed, 7 skipped
```

Integration-прогоны и live API backfill остаются в private runtime. CI/CD workflows в этом репозитории пока не добавлены.

---

## Безопасность

В публичный репозиторий **не** входят:

* реальные данные и дампы PostgreSQL;
* API keys / Adobe credentials / Yandex credentials;
* report suite IDs / segment IDs / company IDs;
* connection strings с секретами;
* private `.env`;
* клиентские URL и sensitive notebook outputs;
* runtime validation artifacts и Airflow logs с клиентским контекстом.

Публичные шаблоны:

* [`.env.example`](.env.example)
* [`config/client-config.example.yaml`](config/client-config.example.yaml)

Пример placeholders:

```env
# PostgreSQL
DB_HOST=
DB_PORT=
DB_USER=
DB_PASSWORD=
RAW_DB_NAME=
CALC_DB_NAME=
CDM_DB_NAME=

# Adobe Analytics
ADOBE_CLIENT_ID=
ADOBE_CLIENT_SECRET=
ADOBE_GLOBAL_COMPANY_ID=
ADOBE_REPORT_SUITE_ID=

# Yandex Wordstat
YANDEX_WORDSTAT_API_KEY=
YANDEX_WORDSTAT_FOLDER_ID=

# Validation artifacts
ARP_VALIDATION_ARTIFACT_ROOT=
```

Локальная подготовка:

1. Скопировать `.env.example` в private `.env` (не коммитить).
2. Скопировать `config/client-config.example.yaml` в private client config.
3. Указать пути через env / Airflow Variables: `ARP_ENV_FILE`, `ARP_CLIENT_CONFIG`, `ARP_VALIDATION_ARTIFACT_ROOT`.

---

## Структура проекта

```text
.
├── dags/
│   └── monthly_private_pipeline.py
├── src/analytics_reporting_platform/
│   ├── sources/
│   │   ├── adobe/          # Adobe client / reports / retries
│   │   └── wordstat/       # Yandex Cloud Wordstat client
│   ├── raw/                # RAW extract + persistence
│   ├── calc/               # CALC builds
│   ├── cdm/                # CDM builds
│   ├── execution/          # Period planning / run models
│   ├── orchestration/      # Stage runners для Airflow / CLI
│   ├── validation/         # DQ suite + artifacts + notifier
│   ├── dq/
│   ├── observability/
│   ├── cli/
│   ├── config/
│   └── db/
├── tests/
│   ├── unit/
│   └── integration/
├── config/                 # Public example client YAML
├── docs/
│   ├── current-state.md
│   └── technical-baseline.md
├── infra/                  # Docker / isolated PostgreSQL helpers
├── scripts/
├── .env.example
└── pyproject.toml
```

---

## Текущее подтверждённое состояние

### Реализовано и проверено

* Python package `analytics_reporting_platform`
* Adobe Monthly source → RAW → CALC → CDM
* Yandex Wordstat source (Yandex Cloud) → Brand Demand RAW/CALC/CDM
* PostgreSQL layered DWH
* Monthly execution (incremental / rebuild / validate)
* Airflow DAG `monthly_private_pipeline` с параллельными RAW tasks
* Brand Demand historical coverage `2025-01 → 2026-07`
* Data Quality suite и validation artifacts
* Unit regression (`233 passed, 7 skipped`)
* Runtime / read-only validation в private окружении

### Следующие этапы

Согласно текущему freeze (`docs/current-state.md`):

* BI presentation layer / operational BI cutover
* anonymized public demo dataset
* CI/CD workflows
* optional notification backends (Telegram / email / Slack)
* public packaging / release preparation

Roadmap не считать доставленным. Weekly Organic — вне текущего Monthly scope, пока явно не переоткрыт.

---

## История проекта

### Legacy MVP

Исходная поставка опиралась на:

* Adobe Analytics API как upstream traffic source;
* Jupyter notebooks для Monthly RAW → CALC → CDM;
* PostgreSQL для слоёв данных;
* Excel dashboard как MVP reporting surface.

Private recovery-материалы (notebooks, dumps, token caches, real env) остаются вне Git.

### Portfolio Edition

Текущая версия переносит процесс в:

* тестируемый Python package;
* Airflow orchestration;
* явный DQ и reconciliation;
* дополнительные источники данных (Yandex Wordstat через Yandex Cloud);
* подготовку к новому BI layer поверх CDM.

Legacy MVP и Portfolio Edition не следует смешивать: notebooks/Excel — исторический контур; этот репозиторий — инженерный Monthly pipeline Portfolio Edition.
