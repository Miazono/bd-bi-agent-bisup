# SQL слоя silver

## Назначение

Этот документ разбирает SQL-логику слоя `silver` в том виде, в каком она реально реализована в репозитории `db-bi-agent-bisup`.
Здесь важно не только что делает каждый запрос, но и почему именно такой способ обновления выбран в связке с `ingestion/load_silver.py`.

## Общая роль слоя

`silver` в проекте — это очищенная аналитическая модель.
На этом уровне данные уже не просто сохраняются, а приводятся к форме, удобной для анализа, соединений и дальнейших витрин.

В текущей реализации слой `silver` включает:

- `silver.dim_article`
- `silver.dim_customer`
- `silver.dim_date`
- `silver.fact_sales_line`
- `silver.fact_customer_article_stats`

## Как устроен процесс обновления

Логика слоя `silver` не живёт в одном SQL-файле.
Она разделена между Python-оркестрацией и отдельными SQL-скриптами.

### Роль `load_silver.py`

[`ingestion/load_silver.py`](/root/repos/db-bi-agent-bisup/ingestion/load_silver.py) отвечает за порядок шагов:

- проверяет, что batch уже есть в `bronze`;
- создаёт схему `iceberg.silver`, если её нет;
- применяет DDL для таблиц `silver`;
- обновляет `dim_article`;
- обновляет `dim_customer`;
- обновляет `dim_date`;
- пересобирает `fact_sales_line` по месячным кускам;
- обновляет `fact_customer_article_stats` либо через `MERGE`, либо через безопасную пересборку по префиксам.

То есть Python здесь выступает как оркестратор, а SQL-файлы содержат саму трансформационную логику.

## `dim_article`

### Что делает таблица

`silver.dim_article` — это товарная размерность.
Одна строка в ней соответствует одному `article_id`.

### Какие SQL-файлы участвуют

- [`sql/queries/silver/refresh_dim_article_delete.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/refresh_dim_article_delete.sql)
- [`sql/queries/silver/refresh_dim_article_insert.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/refresh_dim_article_insert.sql)

### Почему используется `DELETE` + `INSERT`

Для `dim_article` выбран не `MERGE`, а двухшаговая схема:

1. удалить из `silver.dim_article` все строки по тем `article_id`, которые затронуты текущим batch;
2. заново вставить актуальную версию этих же `article_id`.

Такой подход проще контролировать, чем сложный точечный `UPDATE`.
Он хорошо подходит для размерности, где логика выбора актуальной записи выражается через полный пересчёт затронутых ключей.

### Что делает `refresh_dim_article_delete.sql`

Запрос удаляет из `silver.dim_article` все строки, у которых `article_id` встречается в текущем batch `bronze.hm_articles`.
Тем самым перед вставкой таблица очищается от старых версий тех же товаров.

### Что делает `refresh_dim_article_insert.sql`

Логика вставки устроена так:

- сначала выбираются только те `article_id`, которые есть в текущем batch;
- затем по каждому `article_id` строится ранжирование через `row_number() over (...)`;
- сортировка идёт по `ingest_ts DESC`, `batch_id DESC`, `source_file_name DESC`;
- после этого берётся только строка с `rn = 1`, то есть самая свежая запись;
- вычисляются флаги `is_ladieswear`, `is_menswear`, `is_kids`;
- поле `color_family` в текущей реализации заполняется как `NULL`.

### Что важно понимать по логике выбора строки

Таблица `bronze.hm_articles` может содержать несколько версий одной товарной записи.
Поэтому `dim_article` не просто копирует данные, а выбирает актуальную строку по комбинации технических признаков загрузки.

Это важно для защиты: в проекте актуальность измерения определяется не внешним ключом и не отдельным SCD-механизмом, а конкретной SQL-логикой ранжирования.

## `dim_customer`

### Что делает таблица

`silver.dim_customer` — это клиентская размерность.
Одна строка в ней соответствует одному `customer_id`.

### Какие SQL-файлы участвуют

- [`sql/queries/silver/refresh_dim_customer_delete.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/refresh_dim_customer_delete.sql)
- [`sql/queries/silver/refresh_dim_customer_insert.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/refresh_dim_customer_insert.sql)

### Почему используется тот же подход

Для `dim_customer` используется тот же паттерн, что и для `dim_article`:

- сначала удалить все затронутые `customer_id`;
- потом вставить заново актуальные строки.

Это делает логику одинаковой и предсказуемой для двух основных размерностей.

### Что делает `refresh_dim_customer_insert.sql`

Запрос:

- выбирает только клиентов из текущего batch;
- ранжирует записи по `customer_id`;
- берёт самую свежую строку по `ingest_ts`, `batch_id`, `source_file_name`;
- вычисляет `age_band`;
- приводит `active` к булевому признаку `is_active_customer`;
- отмечает наличие флага `fn` через `is_fn_flag_present`.

### Что здесь особенно важно

`age_band` строится не из внешнего справочника, а напрямую в SQL по фиксированным возрастным диапазонам.
Это значит, что сама бизнес-логика сегментации клиента живёт в запросе, а не в Python.

## `dim_date`

### Что делает таблица

`silver.dim_date` — это календарная размерность.
Одна строка соответствует одной дате.

### Какие SQL-файлы участвуют

- [`sql/queries/silver/upsert_dim_date.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/upsert_dim_date.sql)

### Почему здесь не `DELETE` + `INSERT`

Календарная таблица строится особым способом:

- берутся минимальная и максимальная дата в `bronze.hm_transactions` для текущего batch;
- строится последовательность дат через `sequence(min_date, max_date)`;
- из неё вычисляются календарные атрибуты;
- затем вставляются только те даты, которых ещё нет в `silver.dim_date`.

Это именно `upsert`-поведение в смысле пополнения календаря, а не полная пересборка.

### Что делает запрос технически

В запросе используются:

- `min(t_dat)` и `max(t_dat)` для определения диапазона;
- `UNNEST(sequence(...))` для генерации дат;
- `year`, `month`, `day`, `day_of_week`, `week` для вычисления календарных полей;
- `NOT EXISTS`, чтобы не вставлять уже существующие строки.

### Важная особенность

Источник календаря — не внешний справочник, а реальные даты транзакций.
Поэтому таблица растёт только в пределах фактического диапазона данных.

## `fact_sales_line`

### Что делает таблица

`silver.fact_sales_line` — это основной факт продаж.
Одна строка соответствует одной строке покупки из транзакционного источника.

### Какие SQL-файлы участвуют

- [`sql/queries/silver/refresh_fact_sales_line_delete.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/refresh_fact_sales_line_delete.sql)
- [`sql/queries/silver/refresh_fact_sales_line_insert.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/refresh_fact_sales_line_insert.sql)

### Как устроено обновление

Эта таблица пересобирается не целиком, а по месячным частям.
Такой подход нужен, чтобы не пересчитывать весь факт продаж при обработке одного batch.

### Как это связано с `load_silver.py`

В Python-части сначала вызывается `get_batch_months(...)`, который извлекает все месяцы, попавшие в текущий batch.
Потом `resolve_months_to_process(...)` либо берёт все месяцы batch, либо фильтрует их по параметру `--months`.

Для каждого месяца вызывается:

- удаление строк из `silver.fact_sales_line` за этот месяц и batch;
- повторная вставка строк из `bronze.hm_transactions` за тот же месяц и batch.

### Что делает `refresh_fact_sales_line_delete.sql`

Запрос удаляет строки по:

- `batch_id`;
- условию на месяц `sale_date`, переданному как плейсхолдер `__SALE_MONTH_PREDICATE__`.

### Что делает `refresh_fact_sales_line_insert.sql`

Запрос вставляет строки из `bronze.hm_transactions` в `silver.fact_sales_line`, переименовывая `t_dat` в `sale_date` и сохраняя технические поля `ingest_ts`, `source_file_name`, `batch_id`.
Фильтр по месяцу передаётся как плейсхолдер `__BRONZE_MONTH_PREDICATE__`.

### Почему это удобно

Такой вариант даёт баланс между простотой и контролем:

- таблица остаётся фактовой и предсказуемой;
- обновляются только затронутые части;
- SQL остаётся прозрачным для ревью.

## `fact_customer_article_stats`

### Что делает таблица

`silver.fact_customer_article_stats` — это производный агрегат по паре `customer_id + article_id`.
Он нужен для анализа повторных покупок и последующей витрины `repeat_purchase_category`.

### Какие SQL-файлы участвуют

- [`sql/queries/silver/merge_fact_customer_article_stats_batch_delta.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/merge_fact_customer_article_stats_batch_delta.sql)
- [`sql/queries/silver/delete_impacted_stats_prefix.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/delete_impacted_stats_prefix.sql)
- [`sql/queries/silver/insert_impacted_stats_prefix.sql`](/root/repos/db-bi-agent-bisup/sql/queries/silver/insert_impacted_stats_prefix.sql)

### Два режима обновления

Для этой таблицы реализованы два разных сценария.

#### Новый batch

Если `batch_id` ещё не встречался в `silver.fact_sales_line`, используется `MERGE`.
Агрегат по новому batch накладывается на целевую таблицу:

- если пара `customer_id + article_id` уже есть, значения обновляются;
- если пары ещё нет, она вставляется.

#### Повторный batch

Если batch уже существует в `silver.fact_sales_line`, используется безопасная частичная пересборка по префиксам `customer_id`.
Это защищает от повторной обработки и позволяет пересчитать только затронутый диапазон ключей.

### Что делает `merge_fact_customer_article_stats_batch_delta.sql`

Запрос:

- агрегирует строки `silver.fact_sales_line` за текущий batch;
- считает `first_purchase_date`, `last_purchase_date`, `purchase_cnt`, `total_revenue`;
- при совпадении ключей обновляет значения через `least`, `greatest` и арифметическое суммирование;
- при отсутствии пары вставляет новую строку;
- `avg_price` пересчитывается как средняя выручка на строку покупки.

### Что делают `delete_impacted_stats_prefix.sql` и `insert_impacted_stats_prefix.sql`

Эти запросы нужны для безопасной пересборки уже существующего batch.
Логика такая:

- Python вычисляет префиксы `customer_id`, которые затронуты batch;
- затем по каждому префиксу удаляются все соответствующие пары `customer_id + article_id`;
- после этого эти же пары пересчитываются из `silver.fact_sales_line`.

Это не самый короткий путь, но он явно показывает управляемую и безопасную стратегию повторного прогона.

### Почему здесь используется префикс

Префикс нужен как технический способ ограничить область пересчёта.
Он не описывает бизнес-логику клиента, а служит механизмом частичной пересборки при повторных загрузках.

## Какие конструкции Trino здесь реально используются

В слое `silver` встречаются не абстрактные “продвинутые SQL-возможности”, а конкретные конструкции Trino:

- `DELETE FROM ... WHERE ...`
- `INSERT INTO ... SELECT ...`
- `MERGE INTO ... USING ... WHEN MATCHED ... WHEN NOT MATCHED ...`
- `WITH` и многошаговые CTE
- `row_number() over (...)`
- `date_trunc`, `date_add`, `date_diff`
- `min`, `max`, `sum`, `avg`, `count`, `count(distinct ...)`
- `least`, `greatest`
- `sequence` и `UNNEST`
- `regexp_like`
- `coalesce`
- `substr`
- `NOT EXISTS`

Это хороший набор для защиты, потому что можно объяснить не только результат, но и технологический инструмент, которым он достигается.

## Связь SQL и Python

Главное, что нужно понять про слой `silver`: SQL и Python здесь не дублируют друг друга.

`load_silver.py` делает:

- поиск batch;
- выбор месяцев;
- контроль безопасной пересборки;
- вызов SQL-скриптов;
- логирование результатов.

SQL-скрипты делают:

- фильтрацию;
- агрегацию;
- дедупликацию;
- расчёт производных признаков;
- обновление таблиц.

Именно такое разделение делает реализацию читаемой.
Если бы вся логика была в одном Python-файле, её было бы сложнее ревьюить и объяснять.

## Что важно помнить при защите

- `dim_article` и `dim_customer` обновляются через `DELETE` + `INSERT`, а не через `MERGE`.
- `dim_date` пополняется только новыми датами и не пересобирается целиком.
- `fact_sales_line` обновляется по месяцам, а не целиком.
- `fact_customer_article_stats` имеет два сценария обновления: `MERGE` для нового batch и безопасную пересборку по префиксам для повторного batch.
- Вся логика выбора актуальных строк и производных признаков выражена прямо в SQL.

