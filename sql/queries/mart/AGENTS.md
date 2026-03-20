# AGENTS.md — sql/queries/mart

## Назначение
В этом каталоге лежит SQL-логика преобразований, используемая для наполнения mart-таблиц.

Marts — основной BI-ориентированный слой для Trino и BI-агента.

## Источник истины
Перед редактированием mart-запросов прочитай:
1. `docs/data/schema.md`
2. `docs/data/marts.md`
3. этот файл

## Соглашения по именованию
- Предпочитай один SQL-файл на один mart.
- Предпочитай единообразные имена вроде:
  - `mart_sales_daily_channel.sql`
  - `mart_sales_monthly_category.sql`
  - `mart_customer_segment_monthly.sql`
  - `mart_repeat_purchase_category.sql`
  - `mart_customer_rfm_monthly.sql`

## Правила проектирования mart
- У каждого mart должен быть явно задокументирован grain.
- Каждый mart должен отвечать на понятный бизнес-вопрос.
- Каждый mart должен строиться на silver-таблицах, а не на bronze.
- Предпочитай стабильные, удобные для BI имена полей и метрик.
- Не смешивай в одном mart несколько несвязанных бизнес-вопросов.

## Запланированные marts
- `mart.sales_daily_channel`
- `mart.sales_monthly_category`
- `mart.customer_segment_monthly`
- `mart.repeat_purchase_category`
- `mart.customer_rfm_monthly`

## Правила для метрик
- Используй названия метрик, понятные для BI и NL2SQL.
- Явно документируй неоднозначные метрики.
- Для этого датасета `items_sold` должен означать количество transaction line, если иное определение явно не задокументировано.

## Обновляй документацию вместе с кодом
Если mart добавляется, удаляется, переименовывается или меняется его grain, также обновляй при необходимости:
- `docs/data/schema.md`
- `docs/data/marts.md`
- `docs/data/lineage.md`, если меняется происхождение данных
