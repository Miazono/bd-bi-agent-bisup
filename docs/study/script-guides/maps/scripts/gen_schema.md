# Краткая схема `gen_schema.py`

## Назначение

Автоматическая генерация `docs/data/catalog_generated.md` по DDL-файлам из `sql/ddl/`.

## Входы

- файлы `sql/ddl/bronze/*.sql`
- файлы `sql/ddl/silver/*.sql`
- файлы `sql/ddl/mart/*.sql`

## Ключевые объекты

- `ROOT_DIR`
- `DDL_DIR`
- `OUTPUT_PATH`
- `SCHEMA_ORDER`
- `Column`
- `TableDefinition`

## Функции

- `_extract_create_parts()` - достаёт схему, имя таблицы и блок столбцов.
- `_parse_columns()` - превращает блок столбцов в список `Column`.
- `_parse_with_properties()` - читает свойства `WITH (...)`.
- `_normalize_property()` - убирает лишние кавычки и пробелы.
- `_normalize_partitioning()` - приводит `ARRAY[...]` к читабельному виду.
- `parse_ddl_file()` - строит `TableDefinition` для одного файла.
- `collect_tables()` - собирает все таблицы в заданном порядке схем.
- `render_catalog()` - формирует текст Markdown-каталога.
- `main()` - записывает результат в выходной файл.

## Порядок выполнения

1. Находит все DDL-файлы.
2. Парсит каждую таблицу.
3. Формирует Markdown.
4. Перезаписывает `docs/data/catalog_generated.md`.

## Выходы

- обновлённый каталог физических таблиц;
- таблицы, сгруппированные по схемам `bronze`, `silver`, `mart`.

## Зависимости

- `sql/ddl/`
- `docs/data/catalog_generated.md`

## Где может сломаться

- DDL-файл не соответствует ожидаемому шаблону;
- отсутствует `CREATE TABLE IF NOT EXISTS ... WITH (...)`;
- неожиданно изменён формат секции `WITH`;
- в DDL появились строки, которые парсер не умеет интерпретировать.
