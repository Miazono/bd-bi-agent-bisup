"""
Генерация каталога физических таблиц проекта на основе DDL-файлов.

Это учебная, подробно прокомментированная копия исходного скрипта.
Она повторяет структуру `scripts/gen_schema.py`, но объясняет,
как именно из DDL получается человекочитаемый каталог таблиц.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re


# Корень репозитория нужен, чтобы строить абсолютные пути к DDL и выходному файлу.
ROOT_DIR = Path(__file__).resolve().parent.parent
DDL_DIR = ROOT_DIR / "sql" / "ddl"
OUTPUT_PATH = ROOT_DIR / "docs" / "data" / "catalog_generated.md"

# Порядок схем фиксирован, чтобы каталог всегда генерировался в одном и том же виде.
SCHEMA_ORDER = ("bronze", "silver", "mart")


@dataclass(frozen=True)
class Column:
    """
    Описание одного столбца физической таблицы.

    Здесь хранятся только имя и тип, потому что скрипт читает DDL на уровне
    структуры, а не на уровне семантики полей.
    """

    name: str
    data_type: str


@dataclass(frozen=True)
class TableDefinition:
    """
    Итоговое описание одной таблицы из DDL.

    В объекте собраны:
    - схема и имя таблицы;
    - путь к исходному DDL-файлу;
    - location и формат хранения;
    - партиционирование;
    - список столбцов.
    """

    schema_name: str
    table_name: str
    ddl_path: Path
    location: str
    file_format: str
    partitioning: str
    columns: tuple[Column, ...]

    @property
    def fqn(self) -> str:
        # Fully qualified name помогает печатать таблицу в формате schema.table.
        return f"{self.schema_name}.{self.table_name}"


def _extract_create_parts(sql_text: str) -> tuple[str, str, str]:
    """
    Находит в тексте DDL секцию CREATE TABLE и извлекает:
    - схему;
    - имя таблицы;
    - блок столбцов.
    """
    match = re.search(
        r"CREATE TABLE IF NOT EXISTS\s+([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)\s*\((.*?)\)\s*WITH\s*\(",
        sql_text,
        re.DOTALL,
    )
    if not match:
        raise ValueError("Не удалось разобрать секцию CREATE TABLE")

    return match.group(1), match.group(2), match.group(3)


def _parse_columns(columns_block: str) -> tuple[Column, ...]:
    """
    Превращает многострочный блок столбцов DDL в список объектов Column.

    Здесь скрипт ожидает очень простой формат:
    `column_name data_type,`
    """
    columns: list[Column] = []

    for raw_line in columns_block.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue

        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ValueError(f"Не удалось разобрать колонку: {line!r}")

        column_name, data_type = parts
        columns.append(Column(name=column_name, data_type=data_type))

    return tuple(columns)


def _parse_with_properties(sql_text: str) -> dict[str, str]:
    """
    Извлекает свойства из блока WITH (...).

    В текущем проекте это позволяет понять:
    - формат хранения;
    - location таблицы;
    - партиционирование.
    """
    match = re.search(r"WITH\s*\((.*?)\)\s*;?\s*$", sql_text, re.DOTALL)
    if not match:
        raise ValueError("Не удалось разобрать секцию WITH")

    properties: dict[str, str] = {}
    for raw_line in match.group(1).splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or "=" not in line:
            continue

        key, value = line.split("=", 1)
        properties[key.strip()] = value.strip()

    return properties


def _normalize_property(value: str, *, strip_quotes: bool = True) -> str:
    """
    Убирает лишние кавычки и пробелы у значения свойства.
    """
    normalized = value.strip()
    if strip_quotes and normalized.startswith("'") and normalized.endswith("'"):
        return normalized[1:-1]
    return normalized


def _normalize_partitioning(value: str | None) -> str:
    """
    Приводит ARRAY[...] к более читаемому виду.

    Например:
    - `ARRAY['month(t_dat)']` -> `month(t_dat)`
    - `ARRAY['month(sale_date)']` -> `month(sale_date)`
    """
    if not value:
        return "не задано"

    normalized = value.strip()
    match = re.fullmatch(r"ARRAY\[(.*)\]", normalized)
    if match:
        items = [
            item.strip().strip("'")
            for item in match.group(1).split(",")
            if item.strip()
        ]
        return ", ".join(items) if items else "не задано"

    return normalized


def parse_ddl_file(path: Path) -> TableDefinition:
    """
    Читает один DDL-файл и превращает его в структурированное описание таблицы.
    """
    sql_text = path.read_text(encoding="utf-8")
    schema_name, table_name, columns_block = _extract_create_parts(sql_text)
    properties = _parse_with_properties(sql_text)

    return TableDefinition(
        schema_name=schema_name,
        table_name=table_name,
        ddl_path=path,
        location=_normalize_property(properties.get("location", "")),
        file_format=_normalize_property(properties.get("format", ""), strip_quotes=True),
        partitioning=_normalize_partitioning(properties.get("partitioning")),
        columns=_parse_columns(columns_block),
    )


def collect_tables() -> list[TableDefinition]:
    """
    Собирает описания всех таблиц из `sql/ddl` в фиксированном порядке схем.
    """
    tables: list[TableDefinition] = []

    for schema_name in SCHEMA_ORDER:
        schema_dir = DDL_DIR / schema_name
        for ddl_path in sorted(schema_dir.glob("*.sql")):
            tables.append(parse_ddl_file(ddl_path))

    return tables


def render_catalog() -> str:
    """
    Генерирует Markdown-каталог физических таблиц.

    Этот текст затем записывается в `docs/data/catalog_generated.md`.
    """
    tables = collect_tables()
    lines = [
        "# Каталог физических таблиц",
        "",
        "> Этот файл сгенерирован скриптом `scripts/gen_schema.py` на основе DDL из `sql/ddl/`.",
        "> Он описывает только физические таблицы Iceberg и не включает временные внешние таблицы `hive.raw.*`.",
        "",
        "## Что входит в каталог",
        "",
        "- схемы `bronze`, `silver` и `mart`;",
        "- расположение таблиц в объектном хранилище;",
        "- формат хранения и партиционирование;",
        "- полный список столбцов и типов.",
        "",
    ]

    for schema_name in SCHEMA_ORDER:
        schema_tables = [table for table in tables if table.schema_name == schema_name]
        if not schema_tables:
            continue

        lines.extend(
            [
                f"## Схема `{schema_name}`",
                "",
            ]
        )

        for table in schema_tables:
            relative_ddl_path = table.ddl_path.relative_to(ROOT_DIR).as_posix()
            lines.extend(
                [
                    f"### `{table.fqn}`",
                    "",
                    f"- DDL: `{relative_ddl_path}`",
                    f"- Формат: `{table.file_format}`" if table.file_format else "- Формат: не указан",
                    f"- Расположение: `{table.location}`" if table.location else "- Расположение: не указано",
                    f"- Партиционирование: `{table.partitioning}`",
                    "",
                    "| Столбец | Тип |",
                    "| --- | --- |",
                ]
            )

            for column in table.columns:
                lines.append(f"| `{column.name}` | `{column.data_type}` |")

            lines.append("")

    return "\n".join(lines).strip() + "\n"


def main() -> None:
    # Скрипт просто перезаписывает производный Markdown-файл.
    OUTPUT_PATH.write_text(render_catalog(), encoding="utf-8")


if __name__ == "__main__":
    main()
