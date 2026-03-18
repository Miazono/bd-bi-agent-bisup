from __future__ import annotations

import re
from pathlib import Path

TOKEN_PATTERN = re.compile(r"__[A-Z0-9_]+__")


def render_sql_template(path: Path, replacements: dict[str, str]) -> str:
    if not path.exists():
        raise FileNotFoundError(f"SQL template file not found: {path}")

    sql = path.read_text(encoding="utf-8")

    for key, value in replacements.items():
        sql = sql.replace(f"__{key}__", value)

    unresolved_tokens = sorted(set(TOKEN_PATTERN.findall(sql)))
    if unresolved_tokens:
        raise ValueError(
            f"SQL template {path} has unresolved tokens: {', '.join(unresolved_tokens)}"
        )

    return sql
