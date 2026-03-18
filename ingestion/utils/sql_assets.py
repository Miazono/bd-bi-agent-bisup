from __future__ import annotations

from pathlib import Path

from ingestion.utils.sql_templates import render_sql_template


class SqlAssets:
    def __init__(self, repo_root: Path | None = None) -> None:
        self.repo_root = repo_root or Path(__file__).resolve().parents[2]

    def path(self, *parts: str) -> Path:
        return self.repo_root.joinpath(*parts)

    def render(self, *parts: str, replacements: dict[str, str] | None = None) -> str:
        return render_sql_template(self.path(*parts), replacements or {})
