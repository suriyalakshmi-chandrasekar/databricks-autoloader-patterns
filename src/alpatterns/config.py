"""Feed definitions loaded from YAML."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Source:
    name: str
    format: str                                  # json | csv | parquet
    path: str                                    # relative to the landing root
    target: str
    columns: dict[str, str]                      # unified column -> source column
    write_mode: str = "append"                   # append | merge
    merge_keys: tuple[str, ...] = ()
    path_glob: str | None = None
    schema_hints: str | None = None
    options: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if self.write_mode not in ("append", "merge"):
            raise ValueError(f"{self.name}: write_mode must be append or merge")
        if self.write_mode == "merge" and not self.merge_keys:
            raise ValueError(f"{self.name}: merge needs merge_keys")


@dataclass(frozen=True)
class Config:
    unified_columns: tuple[str, ...]
    sources: tuple[Source, ...]


def load_config(path: str | Path) -> Config:
    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    unified = tuple(raw["unified_columns"])
    sources = []
    for s in raw["sources"]:
        src = Source(**{**s, "merge_keys": tuple(s.get("merge_keys", ())), "options": s.get("options", {})})
        missing = [c for c in unified if c not in src.columns]
        if missing:
            raise ValueError(f"{src.name}: no mapping for unified columns {missing}")
        sources.append(src)
    names = [s.name for s in sources]
    if len(set(names)) != len(names):
        raise ValueError("source names must be unique")
    return Config(unified, tuple(sources))
