"""Map each feed's columns onto one schema, split clean rows from quarantine, audit drift."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable

from .config import Config, Source

# Reasons a row lands in quarantine, checked in order.
QUARANTINE_RULES: list[tuple[str, re.Pattern]] = [
    ("NESTED_STRUCTURE", re.compile(r'"(header|line_items?|details)"\s*:', re.I)),
    ("FIELD_RENAMED", re.compile(r'"(available_quantity|qty|stock)"\s*:', re.I)),
    ("TYPE_MISMATCH", re.compile(r'"(quantity|qty_on_hand|stock_level|units_available)"\s*:\s*"', re.I)),
]


def classify_rescued(rescued: str | None) -> str | None:
    """Why a row was rescued. None means the row was clean."""
    if rescued is None or rescued == "":
        return None
    for reason, pattern in QUARANTINE_RULES:
        if pattern.search(rescued):
            return reason
    try:
        json.loads(rescued)
        return "UNEXPECTED_FIELDS"
    except (ValueError, TypeError):
        return "MALFORMED_RECORD"               # PERMISSIVE parsing puts the raw broken line here


def new_columns(current: Iterable[str], expected: Iterable[str]) -> list[str]:
    """Columns a feed started sending that the baseline doesn't know (schema drift report)."""
    ignore = {"_rescued_data", "_source_file", "_ingested_at"}
    return sorted(set(current) - set(expected) - ignore)


def standardize(df, src: Source, cfg: Config):
    """Rename to the unified schema; keep lineage and rescued data."""
    from pyspark.sql import functions as F
    present = set(df.columns)
    cols = [(F.col(f"`{src.columns[u]}`") if src.columns[u] in present else F.lit(None)).cast("string").alias(u)
            for u in cfg.unified_columns]
    rescued = F.col("_rescued_data") if "_rescued_data" in present else F.lit(None).cast("string")
    return df.select(*cols, F.lit(src.name).alias("source"), F.col("_source_file"),
                     rescued.alias("_rescued_data"))


REQUIRED = ("item_id", "quantity")


def split_quarantine(df, required: tuple[str, ...] = REQUIRED):
    """(clean, quarantine) DataFrames; quarantine rows carry a reason.

    A row is quarantined when it was rescued, or when a required field is
    missing (for example a feed that changed layout and no longer maps).
    """
    from pyspark.sql import functions as F
    reason = F.udf(classify_rescued, "string")
    missing = " OR ".join(f"{c} IS NULL" for c in required)
    tagged = df.withColumn(
        "quarantine_reason",
        F.coalesce(reason("_rescued_data"), F.when(F.expr(missing), F.lit("MISSING_REQUIRED_FIELDS"))))
    clean = tagged.where("quarantine_reason IS NULL").drop("quarantine_reason", "_rescued_data")
    quarantine = tagged.where("quarantine_reason IS NOT NULL")
    return clean, quarantine
