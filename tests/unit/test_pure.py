import json
from pathlib import Path

import pytest

from alpatterns.config import Source, load_config
from alpatterns.generate import generate
from alpatterns.standardize import classify_rescued, new_columns

REPO = Path(__file__).resolve().parents[2]


def test_config_loads_and_maps_every_unified_column():
    cfg = load_config(REPO / "config" / "sources.yaml")
    assert len(cfg.sources) == 4
    assert all(set(cfg.unified_columns) <= set(s.columns) for s in cfg.sources)
    assert next(s for s in cfg.sources if s.write_mode == "merge").merge_keys


def test_merge_without_keys_is_rejected():
    with pytest.raises(ValueError, match="merge_keys"):
        Source(name="x", format="csv", path="p", target="t", columns={}, write_mode="merge")


def test_missing_mapping_is_rejected(tmp_path):
    f = tmp_path / "s.yaml"
    f.write_text("unified_columns: [a, b]\nsources:\n  - {name: s, format: csv, path: p, target: t, columns: {a: x}}\n")
    with pytest.raises(ValueError, match="b"):
        load_config(f)


@pytest.mark.parametrize("rescued, reason", [
    (None, None),
    ("", None),
    ('{"header": {"id": 1}}', "NESTED_STRUCTURE"),
    ('{"available_quantity": 5}', "FIELD_RENAMED"),
    ('{"qty_on_hand": "lots"}', "TYPE_MISMATCH"),
    ('{"color": "red"}', "UNEXPECTED_FIELDS"),
    ('{"vendor_id": "V-AC', "MALFORMED_RECORD"),
])
def test_classify_rescued(rescued, reason):
    assert classify_rescued(rescued) == reason


def test_new_columns_report():
    assert new_columns(["sku", "qty", "lot_number", "_rescued_data"], ["sku", "qty"]) == ["lot_number"]


def test_generator_builds_the_drift_scenarios(tmp_path):
    generate(tmp_path, days=30)
    parts = sorted((tmp_path / "northwind_parts" / "inventory").glob("*.json"))
    assert len(parts) == 30
    day1 = json.loads(parts[0].read_text().splitlines()[0])
    day21 = json.loads(parts[20].read_text().splitlines()[0])
    assert "lot_number" not in day1 and {"lot_number", "country_of_origin"} <= set(day21)
    with pytest.raises(json.JSONDecodeError):          # day 26 ends with a half-written line
        json.loads(parts[25].read_text().splitlines()[-1])
    assert len(list((tmp_path / "freshmart").rglob("*.csv"))) == 30
    nested = (tmp_path / "medline_health" / "medisupply" / "report_20260321.json").read_text().splitlines()[0]
    assert "header" in json.loads(nested)
    resend = tmp_path / "medline_health" / "acme_pharma" / "report_20260314_v2.csv"
    original = tmp_path / "medline_health" / "acme_pharma" / "report_20260314.csv"
    assert resend.exists() and original.read_text() != resend.read_text()
