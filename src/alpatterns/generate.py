"""Synthetic landing files with the problems real feeds have. Standard library only.

    python -m alpatterns.generate --out landing --days 30

* day 11+: new columns appear (schema drift)
* day 21+: one pharma vendor switches its JSON to a nested layout
* days 26-28: the parts feed ends with a half-written JSON line (corrupt record)
* day 15: one vendor re-sends a corrected copy of its day-14 file (duplicates to MERGE)

All supplier names are fictional.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

START = date(2026, 3, 1)
PARTS_VENDORS = ["V-ACME", "V-BOLTRIX", "V-GEARWORKS"]
GROCERY_SUPPLIERS = ["S-GREENLEAF", "S-DAIRYCO"]
PHARMA_VENDORS = ["acme_pharma", "curewell", "medisupply"]

PARTS_ROWS, GROCERY_ROWS, PHARMA_ROWS = 40, 30, 20


def _parts(day: int, d: date, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(PARTS_ROWS):
        r = {"vendor_id": rng.choice(PARTS_VENDORS), "sku": f"P-{i:04d}", "product_name": f"Part {i}",
             "qty_on_hand": rng.randint(0, 900), "warehouse_code": rng.choice(["WH1", "WH2"]),
             "last_updated": d.isoformat()}
        if day >= 11:
            r.update(lot_number=f"L{rng.randint(1000, 9999)}", expiry_date=(d + timedelta(days=365)).isoformat())
        if day >= 21:
            r.update(country_of_origin=rng.choice(["DE", "JP", "US"]), lead_time_days=rng.randint(3, 30))
        rows.append(r)
    return rows


def _grocery(day: int, d: date, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(GROCERY_ROWS):
        r = {"supplier_code": rng.choice(GROCERY_SUPPLIERS), "item_code": f"G-{i:04d}",
             "item_description": f"Item {i}", "stock_level": rng.randint(0, 300),
             "store_location": rng.choice(["ST01", "ST02", "ST03"]), "extract_timestamp": f"{d}T06:00:00"}
        if day >= 11:
            r["organic_certified"] = rng.choice(["Y", "N"])
        rows.append(r)
    return rows


def _pharma(vendor: str, day: int, d: date, rng: random.Random) -> list[dict]:
    rows = []
    for i in range(PHARMA_ROWS):
        r = {"manufacturer_id": vendor, "product_id": f"RX-{i:03d}", "drug_name": f"Drug {i}",
             "units_available": rng.randint(0, 500), "facility_code": f"F{i % 2 + 1}",
             "report_date": d.isoformat()}
        if day >= 11:
            r["storage_temp_celsius"] = rng.choice([2, 8, 25])
        rows.append(r)
    return rows


def _write_csv(path: Path, rows: list[dict], sep: str = ",") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]), delimiter=sep)
        w.writeheader()
        w.writerows(rows)


def _write_json_lines(path: Path, rows: list[dict], truncate: bool = False) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(r) for r in rows]
    if truncate:                                     # the writer crashed mid-line
        lines.append(json.dumps(rows[0])[:25])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate(out: Path, days: int = 30, seed: int = 5, first_day: int = 1) -> dict[str, int]:
    rng = random.Random(seed + first_day)
    files = 0
    resend = None
    for day in range(first_day, first_day + days):
        d = START + timedelta(days=day - 1)
        _write_json_lines(out / "northwind_parts" / "inventory" / f"inventory_{d:%Y%m%d}.json",
                          _parts(day, d, rng), truncate=26 <= day <= 28)
        _write_csv(out / "freshmart" / f"{d:%Y/%m/%d}" / f"stock_{d:%Y%m%d}.csv", _grocery(day, d, rng), sep="|")
        files += 2
        for vendor in PHARMA_VENDORS:
            rows = _pharma(vendor, day, d, rng)
            folder = out / "medline_health" / vendor
            if vendor == "medisupply":               # sends JSON; switches to a nested layout on day 21
                if day >= 21:
                    rows = [{"header": {"manufacturer_id": vendor, "report_date": r["report_date"]},
                             "line_item": {k: r[k] for k in ("product_id", "drug_name", "units_available")}}
                            for r in rows]
                _write_json_lines(folder / f"report_{d:%Y%m%d}.json", rows)
            else:
                _write_csv(folder / f"report_{d:%Y%m%d}.csv", rows)
                if vendor == "acme_pharma" and day == 14:
                    resend = rows
            files += 1
        if day == 15 and resend:                     # corrected copy of the day-14 file
            prev = START + timedelta(days=13)
            fixed = [dict(r, units_available=r["units_available"] + 1) for r in resend]
            _write_csv(out / "medline_health" / "acme_pharma" / f"report_{prev:%Y%m%d}_v2.csv", fixed)
            files += 1
    return {"files": files}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", type=Path, default=Path("landing"))
    ap.add_argument("--days", type=int, default=30)
    a = ap.parse_args()
    print(generate(a.out, a.days))
