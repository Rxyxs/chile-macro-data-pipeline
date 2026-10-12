"""Export: the marts from the DuckDB warehouse to the JSON files the dashboard reads.

Each mart becomes ``site/data/<mart>.json`` as column arrays (smaller than records and
what the charts consume directly). Daily series are cut to ``DAILY_FROM`` to keep the page
light; the warehouse keeps the full history.
"""
from __future__ import annotations

import json
import math
import os
from datetime import date, datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE = Path(os.environ.get("WAREHOUSE_PATH", ROOT / "warehouse" / "warehouse.duckdb"))
OUT = ROOT / "site" / "data"
DAILY_FROM = "2015-01-01"

QUERIES = {
    "mercados": f"select * from mart_mercados_diario where fecha >= '{DAILY_FROM}' order by fecha",
    "macro": "select * from mart_macro_mensual order by mes",
    "fondos": f"select fecha, fondo, indice_real, retorno_real_12m, caida_desde_maximo "
              f"from mart_fondos where fecha >= '{DAILY_FROM}' order by fondo, fecha",
    "morosidad": "select * from mart_morosidad order by mes",
    "salud": "select * from mart_salud_fuentes order by fuente",
    "revisiones": "select * from mart_revisiones limit 500",
}


def _clean(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    if isinstance(v, float):
        return round(v, 6)
    return v


def export(warehouse: Path = WAREHOUSE, out: Path = OUT) -> dict:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse), read_only=True)
    sizes = {}
    for name, sql in QUERIES.items():
        cur = con.execute(sql)
        cols = [c[0] for c in cur.description]
        rows = cur.fetchall()
        payload = {c: [_clean(r[i]) for r in rows] for i, c in enumerate(cols)}
        path = out / f"{name}.json"
        path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        sizes[name] = len(rows)
    meta = {"generado_en": datetime.utcnow().replace(microsecond=0).isoformat() + "Z", "filas": sizes}
    (out / "meta.json").write_text(json.dumps(meta), encoding="utf-8")
    con.close()
    return meta


if __name__ == "__main__":
    print(export())
