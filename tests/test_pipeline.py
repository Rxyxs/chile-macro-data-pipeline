"""Pipeline tests without network: fake sources for the extractor, and a full ``dbt build``
plus export over a small hand-made raw layer."""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pipeline import export, extract

ROOT = Path(__file__).resolve().parents[1]


def _src(name, frame_fn, key=("fecha",), value="valor", hosts=("h",)):
    return extract.Source(name, frame_fn, key, value, hosts)


@pytest.fixture(autouse=True)
def hosts_up(monkeypatch):
    monkeypatch.setattr(extract, "HOSTS", {"h": "x", "down": "y"})
    monkeypatch.setattr(extract, "probe", lambda url: None if url == "x" else "Timeout")
    monkeypatch.setattr(extract, "FETCH_TRIES", 1)


def test_failed_source_keeps_last_good_file_and_others_load(tmp_path):
    good = pd.DataFrame({"fecha": pd.to_datetime(["2024-01-01", "2024-01-02"]), "valor": [1.0, 2.0]})
    extract.run([_src("a", lambda: good)], raw=tmp_path)

    def boom():
        raise TimeoutError("sin respuesta")
    log = extract.run([_src("a", boom), _src("b", lambda: good)], raw=tmp_path)
    assert log.set_index("fuente")["estado"].to_dict() == {"a": "fallo", "b": "ok"}
    assert len(pd.read_parquet(tmp_path / "a.parquet")) == 2
    assert len(pd.read_parquet(tmp_path / "_loads.parquet")) == 3


def test_dead_host_skips_fetch(tmp_path):
    called = []
    log = extract.run([_src("a", lambda: called.append(1), hosts=("down",))], raw=tmp_path)
    assert not called
    assert log["error"].iloc[0].startswith("ConnectionError: host no responde")


def test_revisions_record_changed_past_values(tmp_path):
    v1 = pd.DataFrame({"fecha": pd.to_datetime(["2024-01-01", "2024-02-01"]), "valor": [0.5, 0.7]})
    v2 = pd.DataFrame({"fecha": pd.to_datetime(["2024-01-01", "2024-02-01", "2024-03-01"]),
                       "valor": [0.5, 0.8, 0.1]})
    extract.run([_src("imacec", lambda: v1)], raw=tmp_path)
    log = extract.run([_src("imacec", lambda: v2)], raw=tmp_path)
    rev = pd.read_parquet(tmp_path / "_revisions.parquet")
    assert log["revisiones"].iloc[0] == 1
    assert rev[["clave", "antes", "ahora"]].values.tolist() == [["2024-02-01", 0.7, 0.8]]


def test_duplicate_keys_fail_the_source(tmp_path):
    dup = pd.DataFrame({"fecha": pd.to_datetime(["2024-01-01"] * 2), "valor": [1.0, 1.1]})
    log = extract.run([_src("a", lambda: dup)], raw=tmp_path)
    assert log["estado"].iloc[0] == "fallo"


# --- dbt over a fixture raw layer -------------------------------------------------------

def _fixture_raw(raw: Path) -> None:
    days = pd.bdate_range("2023-01-02", "2024-12-31")
    cal = pd.date_range("2023-01-01", "2024-12-31")
    months = pd.date_range("2023-01-01", "2024-12-01", freq="MS")
    uf = pd.DataFrame({"fecha": cal, "valor": 35000 * 1.0001 ** np.arange(len(cal))})
    series = {
        "uf": uf,
        "dolar": pd.DataFrame({"fecha": days, "valor": 900.0}),
        "cobre": pd.DataFrame({"fecha": days, "valor": 4.0}),
        "ipc": pd.DataFrame({"fecha": months, "valor": 0.5}),
        "tpm": pd.DataFrame({"fecha": months, "valor": 5.0}),
        "imacec": pd.DataFrame({"fecha": months, "valor": 2.0}),
        "desempleo": pd.DataFrame({"fecha": months, "valor": 8.5}),
    }
    fondos = pd.concat([pd.DataFrame({"fecha": days, "fondo": f, "indice": 100 + np.arange(len(days)) * 0.01,
                                      "retorno": 0.0001}) for f in "ABCDE"])
    morosidad = pd.DataFrame({"fecha": months, "total": 2.1, "clientes": 2.0, "comercial": 2.5,
                              "personas": 1.9, "consumo": 2.8, "vivienda": 1.3, "monto_moroso_mm": 5e6})
    now = pd.Timestamp.now(tz="UTC").floor("s")
    for name, df in {**series, "fondos": fondos, "morosidad": morosidad}.items():
        df.assign(_cargado_en=now).to_parquet(raw / f"{name}.parquet", index=False)
    pd.DataFrame({"fuente": list(series) + ["fondos", "morosidad"], "cargado_en": now, "estado": "ok",
                  "filas": 10, "ultima_fecha": pd.Timestamp("2024-12-31"), "revisiones": 0,
                  "error": None}).to_parquet(raw / "_loads.parquet", index=False)
    pd.DataFrame({"clave": ["2024-02-01"], "antes": [0.7], "ahora": [0.8], "fuente": ["imacec"],
                  "detectado_en": [now]}).to_parquet(raw / "_revisions.parquet", index=False)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    work = tmp_path_factory.mktemp("wh")
    raw = work / "raw"
    raw.mkdir()
    _fixture_raw(raw)
    env = {**os.environ, "RAW_PATH": raw.as_posix(), "WAREHOUSE_PATH": (work / "wh.duckdb").as_posix()}
    dbt = shutil.which("dbt", path=str(Path(sys.executable).parent)) or "dbt"
    proc = subprocess.run([dbt, "build", "--project-dir", "transform", "--profiles-dir", "transform"],
                          cwd=ROOT, env=env, capture_output=True, text=True)
    assert proc.returncode == 0, proc.stdout[-3000:]
    return work


def test_dbt_build_passes_on_fixture(built):
    assert (built / "wh.duckdb").exists()


def test_marts_compute_expected_values(built):
    import duckdb
    con = duckdb.connect(str(built / "wh.duckdb"), read_only=True)
    cu = con.execute("select cobre_clp_lb from mart_mercados_diario limit 1").fetchone()[0]
    assert cu == pytest.approx(3600.0)
    ipc12 = con.execute("select ipc_12m, tpm_real_ex_post from mart_macro_mensual where mes = '2024-12-01'").fetchone()
    assert ipc12[0] == pytest.approx((1.005 ** 12 - 1) * 100)
    assert ipc12[1] == pytest.approx(5.0 - ipc12[0])
    assert con.execute("select count(*) from mart_macro_mensual where ipc_12m is null").fetchone()[0] == 11
    health = con.execute("select fuente, revisiones_totales from mart_salud_fuentes where fuente='imacec'").fetchone()
    assert health == ("imacec", 1)
    # the fixture data ends in 2024, so every source is stale today
    assert con.execute("select bool_and(desactualizada) from mart_salud_fuentes").fetchone()[0]


def test_export_writes_column_arrays(built, tmp_path):
    meta = export.export(built / "wh.duckdb", tmp_path)
    assert meta["filas"]["fondos"] > 0
    mercados = json.loads((tmp_path / "mercados.json").read_text(encoding="utf-8"))
    assert set(mercados) >= {"fecha", "uf", "dolar", "cobre_uf_lb"}
    assert len(mercados["fecha"]) == len(mercados["uf"])


def test_fallback_used_when_primary_host_is_down(tmp_path, monkeypatch):
    from pipeline import bcentral
    monkeypatch.setattr(bcentral, "credentials", lambda: ("u", "p"))
    frame = pd.DataFrame({"fecha": pd.to_datetime(["2024-01-01"]), "valor": [37000.0]})
    src = extract.Source("uf", lambda: None, ("fecha",), "valor", ("down",), lambda: frame)
    log = extract.run([src], raw=tmp_path)
    assert log[["estado", "origen"]].iloc[0].tolist() == ["ok", "respaldo"]


def test_no_fallback_without_credentials(tmp_path, monkeypatch):
    from pipeline import bcentral
    monkeypatch.setattr(bcentral, "credentials", lambda: None)
    src = extract.Source("uf", lambda: None, ("fecha",), "valor", ("down",), lambda: 1 / 0)
    assert extract.run([src], raw=tmp_path)["estado"].iloc[0] == "fallo"
