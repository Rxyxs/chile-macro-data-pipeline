"""Extract: every source through datoschile into ``warehouse/raw/<source>.parquet``.

Three things the daily run has to survive, all seen on the real sources:

- **A source that does not answer.** mindicador.cl can take 40 s per request or time out.
  A failed source keeps its last good file, the failure is logged, and the run goes on;
  the dashboard shows that source as stale instead of the whole pipeline going dark.
  Before any series is requested each host gets one probe; if it does not answer, its
  series are marked failed at once instead of spending an hour in per-year retries.
- **Re-downloading history every day.** Closed years and months are cached on disk by
  datoschile (``DATOSCHILE_CACHE``); CI keeps that folder between runs, so a normal day
  downloads only the open period.
- **Sources that rewrite the past.** Each new extract is compared with the previous one
  on its key; every value that changed is appended to ``_revisions.parquet`` with both
  numbers, so a silently revised IMACEC or CMF figure leaves a trace.
"""
from __future__ import annotations

import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import datoschile as dc
import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "warehouse" / "raw"
START_YEAR = 2010
FETCH_TRIES = 3


@dataclass(frozen=True)
class Source:
    name: str
    fetch: Callable[[], pd.DataFrame]
    key: tuple[str, ...]
    value: str
    hosts: tuple[str, ...]
    fallback: Callable[[], pd.DataFrame] | None = None
    # Official keyless source tried before the BDE fallback (which needs credentials).
    official: Callable[[], pd.DataFrame] | None = None


HOSTS = {
    "mindicador": "https://mindicador.cl/api/uf/2010",
    "spensiones": "https://www.spensiones.cl/apps/valoresCuotaFondo/vcfAFPxls.php?aaaaini=2025&aaaafin=2025&tf=A&fecconf=20251231",
    "cmf": dc.cmf.URL_INDICE,
    "sii": "https://www.sii.cl/valores_y_fechas/uf/uf2024.htm",
}


def probe(url: str, timeout: int = 60, tries: int = 3) -> str | None:
    """None if the host answers within ``tries`` attempts, else the last reason it did not.
    mindicador.cl fails intermittently (a dropped TLS handshake, then a 40 s answer), so
    one failed request is not enough to call it down."""
    reason = None
    for i in range(tries):
        try:
            requests.get(url, timeout=timeout,
                         headers={"User-Agent": "chile-macro-data-pipeline"}).raise_for_status()
            return None
        except requests.RequestException as e:
            reason = f"{type(e).__name__}: {e}"[:300]
            time.sleep(5 * (i + 1))
    return reason


def fetch_with_retries(source: "Source", tries: int | None = None) -> pd.DataFrame:
    """Each closed year a failed attempt managed to download is already cached, so a retry
    resumes where the last one stopped instead of starting over."""
    tries = tries or FETCH_TRIES
    for i in range(tries):
        try:
            return source.fetch()
        except ConnectionError:
            if i == tries - 1:
                raise
            time.sleep(20 * (i + 1))


def _indicator(code: str) -> Callable[[], pd.DataFrame]:
    def fetch() -> pd.DataFrame:
        df = dc.indicadores.serie(code, desde=START_YEAR)
        return df.rename(columns={df.columns[-1]: "valor"})[["fecha", "valor"]]
    return fetch


def _uf() -> pd.DataFrame:
    df = dc.indicadores.uf(desde=START_YEAR)
    return df.rename(columns={df.columns[-1]: "valor"})[["fecha", "valor"]]


def _pension_index() -> pd.DataFrame:
    """Real (UF) index of each fund A-E, all AFPs weighted by assets."""
    return dc.pensiones.indice(fondos="ABCDE", desde=START_YEAR, real=True)


def _delinquency() -> pd.DataFrame:
    """Banking system only, one row per month; the CMF files start in 2016."""
    df = dc.cmf.morosidad(desde=f"{START_YEAR}-01", solo_sistema=True)
    return df[["fecha", *dc.cmf.CARTERAS, "monto_moroso_mm"]].reset_index(drop=True)


def _sii(code: str) -> Callable[[], pd.DataFrame]:
    """UF or observed dollar from the SII's official yearly pages (datoschile.sii), the
    same values as mindicador day by day; used when mindicador does not answer."""
    def fetch() -> pd.DataFrame:
        rows = []
        for year in range(START_YEAR, pd.Timestamp.today().year + 1):
            rows.extend(dc.sii.descargar_anio(code, year))
        df = pd.DataFrame(rows)
        df["fecha"] = pd.to_datetime(df["fecha"])
        return df.drop_duplicates("fecha").sort_values("fecha").reset_index(drop=True)[["fecha", "valor"]]
    return fetch


def _bde(name: str) -> Callable[[], pd.DataFrame]:
    def fetch() -> pd.DataFrame:
        from pipeline import bcentral
        return bcentral.serie(name, START_YEAR)
    return fetch


SOURCES = [
    Source("uf", _uf, ("fecha",), "valor", ("mindicador",), _bde("uf"), _sii("uf")),
    Source("dolar", _indicator("dolar"), ("fecha",), "valor", ("mindicador",), _bde("dolar"), _sii("dolar")),
    Source("cobre", _indicator("libra_cobre"), ("fecha",), "valor", ("mindicador",), _bde("cobre")),
    Source("ipc", _indicator("ipc"), ("fecha",), "valor", ("mindicador",), _bde("ipc")),
    Source("tpm", _indicator("tpm"), ("fecha",), "valor", ("mindicador",), _bde("tpm")),
    Source("imacec", _indicator("imacec"), ("fecha",), "valor", ("mindicador",), _bde("imacec")),
    Source("desempleo", _indicator("tasa_desempleo"), ("fecha",), "valor", ("mindicador",), _bde("desempleo")),
    # The real index needs the UF, which datoschile takes from the SII if mindicador is down.
    Source("fondos", _pension_index, ("fecha", "fondo"), "indice", ("spensiones",)),
    Source("morosidad", _delinquency, ("fecha",), "total", ("cmf",)),
]


def revisions(old: pd.DataFrame, new: pd.DataFrame, source: Source) -> pd.DataFrame:
    """Rows present in both extracts whose value changed."""
    key = list(source.key)
    both = old[key + [source.value]].merge(new[key + [source.value]], on=key,
                                           suffixes=("_antes", "_ahora"))
    a, b = both[f"{source.value}_antes"], both[f"{source.value}_ahora"]
    changed = both[~((a == b) | (a.isna() & b.isna()) | ((a - b).abs() <= 1e-9 * b.abs()))]
    out = changed.rename(columns={f"{source.value}_antes": "antes", f"{source.value}_ahora": "ahora"})
    # With no changed rows and a multi-column key, agg(axis=1) returns an empty DataFrame
    # instead of a Series, and assigning it to one column raises.
    out["clave"] = (out[key].astype(str).agg("|".join, axis=1) if len(out)
                    else pd.Series(dtype=str, index=out.index))
    return out[["clave", "antes", "ahora"]].assign(fuente=source.name)


def _append(path: Path, rows: pd.DataFrame) -> None:
    if rows.empty:
        return
    if path.exists():
        rows = pd.concat([pd.read_parquet(path), rows], ignore_index=True)
    rows.to_parquet(path, index=False)


def run(sources=SOURCES, raw: Path = RAW) -> pd.DataFrame:
    raw.mkdir(parents=True, exist_ok=True)
    loaded_at = datetime.now(timezone.utc).replace(microsecond=0)
    rev_path = raw / "_revisions.parquet"
    if not rev_path.exists():  # dbt reads it as a source, so it must exist from day one
        pd.DataFrame({"clave": pd.Series(dtype=str), "antes": pd.Series(dtype=float),
                      "ahora": pd.Series(dtype=float), "fuente": pd.Series(dtype=str),
                      "detectado_en": pd.Series(dtype="datetime64[us, UTC]")}).to_parquet(rev_path, index=False)
    log = []
    needed = {h for s in sources for h in s.hosts} | ({"sii"} if any(s.official for s in sources) else set())
    down = {h: probe(HOSTS[h]) for h in sorted(needed) if h in HOSTS}
    if down.get("mindicador"):
        # datoschile then takes the UF it needs (e.g. for the real pension index) from the
        # SII straight away instead of waiting out mindicador's timeout year after year.
        dc.indicadores._mindicador_caido = True
    for s in sources:
        path = raw / f"{s.name}.parquet"
        entry = {"fuente": s.name, "cargado_en": loaded_at}
        try:
            dead = [f"{h}: {down[h]}" for h in s.hosts if down.get(h)]
            entry["origen"] = "principal"
            try:
                if dead:
                    raise ConnectionError("host no responde, " + "; ".join(dead))
                new = fetch_with_retries(s)
            except ConnectionError:
                from pipeline import bcentral
                if s.official is not None and not down.get("sii"):
                    new = s.official()
                    entry["origen"] = "sii"
                elif s.fallback is None or bcentral.credentials() is None:
                    raise
                else:
                    new = s.fallback()
                    entry["origen"] = "respaldo"
            missing = set(s.key) | {s.value}
            missing -= set(new.columns)
            if missing:
                raise ValueError(f"columnas faltantes: {sorted(missing)}")
            if new.duplicated(list(s.key)).any():
                raise ValueError("claves duplicadas en la extraccion")
            new = new.assign(_cargado_en=loaded_at)
            # datoschile leaves reports with date objects in df.attrs (uf()'s cleaning report,
            # serie()'s list of duplicated days); pyarrow writes attrs to the parquet metadata as
            # JSON and fails on them. The reports are not stored.
            new.attrs = {}
            n_rev = 0
            if path.exists():
                rev = revisions(pd.read_parquet(path), new, s).assign(detectado_en=loaded_at)
                _append(raw / "_revisions.parquet", rev)
                n_rev = len(rev)
            new.to_parquet(path, index=False)
            entry.update(estado="ok", filas=len(new), ultima_fecha=pd.Timestamp(new["fecha"].max()),
                         revisiones=n_rev, error=None)
        except Exception as e:  # noqa: BLE001 - a failing source must not stop the others
            entry.update(estado="fallo", filas=None, ultima_fecha=None, revisiones=None,
                         error=f"{type(e).__name__}: {e}"[:500])
            traceback.print_exc(file=sys.stderr)
        log.append(entry)
        print(entry, flush=True)
    frame = pd.DataFrame(log)
    _append(raw / "_loads.parquet", frame)
    return frame


if __name__ == "__main__":
    result = run()
    # Fail the job only if no source loaded at all; a partial run still publishes.
    sys.exit(0 if (result["estado"] == "ok").any() else 1)
