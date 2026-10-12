"""Second source for the mindicador series: the Central Bank of Chile's BDE web service.

mindicador.cl republishes Central Bank and INE series; the BDE API is where they come
from. It needs a free user (https://si3.bcentral.cl/estadisticas/Principal1/Web_Services/),
read from the ``BCCH_USER`` and ``BCCH_PASS`` environment variables (repository secrets in
CI, never in the code). Without them this module is inactive and the pipeline uses
mindicador alone.

The series codes below are the BDE codes for the same measures. They have not been checked
against the live service yet, so ``verify_codes`` asks the API to describe each one before
any data is used and fails if a code is unknown or its title does not match.
"""
from __future__ import annotations

import os
from datetime import date

import pandas as pd
import requests

URL = "https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx"

# source name -> (BDE series code, word the series title must contain)
SERIES = {
    "uf": ("F073.UFF.PRE.Z.D", "fomento"),
    "dolar": ("F073.TCO.PRE.Z.D", "observado"),
    "cobre": ("F019.PPB.PRE.100.D", "cobre"),
    "ipc": ("F074.IPC.VAR.Z.Z.C.M", "ipc"),
    "tpm": ("F022.TPM.TIN.D001.NO.Z.D", "política"),
    "imacec": ("F032.IMC.V12.Z.Z.2018.Z.Z.0.M", "imacec"),
    "desempleo": ("F049.DES.TAS.INE9.10.M", "desocupación"),
}


def credentials() -> tuple[str, str] | None:
    user, pw = os.environ.get("BCCH_USER"), os.environ.get("BCCH_PASS")
    return (user, pw) if user and pw else None


def _call(params: dict, timeout: int = 60) -> dict:
    user, pw = credentials() or (None, None)
    if not user:
        raise RuntimeError("BCCH_USER / BCCH_PASS not set")
    resp = requests.get(URL, params={"user": user, "pass": pw, **params}, timeout=timeout)
    resp.raise_for_status()
    body = resp.json()
    if body.get("Codigo") not in (0, None):
        raise ValueError(f"BDE error {body.get('Codigo')}: {body.get('Descripcion')}")
    return body


def verify_codes() -> dict[str, str]:
    """Title of each configured series, failing on an unknown code or a mismatched title."""
    titles = {}
    for name, (code, word) in SERIES.items():
        info = _call({"function": "GetSeries", "timeseries": code,
                      "firstdate": "2024-01-01", "lastdate": "2024-01-31"})["Series"]
        title = f"{info.get('descripEsp', '')} {info.get('descripIng', '')}".lower()
        if word not in title:
            raise ValueError(f"{name}: code {code} is '{title.strip()}', expected '{word}'")
        titles[name] = title.strip()
    return titles


def serie(name: str, desde: int) -> pd.DataFrame:
    """``fecha``/``valor`` frame, the same shape the extractor gets from datoschile."""
    code, _ = SERIES[name]
    obs = _call({"function": "GetSeries", "timeseries": code, "firstdate": f"{desde}-01-01",
                 "lastdate": date.today().isoformat()})["Series"]["Obs"]
    df = pd.DataFrame(obs)
    df = df[df["statusCode"] == "OK"]
    return pd.DataFrame({"fecha": pd.to_datetime(df["indexDateString"], format="%d-%m-%Y"),
                         "valor": pd.to_numeric(df["value"], errors="coerce")}).dropna()
