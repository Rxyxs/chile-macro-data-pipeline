[ 🇺🇸 English ] | [ 🇨🇱 [Leer en Español](README.es.md) ]

# Chile Macro Data Pipeline

[![pipeline](https://github.com/Rxyxs/chile-macro-data-pipeline/actions/workflows/pipeline.yml/badge.svg)](https://github.com/Rxyxs/chile-macro-data-pipeline/actions/workflows/pipeline.yml)
[![tests](https://github.com/Rxyxs/chile-macro-data-pipeline/actions/workflows/tests.yml/badge.svg)](https://github.com/Rxyxs/chile-macro-data-pipeline/actions/workflows/tests.yml)
![dbt](https://img.shields.io/badge/dbt-duckdb-FF694B?logo=dbt&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-green)

A scheduled pipeline that runs every morning on GitHub Actions, pulls nine public Chilean series (UF, dollar, copper, CPI, policy rate, IMACEC, unemployment, pension funds A to E and bank delinquency), models and tests them with dbt on DuckDB, and publishes a **[live dashboard](https://rxyxs.github.io/chile-macro-data-pipeline/)**. It is built around the way these sources actually fail: one of them was down for most of the first week the pipeline ran.

## Architecture

```
            GitHub Actions, daily 13:30 UTC
┌──────────────────────────────────────────────────────────────────┐
│ pipeline/extract.py   datoschile → warehouse/raw/*.parquet       │
│   probe each host · retry · keep last good file · log revisions  │
│                    │                                             │
│ transform/ (dbt)   staging views → marts → tests                 │
│   a failing test stops the run: bad data is never published      │
│                    │                                             │
│ pipeline/export.py marts → site/data/*.json                      │
│                    │                                             │
│ deploy-pages       site/index.html (Chart.js) reads the JSON     │
└──────────────────────────────────────────────────────────────────┘
 state between runs: actions/cache (download cache + raw layer),
 load and revision logs also published with the site as a backup
```

## Designed for unreliable sources

STATUS_SECTION

| Failure | What the pipeline does |
|---|---|
| A host does not answer | Each host gets a probe with three attempts before any series is requested. If it is down, its series are marked failed in seconds instead of spending an hour in per-year retries, and the other sources still load. |
| A source fails halfway | The source is retried whole, up to three times. Every closed year downloaded on a failed attempt is already cached, so the retry resumes instead of starting over; the cache is saved even when the run fails. |
| A source fails on a normal day | Its last good file is kept, the failure goes into `_loads`, and the dashboard shows that source as failed or stale with the age of its newest value. |
| A source rewrites the past | Each extract is compared with the previous one on its key, and every changed value goes into `_revisions` with both numbers. |
| A value is wrong | dbt tests: the UF never moves 1% in a day, values stay in plausible ranges (catches a unit change), no gaps longer than a week in the trading days, unique keys. If one fails, nothing is deployed and the site keeps yesterday's data. |
| The cache is evicted | The load and revision logs are restored from the published site before extracting. |

A source is **stale** when its newest value is older than its publication rhythm allows: a week for daily series, about two and a half months for the CPI and the IMACEC, three and a half for unemployment and four for the CMF files.

## Models

| Mart | Grain | Content |
|---|---|---|
| `mart_mercados_diario` | trading day | UF, dollar, copper in USD, CLP and UF per pound |
| `mart_macro_mensual` | month | CPI monthly and compounded 12-month, policy rate, ex-post real rate, IMACEC, unemployment |
| `mart_fondos` | day × fund | real (UF) index of funds A–E, 12-month real return, drawdown from the previous peak |
| `mart_morosidad` | month | banking-system 90-day delinquency by portfolio |
| `mart_salud_fuentes` | source | latest run, latest good load, data age, stale flag, revision count |
| `mart_revisiones` | revised value | before, after, when it was detected |

The cleaning of each source (the UF days where mindicador leaks the dollar, the pension files whose header changes 31 times, the CMF's three Excel formats) lives in [datoschile](https://github.com/Rxyxs/datoschile), so this repository only does orchestration, modelling and testing.

## Reproduce

```bash
python -m venv .venv
.venv\Scripts\activate          # Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt

python -m pipeline.extract       # download (closed years are cached in ~/.cache/datoschile)
cd transform && dbt build --profiles-dir . && cd ..
python -m pipeline.export        # site/data/*.json
python -m http.server -d site    # dashboard at http://localhost:8000
pytest                           # no network: fake sources and a dbt build on a fixture
```

## Scope

- The warehouse is DuckDB inside the runner, not a hosted warehouse. The dbt models are plain SQL, so moving them to BigQuery or Snowflake is a profile change plus the `asof join` in `mart_fondos`; that move needs a cloud account and was not done.
- Revisions are only measured from the day the pipeline started running; nothing is known about revisions before that.
- The series are the ones mindicador.cl, the Pension Superintendency and the CMF publish; there is no second source to cross-check against.
