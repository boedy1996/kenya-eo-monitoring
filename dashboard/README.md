# Kenya crop monitoring dashboard

Local development application for exploring Kenya crop-model predictions and
the supporting Sentinel-1 and Sentinel-2 weekly observations.

## Components

- `api/`: FastAPI API and PostGIS vector-tile service.
- `web/`: React, TypeScript, Vite and MapLibre client.
- `db/`: idempotent SQL migrations.
- `scripts/`: development-bundle export, load and validation utilities.
- `data/dev/`: local data bundles; ignored by Git.

## Local prerequisites

- Python 3.12 through the `kenya-eo-monitoring` pyenv environment.
- Node 22, selected through the repository `.nvmrc`.
- PostgreSQL 16 on `127.0.0.1:5432`.
- Database `kenya_dashboard_dev` with the PostGIS extension enabled.

## Environment

The API accepts either a libpq URL or a SQLAlchemy-style psycopg URL:

```bash
export DATABASE_URL="postgresql+psycopg://boedy1996@127.0.0.1:5432/kenya_dashboard_dev"
```

Copy `api/.env.example` to `api/.env` if a persistent local configuration is
preferred. The `.env` file is ignored by Git.

## Development workflow

```bash
python dashboard/scripts/migrate.py
python dashboard/scripts/load_dev_bundle.py \
  --bundle-dir dashboard/data/dev/kenya-dashboard-dev-v1

uvicorn dashboard.api.app.main:app --reload --host 127.0.0.1 --port 8000
```

In a second terminal:

```bash
cd dashboard/web
nvm use
npm run dev
```

Open `http://127.0.0.1:3000`. Vite proxies `/api` and `/tiles` to FastAPI on
port `8000`.

### Basemaps

The basemap selector uses browser-loaded public tile services and does not
require credentials. Available layers are OpenStreetMap, OpenTopoMap, Esri
World Imagery, Esri World Street Map, Esri World Topographic and a neutral
background. Provider attribution remains visible in the map control.

Alternatively, start both services with:

```bash
./dashboard/run_local.sh
```

## Data contract

The standard local bundle contains 2,000 fields, 543 weekly timestamps and
1,086,000 prediction rows. It includes the full Giuseppe field list, all known
out-of-range audit fields, and a deterministic supplement balanced to 1,000
fields per county.

The model remains a pilot baseline. The dashboard must always display this
status and the model reason alongside every prediction.
