# Kenya dashboard production deployment

Target host: `yield-forecast.geo-infinity.com`

The deployment keeps application releases under `/opt/kenya-dashboard` and
reads the existing production sources in `/opt/project`. It does not duplicate
the Sentinel-1 or Sentinel-2 tables. Only field geometry, crop predictions and
the compact prediction summary are added to the `kenya_monitoring` database.

## Audited production facts

- PostgreSQL 16.15 and PostGIS 3.6.4 are active on localhost.
- Existing `timeseries.s1_indices` and `timeseries.s2_indices` contain the full
  production observations required by the API.
- Model run: `kenya_v3_all_fields_20261014`.
- Expected inventory: 269,453 fields, 543 weeks and 146,312,979 predictions.
- Source geometry: `/opt/project/final2.gpkg`.
- Source predictions:
  `/opt/project/model_runs/kenya_v3_all_fields_20261014/outputs`.
- Root filesystem has enough working space. `/data` is 98% full, so loader
  scratch files and PostgreSQL data must remain on the root filesystem.
- Nginx is not currently installed or active. Port 80, 443 and 8000 are free.

## 1. Build and publish the deployment branch from the Mac

Run from the repository root:

```bash
cd /Users/boedy1996/kenya-eo-monitoring
pyenv local kenya-eo-monitoring
nvm use

cd dashboard/web
npm ci
npm run build
cd ../..

git switch dashboard-production
git add .gitignore .nvmrc dashboard
git add -f dashboard/web/dist
git diff --cached --stat
git commit -m "Update production crop monitoring dashboard"
git push origin dashboard-production
```

The generated frontend is deliberately tracked on this deployment branch, so
the server does not need Node.js. Local data, `node_modules`, Python caches and
environment files remain excluded by `.gitignore`.

## 2. Clone an immutable release on the server

```bash
ssh -i ~/.ssh/server_victor.pem root@yield-forecast.geo-infinity.com

export RELEASE_SHA="$(git ls-remote \
  git@github.com:boedy1996/kenya-eo-monitoring.git \
  refs/heads/dashboard-production | cut -c1-12)"
export RELEASE_ID="dashboard-${RELEASE_SHA}"
echo "$RELEASE_ID"

install -d -m 755 /opt/kenya-dashboard/releases
install -d -o postgres -g postgres -m 750 /opt/kenya-dashboard/load-tmp

if [ ! -d "/opt/kenya-dashboard/releases/${RELEASE_ID}/.git" ]; then
  git clone --branch dashboard-production --single-branch \
    git@github.com:boedy1996/kenya-eo-monitoring.git \
    "/opt/kenya-dashboard/releases/${RELEASE_ID}"
fi

git -C "/opt/kenya-dashboard/releases/${RELEASE_ID}" checkout "$RELEASE_SHA"
test -f "/opt/kenya-dashboard/releases/${RELEASE_ID}/dashboard/web/dist/index.html"

id kenya_dashboard >/dev/null 2>&1 || \
  useradd --system --home-dir /nonexistent --shell /usr/sbin/nologin kenya_dashboard

ln -sfn "/opt/kenya-dashboard/releases/${RELEASE_ID}" /opt/kenya-dashboard/current

apt-get update
apt-get install -y python3-venv

if [ ! -x /opt/kenya-dashboard/venv/bin/python ]; then
  /usr/bin/python3 -m venv /opt/kenya-dashboard/venv
fi

/opt/kenya-dashboard/venv/bin/pip install --upgrade pip
/opt/kenya-dashboard/venv/bin/pip install \
  -r /opt/kenya-dashboard/current/dashboard/api/requirements.lock \
  -r /opt/kenya-dashboard/current/dashboard/requirements-loader.lock
```

## 3. Apply the dashboard database migrations

Run migrations as the PostgreSQL administrator:

```bash
sudo -u postgres env \
  DATABASE_URL='postgresql:///kenya_monitoring?host=/var/run/postgresql' \
  /opt/kenya-dashboard/venv/bin/python \
  /opt/kenya-dashboard/current/dashboard/scripts/migrate.py
```

Expected final message:

```text
Database migrations are current.
```

## 4. Load the full production model data

The loader is resumable. It commits and checkpoints each of the 55 Parquet
files independently. Running the same command again skips completed files.

```bash
export RUN_ID=kenya_v3_all_fields_20261014
export LOAD_LOG="/var/log/kenya-dashboard-load-${RUN_ID}.log"

sudo -u postgres env \
  DATABASE_URL='postgresql:///kenya_monitoring?host=/var/run/postgresql' \
  nohup /opt/kenya-dashboard/venv/bin/python \
  /opt/kenya-dashboard/current/dashboard/scripts/load_production.py \
  --model-output-root /opt/project/model_runs/kenya_v3_all_fields_20261014/outputs \
  --gpkg /opt/project/final2.gpkg \
  --model-run-id "$RUN_ID" \
  --expected-fields 269453 \
  --expected-weeks 543 \
  --expected-predictions 146312979 \
  --scratch-dir /opt/kenya-dashboard/load-tmp \
  > "$LOAD_LOG" 2>&1 &

echo "loader_pid=$!"
tail -f "$LOAD_LOG"
```

Use `Ctrl+C` to leave `tail`; it does not stop the loader. Check progress with:

```bash
tail -n 40 "$LOAD_LOG"
df -h /
sudo -u postgres psql -d kenya_monitoring -c \
  "SELECT count(*) AS files, sum(row_count) AS rows FROM dashboard.load_checkpoints WHERE model_run_id = '$RUN_ID';"
```

The successful final line is:

```text
Production dashboard load complete
```

Do not start the public API before this completes. The run is marked active
only after geometry, all prediction files and the summary have loaded.

## 5. Validate the production load

```bash
sudo -u postgres env \
  DATABASE_URL='postgresql:///kenya_monitoring?host=/var/run/postgresql' \
  /opt/kenya-dashboard/venv/bin/python \
  /opt/kenya-dashboard/current/dashboard/scripts/validate_production.py
```

Continue only when the JSON report ends with `"status": "passed"`.

## 6. Create the read-only database service account

The OS account and PostgreSQL role share the same name, so PostgreSQL peer
authentication can be used without storing a database password.

```bash
sudo -u postgres psql -d kenya_monitoring -v ON_ERROR_STOP=1 <<'SQL'
DO $block$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kenya_dashboard') THEN
        CREATE ROLE kenya_dashboard LOGIN;
    END IF;
END
$block$;

GRANT CONNECT ON DATABASE kenya_monitoring TO kenya_dashboard;
GRANT USAGE ON SCHEMA dashboard, timeseries TO kenya_dashboard;
GRANT SELECT ON ALL TABLES IN SCHEMA dashboard, timeseries TO kenya_dashboard;
ALTER DEFAULT PRIVILEGES IN SCHEMA dashboard
    GRANT SELECT ON TABLES TO kenya_dashboard;
ALTER DEFAULT PRIVILEGES IN SCHEMA timeseries
    GRANT SELECT ON TABLES TO kenya_dashboard;
SQL

sudo -u kenya_dashboard psql -d kenya_monitoring -Atc \
  "SELECT model_run_id FROM dashboard.model_runs WHERE is_active;"
```

The last command should print `kenya_v3_all_fields_20261014`.

## 7. Install and start FastAPI with systemd

```bash
install -m 600 \
  /opt/kenya-dashboard/current/dashboard/deploy/kenya-dashboard.env.example \
  /etc/kenya-dashboard.env

install -m 644 \
  /opt/kenya-dashboard/current/dashboard/deploy/kenya-dashboard-api.service \
  /etc/systemd/system/kenya-dashboard-api.service

systemctl daemon-reload
systemctl enable --now kenya-dashboard-api
systemctl status kenya-dashboard-api --no-pager

curl --fail http://127.0.0.1:8000/api/v1/health
curl --fail http://127.0.0.1:8000/api/v1/catalog
```

View service logs with:

```bash
journalctl -u kenya-dashboard-api -f
```

## 8. Install Nginx and expose the dashboard

```bash
apt-get update
apt-get install -y nginx python3-certbot-nginx

install -m 644 \
  /opt/kenya-dashboard/current/dashboard/deploy/nginx-kenya-dashboard.conf \
  /etc/nginx/sites-available/kenya-dashboard

ln -sfn /etc/nginx/sites-available/kenya-dashboard \
  /etc/nginx/sites-enabled/kenya-dashboard

nginx -t
systemctl enable --now nginx

curl --fail -H 'Host: yield-forecast.geo-infinity.com' \
  http://127.0.0.1/api/v1/health
curl --fail -I -H 'Host: yield-forecast.geo-infinity.com' \
  http://127.0.0.1/
```

The DNS name already resolves to the server. After the HTTP checks pass,
request the certificate and redirect HTTP to HTTPS:

```bash
certbot --nginx -d yield-forecast.geo-infinity.com --redirect
```

Final checks from the Mac:

```bash
curl --fail https://yield-forecast.geo-infinity.com/api/v1/health
open https://yield-forecast.geo-infinity.com/
```

## 9. Release rollback

Application rollback does not alter production tables. Point `current` to the
previous release and restart the API:

```bash
ls -1 /opt/kenya-dashboard/releases
ln -sfn /opt/kenya-dashboard/releases/PREVIOUS_RELEASE /opt/kenya-dashboard/current
systemctl restart kenya-dashboard-api
nginx -t && systemctl reload nginx
```
