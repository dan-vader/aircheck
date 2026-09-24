# AirCheck - IoT Air Quality Data Platform

End-to-end Databricks platform for ingesting, processing, and analyzing air quality
data from IoT sensors (sensor.community), combining historical batch data with a
live streaming feed and demonstrating schema evolution on the streaming path.

## Architecture

Medallion architecture on a single Unity Catalog catalog, split into four schemas:

* `aircheck_bronze` - raw ingested data, append-only, with `_source` and `_ingested_at`
  on every table.
* `aircheck_silver` - cleaned, deduplicated, unpivoted into a long format
  (`sensor_id`, `event_ts_utc`, `value_type`, `value`).
* `aircheck_gold` - business-ready aggregates and anomaly detection.
* `aircheck_ops` - reserved for `pipeline_log`, an append-only run log. Not wired into
  the jobs yet (see Known Limitations).

Data and streaming checkpoints live in a managed Unity Catalog volume.

## Data Sources

* **Batch**: historical CSV archive from sensor.community, filtered to a known set of
  sensor IDs pulled from a device registry built separately from the live API.
* **Streaming**: live sensor readings pushed through Azure Event Hub.

## Ingestion

* **Batch** (`ingestion/batch/`): Auto Loader with `availableNow` trigger. Idempotency
  relies on Auto Loader's own checkpoint, so a file is never processed twice. The
  device registry is a separate MERGE-based upsert job.
* **Streaming** (`ingestion/streaming/`): an Event Hub producer polls the live API and
  publishes events; the consumer notebook (`load_bronze_streaming.py`) reads them via
  the Kafka-compatible endpoint and writes to bronze.
* **Schema evolution**: the streaming schema is config-driven (`config/aircheck.yaml`),
  not a generic map type, so a new sensor field becomes a real column in
  `DESCRIBE TABLE` rather than being hidden in a rescued-data blob. `mergeSchema` is
  enabled on every streaming write.

## Governance

* Secrets are read through a Key Vault-backed Databricks secret scope, with an
  `aircheck-*` key prefix.
* Grants are least-privilege per role (batch, stream, analytics) - see
  `infra/03_apply_grants.py`.

## Gold Layer

* `air_quality_hourly`, `air_quality_daily_by_country` - aggregates by country and
  metric type, outdoor sensors only.
* `sensor_network_health` - active sensors and data freshness per country.
* `anomalies` - readings that exceed WHO daily thresholds for PM10 / PM2.5.

## Dashboard

**Streaming (live data):**

* Active Sensors Network - live counter of active sensors.
* Live PM10 & PM2.5 Trends - average pollution levels, updated in near real time.
* Live Air Quality Map - an interactive map colored by pollution level.
* Top-10 Most Polluted Locations.

**Aggregated (gold layer):**

* Active Sensors by Region - active sensor count per region (`sensor_network_health`).
* Average PM Level - hourly pollution averages (`air_quality_hourly`).
* Daily Air Quality by Country (`air_quality_daily_by_country`).
* Critical Pollution Anomalies - a table of threshold breaches (`anomalies`).

**Technical:**

* Schema Evolution Timeline - marks the point where a new metric appeared in the
  stream.

## How to Run

1. Run `ingestion/streaming/eventhub_producer.py` locally to publish live events to
   Event Hub. It is the single entry point; everything under
   `ingestion/streaming/producer/` is imported by it, not run separately.
2. Run `ingestion/streaming/load_bronze_streaming.py` in Databricks to consume them
   into bronze.
3. Run the batch notebooks in `ingestion/batch/` for the historical archive and device
   registry.
4. Run `transform/silver_readings.py`, `transform/silver_devices.py`, then
   `transform/gold_aggregates.py`.

## Known Limitations

* `silver.readings` is a full overwrite on every run, not incremental.
* No automated tests yet.
* Per-job logging into `ops.pipeline_log` is on a separate branch, not yet merged;
  jobs currently log to stdout only. The dashboard's technical view does not yet
  include a live event log for this reason.

## Tech Stack

Azure Databricks, PySpark, Delta Lake, Unity Catalog, Azure Event Hub, Databricks
Lakeview.
