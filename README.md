# FHIR Medallion Lakehouse

Incremental ingestion of FHIR R4 data (Patient, Encounter, Observation, Condition) from the public HAPI test server into a Raw -> Bronze -> Silver -> Gold lakehouse on Databricks, with SCD Type 2 history in silver.

Author: Abhijeet Kumar Dubey

## What it does

1. Calls `https://hapi.fhir.org/baseR4` one day at a time using `_lastUpdated` windows, and follows the Bundle `next` links until a day is finished.
2. Saves every page exactly as returned in the raw zone, bucketed by date and run.
3. Logs every API call (url, when it was called, when it was saved, status, file) in `api_call_log`.
4. Loads the pages into bronze Delta tables, one row per resource version, with `extraction_timestamp` and `api_url_or_params`.
5. Builds silver tables: typed columns, cleaned, de-duplicated, with SCD Type 2 history.
6. Builds gold views and a date table for reporting.
7. A Databricks Workflow runs it all in the order Patient -> Encounter -> Observation -> Condition.

Details on layers, tables and relationships are in [docs/architecture.md](docs/architecture.md).

## Repo layout

```
config/pipeline_config.yml     all settings (url, page size, schemas, resource order, ...)
src/fhir_pipeline/             reusable code
    fhir_client.py             paging, retries, incremental windows
    raw_writer.py              raw zone writer
    audit.py                   api call log, run log, watermark tables
    bronze.py                  raw pages -> bronze Delta
    silver_models.py           FHIR field -> silver column mapping
    silver.py / scd2.py        flatten, clean, de-duplicate, SCD2 logic
    gold.py                    gold views + dim_date
    ingest.py                  orchestrates one resource (api -> raw -> bronze -> logs)
    fhir_schema.py             field specs, used for JSON parsing and the XML option
notebooks/                     Databricks notebooks (thin wrappers over src/)
sql/gold_views.sql             gold layer view definitions
workflows/fhir_pipeline_job.json   Databricks Workflow definition
powerbi/README.md              one-page report design on top of gold
tests/                         unit tests
docs/                          architecture, data dictionary
PROGRESS.md                    build log
```

## Setup (Databricks)

1. Create a Databricks workspace (the free edition is enough) and push this repo to GitHub.
2. In Databricks go to Workspace -> Create -> Git folder and add the GitHub repo.
3. Check `config/pipeline_config.yml`. The defaults match the free edition (`catalog: workspace`). Change the catalog name if yours is different.
4. Open `notebooks/00_setup` and run it. It creates the schemas, the raw volume and the metadata tables.

## Running it

### Option A: Workflow

1. Open `workflows/fhir_pipeline_job.json`, check that `git_url` points to your repo, and create the job (Workflows -> Create job -> JSON, or the Jobs API / CLI).
2. Run it with the defaults. With no dates and no watermark it loads the last 3 days.
3. Run it again the next day (or after the source data changes). It continues from the watermark, and silver adds a new version for every record that changed.

Job parameters: `start_date`, `end_date` (YYYY-MM-DD, end inclusive), `payload_format` (`json` or `xml`), `config_path`.

Task order: `setup` -> `ingest_patient` -> `ingest_encounter` -> `ingest_observation` -> `ingest_condition`. Each silver task starts as soon as its own ingest task is done, `gold` waits for all four, and `validation` runs last.

### Option B: notebooks by hand

Run `01_ingest_resource` for Patient, Encounter, Observation, Condition (same `run_id` for the ingest and silver notebooks of one resource), then `02_silver_resource` for each, then `03_gold` and `04_validation`.

## Configuration

Everything that can change lives in `config/pipeline_config.yml`: base url, page size, retry settings, resource list and order, schema names, table names, backfill days, and a cap on pages per day (`max_pages_per_window`, 10 by default so a demo run stays small; set it to `null` to load everything). Notebook widgets and job parameters override dates and format.

## XML option

Set `payload_format` to `xml` (config, widget or job parameter). The raw zone then holds `.xml` pages. Bronze keeps the original XML in `resource_payload` and a JSON copy in `resource_json`; silver, gold and the rest of the pipeline are unchanged. Use a separate catalog/schema set (config override) if you want the XML run next to the JSON run instead of on top of it.

## Tests

```
pip install pyspark pyyaml requests pytest
pytest tests
```

Covers: paging and retries (mocked API), day-window logic, JSON and XML parsing, silver column mapping, SCD2 change detection, and the gold SQL running against small sample tables.

## Test status

- 24 unit tests pass locally (PySpark).
- Ingestion, bronze, silver, gold and the SCD2 merge were exercised on a Databricks Free Edition workspace against the live HAPI server. The notebooks and workflow JSON were not run end to end as a job; see PROGRESS.md for details.

## Design notes and assumptions

- **Incremental logic:** one window per UTC day, stored watermark per resource in `pipeline_watermark`. The watermark only moves after bronze is written, so a failed run is retried from the same point.
- **Idempotent loads:** bronze inserts are keyed on `run_id + resource_id + version_id`; silver only adds a version when the business columns actually change (row hash) and the incoming record is newer than the current one.
- **Change detection:** the hash covers business columns only, not the FHIR version number, so a pure metadata bump does not create a new silver version. Bronze still keeps every version received.
- **Same record twice in one load:** silver keeps the latest one. Earlier ones stay in bronze.
- **Deletes:** the search API does not report deleted resources, so deletes are not tracked.
- **Public server:** HAPI is a shared test server with a lot of data and occasional 429/5xx responses. The client retries with backoff and waits briefly between pages.
- **Future-dated test data:** HAPI contains records with dates far in the future. `fact_observation.is_future_dated` marks them and `dim_date` stops at today.
- **References:** encounter/observation/condition rows can point to patients that were not loaded in the same window. Gold facts expose `patient_in_warehouse` for that.
- **Dataflow Gen2** is a Fabric feature and is not used here. All transformations are Spark. The notebooks only use config paths, so they can be moved to Fabric by changing `catalog` and `raw_root_override`.
