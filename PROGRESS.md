# Progress Tracker

Last updated: 2026-11-07

## Steps

| # | Step | Status |
|---|------|--------|
| 1 | Project structure, config file | done |
| 2 | Core modules (client, raw writer, audit, bronze, silver, SCD2, gold) | done |
| 3 | Databricks notebooks (setup, ingest, silver, gold, validation) | done |
| 4 | Workflow definition (Patient -> Encounter -> Observation -> Condition) | done |
| 5 | SQL gold views, Power BI design notes | done |
| 6 | Docs (README, architecture, data dictionary) | done |
| 7 | Unit tests (24 passing locally with PySpark) | done |
| 8 | ZIP package | done |

## Still to do on the Databricks side
- Push to GitHub, add as a Git folder, set the repo url in `workflows/fhir_pipeline_job.json`.
- Run `00_setup`, then the workflow, then `04_validation`.
- Run a second day later to see SCD2 versions appear.
- Build the Power BI page from `powerbi/README.md` (optional).

## Live test on Databricks Free Edition (2026-11-07)
- HAPI reachable from serverless compute (HTTP 200); all four resources loaded raw -> bronze -> silver (first load) -> gold, 3 pages per day as a demo cap.
- SCD2 merge tested on a scratch copy of silver.patient: 5 changed records produced 5 closed rows plus 5 new current rows (`valid_to` of v1 = `valid_from` of v2); replaying the same batch changed nothing. Scratch table dropped afterwards.
- Finding: the public server has an observation dated 2137. `dim_date` is now capped at today and `fact_observation` has an `is_future_dated` flag.

## Not verified live
- Notebooks 00-04 and the workflow JSON were not run as a job (module functions were called from a scratch notebook instead).
- XML mode ran only in the unit tests.
- The `dim_date` cap was tested locally, not re-run in the workspace.
