-- Gold layer: reporting views on top of the current rows in silver.
-- Placeholders {catalog}, {silver}, {gold}, {meta} are filled in from the config by the gold notebook.

CREATE OR REPLACE VIEW {catalog}.{gold}.dim_patient AS
SELECT
    patient_id,
    full_name,
    given_name,
    family_name,
    gender,
    birth_date,
    CASE WHEN birth_date IS NULL THEN NULL
         ELSE CAST(floor(months_between(current_date(), birth_date) / 12) AS INT) END AS age_years,
    CASE
        WHEN birth_date IS NULL THEN 'Unknown'
        WHEN months_between(current_date(), birth_date) / 12 < 18 THEN '0-17'
        WHEN months_between(current_date(), birth_date) / 12 < 40 THEN '18-39'
        WHEN months_between(current_date(), birth_date) / 12 < 65 THEN '40-64'
        ELSE '65+'
    END AS age_group,
    marital_status_display,
    address_city AS city,
    address_state AS state,
    address_country AS country,
    is_deceased,
    active,
    valid_from AS record_valid_from,
    version_number
FROM {catalog}.{silver}.patient
WHERE is_current;

CREATE OR REPLACE VIEW {catalog}.{gold}.fact_encounter AS
SELECT
    e.encounter_id,
    e.patient_id,
    p.patient_id IS NOT NULL AS patient_in_warehouse,
    e.status,
    e.class_code,
    e.class_display,
    e.type_display,
    e.reason_display,
    e.period_start,
    e.period_end,
    CAST(e.period_start AS DATE) AS encounter_date,
    CASE WHEN e.period_start IS NOT NULL AND e.period_end IS NOT NULL
         THEN (unix_timestamp(e.period_end) - unix_timestamp(e.period_start)) / 60.0 END AS duration_minutes,
    e.service_provider_reference
FROM {catalog}.{silver}.encounter e
LEFT JOIN {catalog}.{silver}.patient p
    ON e.patient_id = p.patient_id AND p.is_current
WHERE e.is_current;

CREATE OR REPLACE VIEW {catalog}.{gold}.fact_observation AS
SELECT
    o.observation_id,
    o.patient_id,
    o.encounter_id,
    p.patient_id IS NOT NULL AS patient_in_warehouse,
    o.status,
    o.category_code,
    o.code,
    o.code_display,
    o.effective_ts,
    CAST(o.effective_ts AS DATE) AS effective_date,
    CAST(o.effective_ts AS DATE) > current_date() AS is_future_dated,
    o.value_number,
    o.value_unit,
    o.value_text,
    o.interpretation_code
FROM {catalog}.{silver}.observation o
LEFT JOIN {catalog}.{silver}.patient p
    ON o.patient_id = p.patient_id AND p.is_current
WHERE o.is_current;

CREATE OR REPLACE VIEW {catalog}.{gold}.fact_condition AS
SELECT
    c.condition_id,
    c.patient_id,
    c.encounter_id,
    p.patient_id IS NOT NULL AS patient_in_warehouse,
    c.clinical_status_code,
    c.verification_status_code,
    c.category_code,
    c.code,
    c.code_display,
    c.severity_display,
    c.onset_ts,
    CAST(c.onset_ts AS DATE) AS onset_date,
    c.abatement_ts,
    c.recorded_date
FROM {catalog}.{silver}.condition c
LEFT JOIN {catalog}.{silver}.patient p
    ON c.patient_id = p.patient_id AND p.is_current
WHERE c.is_current;

CREATE OR REPLACE VIEW {catalog}.{gold}.vw_patient_summary AS
SELECT
    p.patient_id,
    p.full_name,
    p.gender,
    p.age_years,
    p.age_group,
    p.city,
    p.state,
    COALESCE(e.encounter_count, 0)    AS encounter_count,
    COALESCE(o.observation_count, 0)  AS observation_count,
    COALESCE(c.condition_count, 0)    AS condition_count,
    e.last_encounter_date
FROM {catalog}.{gold}.dim_patient p
LEFT JOIN (
    SELECT patient_id, COUNT(*) AS encounter_count, MAX(encounter_date) AS last_encounter_date
    FROM {catalog}.{gold}.fact_encounter GROUP BY patient_id
) e ON p.patient_id = e.patient_id
LEFT JOIN (
    SELECT patient_id, COUNT(*) AS observation_count
    FROM {catalog}.{gold}.fact_observation GROUP BY patient_id
) o ON p.patient_id = o.patient_id
LEFT JOIN (
    SELECT patient_id, COUNT(*) AS condition_count
    FROM {catalog}.{gold}.fact_condition GROUP BY patient_id
) c ON p.patient_id = c.patient_id;

CREATE OR REPLACE VIEW {catalog}.{gold}.vw_daily_encounters AS
SELECT
    encounter_date,
    COALESCE(class_display, class_code, 'Unknown') AS encounter_class,
    COUNT(*)                         AS encounter_count,
    COUNT(DISTINCT patient_id)       AS patient_count,
    ROUND(AVG(duration_minutes), 1)  AS avg_duration_minutes
FROM {catalog}.{gold}.fact_encounter
WHERE encounter_date IS NOT NULL
GROUP BY encounter_date, COALESCE(class_display, class_code, 'Unknown');

CREATE OR REPLACE VIEW {catalog}.{gold}.vw_top_conditions AS
SELECT
    COALESCE(code_display, code, 'Unknown') AS condition_name,
    code,
    COUNT(*)                   AS condition_count,
    COUNT(DISTINCT patient_id) AS patient_count
FROM {catalog}.{gold}.fact_condition
GROUP BY COALESCE(code_display, code, 'Unknown'), code;

CREATE OR REPLACE VIEW {catalog}.{gold}.vw_vital_signs_summary AS
SELECT
    code,
    code_display,
    value_unit,
    COUNT(*)                      AS reading_count,
    ROUND(AVG(value_number), 2)   AS avg_value,
    ROUND(MIN(value_number), 2)   AS min_value,
    ROUND(MAX(value_number), 2)   AS max_value
FROM {catalog}.{gold}.fact_observation
WHERE category_code = 'vital-signs' AND value_number IS NOT NULL
GROUP BY code, code_display, value_unit;

CREATE OR REPLACE VIEW {catalog}.{gold}.vw_pipeline_audit AS
SELECT
    r.run_id,
    r.resource_type,
    r.layer,
    r.status,
    r.rows_read,
    r.rows_written,
    r.started_at,
    r.finished_at,
    r.message
FROM {catalog}.{meta}.{run_log} r;

CREATE OR REPLACE VIEW {catalog}.{gold}.vw_api_call_summary AS
SELECT
    run_id,
    resource_type,
    CAST(window_start AS DATE) AS load_date,
    COUNT(*)                   AS pages_fetched,
    SUM(entry_count)           AS resources_received,
    MIN(called_at)             AS first_call_at,
    MAX(saved_at)              AS last_saved_at
FROM {catalog}.{meta}.{api_call_log}
WHERE http_status = 200
GROUP BY run_id, resource_type, CAST(window_start AS DATE);
