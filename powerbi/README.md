# Power BI report (optional extension)

One-page report on top of the gold layer. The .pbix file is not part of this repo; this page describes how to build it.

## Connect

Get Data -> Databricks (or the SQL endpoint of your workspace) -> select the schema `fhir_gold`. Import mode is fine for this data size.

Tables / views to load: `dim_patient`, `fact_encounter`, `fact_observation`, `fact_condition`, `dim_date`, `vw_top_conditions`, `vw_vital_signs_summary`.

## Model

| From | To | Cardinality |
|------|----|-------------|
| `dim_patient[patient_id]` | `fact_encounter[patient_id]` | one to many |
| `dim_patient[patient_id]` | `fact_observation[patient_id]` | one to many |
| `dim_patient[patient_id]` | `fact_condition[patient_id]` | one to many |
| `dim_date[date_key]` | `fact_encounter[encounter_date]` | one to many |

Keep the other date relationships inactive to avoid ambiguity.

## Measures

```
Patients       = DISTINCTCOUNT ( dim_patient[patient_id] )
Encounters     = DISTINCTCOUNT ( fact_encounter[encounter_id] )
Observations   = DISTINCTCOUNT ( fact_observation[observation_id] )
Conditions     = DISTINCTCOUNT ( fact_condition[condition_id] )
Avg Visit Min  = AVERAGE ( fact_encounter[duration_minutes] )
```

## Page layout

- Top row, four cards: Patients, Encounters, Observations, Conditions.
- Left: column chart of encounters per day by class (`dim_date` / `fact_encounter[class_display]`).
- Middle: bar chart of the top 10 conditions (`vw_top_conditions`).
- Right: patients by age group (`dim_patient[age_group]`) as a donut.
- Bottom: table of vital signs (`vw_vital_signs_summary`) with average, min and max.
- Slicers: gender, encounter class, date range.
