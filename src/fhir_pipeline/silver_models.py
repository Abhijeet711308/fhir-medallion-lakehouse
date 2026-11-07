"""Column mapping from the parsed FHIR resource (alias r) to flat silver columns.

Expressions are Spark SQL. try_element_at / try_cast are used so a missing array
element or an odd value gives NULL instead of failing the job.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

from .fhir_schema import spec_to_ddl


def first(expr: str) -> str:
    return f"try_element_at({expr}, 1)"


def ref_id(expr: str, rtype: str) -> str:
    """'Patient/123' -> '123'. Returns NULL when the reference is not of that type."""
    return f"nullif(regexp_extract({expr}, '(?:^|/){rtype}/([^/]+)$', 1), '')"


def ts(expr: str) -> str:
    return f"try_to_timestamp({expr})"


# first coding of the first element of a CodeableConcept array
def _cc_first(arr: str, field: str) -> str:
    return f"{first(first(arr) + '.coding')}.{field}"


def _cc(cc: str, field: str) -> str:
    return f"{first(cc + '.coding')}.{field}"


def telecom_value(system: str) -> str:
    """First telecom value for a given system (phone, email, ...)."""
    matches = f"filter(r.telecom, x -> x.system = '{system}')"
    return f"{first(matches)}.value"


# (silver column, spark sql expression)
PATIENT_COLUMNS: List[Tuple[str, str]] = [
    ("patient_id", "r.id"),
    ("active", "r.active"),
    ("identifier_system", f"{first('r.identifier')}.system"),
    ("identifier_value", f"{first('r.identifier')}.value"),
    ("given_name", f"trim({first(first('r.name') + '.given')})"),
    ("family_name", f"trim({first('r.name')}.family)"),
    (
        "full_name",
        "nullif(regexp_replace(trim(concat_ws(' ', "
        f"trim({first(first('r.name') + '.given')}), trim({first('r.name')}.family))), '\\\\s+', ' '), '')",
    ),
    ("gender", "lower(trim(r.gender))"),
    ("birth_date", "try_cast(r.birthDate AS DATE)"),
    ("birth_year", "try_cast(substring(r.birthDate, 1, 4) AS INT)"),
    ("is_deceased", "coalesce(r.deceasedBoolean, r.deceasedDateTime IS NOT NULL, false)"),
    ("deceased_datetime", ts("r.deceasedDateTime")),
    ("marital_status_code", _cc("r.maritalStatus", "code")),
    ("marital_status_display", f"coalesce({_cc('r.maritalStatus', 'display')}, r.maritalStatus.text)"),
    ("phone", telecom_value("phone")),
    ("email", telecom_value("email")),
    ("address_city", f"{first('r.address')}.city"),
    ("address_state", f"{first('r.address')}.state"),
    ("address_postal_code", f"{first('r.address')}.postalCode"),
    ("address_country", f"{first('r.address')}.country"),
    ("language_code", _cc(f"{first('r.communication')}.language", "code")),
]

ENCOUNTER_COLUMNS: List[Tuple[str, str]] = [
    ("encounter_id", "r.id"),
    ("status", "r.status"),
    ("class_code", "r.`class`.code"),
    ("class_display", "r.`class`.display"),
    ("type_code", _cc_first("r.type", "code")),
    ("type_display", f"coalesce({_cc_first('r.type', 'display')}, {first('r.type')}.text)"),
    ("patient_id", ref_id("r.subject.reference", "Patient")),
    ("period_start", ts("r.period.start")),
    ("period_end", ts("r.period.`end`")),
    ("reason_code", _cc_first("r.reasonCode", "code")),
    ("reason_display", f"coalesce({_cc_first('r.reasonCode', 'display')}, {first('r.reasonCode')}.text)"),
    ("service_provider_reference", "r.serviceProvider.reference"),
]

OBSERVATION_COLUMNS: List[Tuple[str, str]] = [
    ("observation_id", "r.id"),
    ("status", "r.status"),
    ("category_code", _cc_first("r.category", "code")),
    ("code_system", _cc("r.code", "system")),
    ("code", _cc("r.code", "code")),
    ("code_display", f"coalesce({_cc('r.code', 'display')}, r.code.text)"),
    ("patient_id", ref_id("r.subject.reference", "Patient")),
    ("encounter_id", ref_id("r.encounter.reference", "Encounter")),
    ("effective_ts", f"coalesce({ts('r.effectiveDateTime')}, {ts('r.effectivePeriod.start')})"),
    ("issued_ts", ts("r.issued")),
    ("value_number", "coalesce(r.valueQuantity.value, r.valueInteger)"),
    ("value_unit", "r.valueQuantity.unit"),
    (
        "value_text",
        f"coalesce(r.valueString, {_cc('r.valueCodeableConcept', 'display')}, r.valueCodeableConcept.text, cast(r.valueBoolean AS STRING))",
    ),
    ("interpretation_code", _cc_first("r.interpretation", "code")),
    ("component_count", "CASE WHEN r.component IS NULL THEN 0 ELSE size(r.component) END"),
]

CONDITION_COLUMNS: List[Tuple[str, str]] = [
    ("condition_id", "r.id"),
    ("clinical_status_code", _cc("r.clinicalStatus", "code")),
    ("verification_status_code", _cc("r.verificationStatus", "code")),
    ("category_code", _cc_first("r.category", "code")),
    ("code_system", _cc("r.code", "system")),
    ("code", _cc("r.code", "code")),
    ("code_display", f"coalesce({_cc('r.code', 'display')}, r.code.text)"),
    ("severity_display", f"coalesce({_cc('r.severity', 'display')}, r.severity.text)"),
    ("patient_id", ref_id("r.subject.reference", "Patient")),
    ("encounter_id", ref_id("r.encounter.reference", "Encounter")),
    ("onset_ts", ts("r.onsetDateTime")),
    ("abatement_ts", ts("r.abatementDateTime")),
    ("recorded_date", "try_cast(substring(r.recordedDate, 1, 10) AS DATE)"),
]


MODELS: Dict[str, Dict[str, object]] = {
    "Patient": {"key": "patient_id", "columns": PATIENT_COLUMNS},
    "Encounter": {"key": "encounter_id", "columns": ENCOUNTER_COLUMNS},
    "Observation": {"key": "observation_id", "columns": OBSERVATION_COLUMNS},
    "Condition": {"key": "condition_id", "columns": CONDITION_COLUMNS},
}


def model_for(resource: str) -> Dict[str, object]:
    model = dict(MODELS[resource])
    model["ddl"] = spec_to_ddl(resource)
    model["business_columns"] = [c for c, _ in model["columns"] if c != model["key"]]
    return model
