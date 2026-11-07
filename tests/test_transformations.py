"""Spark-based tests: JSON/XML parsing, silver flattening and SCD2 change detection.

Uses a local Spark session, no Delta needed (the merge itself is a thin wrapper
around the change logic tested here).
"""

import json
import xml.etree.ElementTree as ET
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import functions as F

from fhir_pipeline.bronze import _json_entries
from fhir_pipeline.fhir_schema import xml_resource_to_dict
from fhir_pipeline.scd2 import add_row_hash, compute_scd2_changes
from fhir_pipeline.silver import flatten_bronze
from fhir_pipeline.silver_models import model_for


PATIENT = {
    "resourceType": "Patient",
    "id": "p1",
    "meta": {"versionId": "2", "lastUpdated": "2026-11-05T10:00:00.000+00:00"},
    "active": True,
    "identifier": [{"system": "urn:mrn", "value": "MRN-1"}],
    "name": [{"use": "official", "family": " Sharma ", "given": ["Asha", "K"]}],
    "telecom": [{"system": "email", "value": "a@x.org"}, {"system": "phone", "value": "555-0100"}],
    "gender": "Female",
    "birthDate": "1990-04-12",
    "address": [{"city": "Pune", "state": "MH", "postalCode": "411001", "country": "IN"}],
    "maritalStatus": {"coding": [{"system": "s", "code": "M", "display": "Married"}]},
}

ENCOUNTER = {
    "resourceType": "Encounter",
    "id": "e1",
    "meta": {"versionId": "1", "lastUpdated": "2026-11-05T11:00:00.000+00:00"},
    "status": "finished",
    "class": {"system": "s", "code": "AMB", "display": "ambulatory"},
    "type": [{"coding": [{"code": "99213", "display": "Office visit"}]}],
    "subject": {"reference": "Patient/p1"},
    "period": {"start": "2026-11-05T09:00:00+00:00", "end": "2026-11-05T09:30:00+00:00"},
    "reasonCode": [{"text": "Fever"}],
}

OBSERVATION = {
    "resourceType": "Observation",
    "id": "o1",
    "meta": {"versionId": "1", "lastUpdated": "2026-11-05T12:00:00.000+00:00"},
    "status": "final",
    "category": [{"coding": [{"code": "vital-signs"}]}],
    "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4", "display": "Heart rate"}]},
    "subject": {"reference": "Patient/p1"},
    "encounter": {"reference": "Encounter/e1"},
    "effectiveDateTime": "2026-11-05T09:10:00+00:00",
    "valueQuantity": {"value": 72, "unit": "beats/min"},
    "component": [{"code": {"text": "x"}}],
}

CONDITION = {
    "resourceType": "Condition",
    "id": "c1",
    "meta": {"versionId": "1", "lastUpdated": "2026-11-05T13:00:00.000+00:00"},
    "clinicalStatus": {"coding": [{"code": "active"}]},
    "code": {"text": "Hypertension"},
    "subject": {"reference": "Patient/p1"},
    "encounter": {"reference": "urn:uuid:not-an-encounter-ref"},
    "onsetDateTime": "2020-01-01",
    "recordedDate": "2026-11-05T13:00:00+00:00",
}


def bronze_df(spark, resource_dict):
    row = (
        json.dumps(resource_dict),
        resource_dict["meta"]["versionId"],
        datetime.fromisoformat(resource_dict["meta"]["lastUpdated"]),
        datetime(2026, 11, 6, 1, 0, 0),
        "run1",
    )
    return spark.createDataFrame(row and [row], "resource_json string, version_id string, last_updated timestamp, extraction_timestamp timestamp, run_id string")


def one(df):
    return df.collect()[0].asDict()


# ------------------------------------------------------------- silver flattening
def test_patient_flatten(spark):
    r = one(flatten_bronze(bronze_df(spark, PATIENT), "Patient"))
    assert r["patient_id"] == "p1"
    assert r["family_name"] == "Sharma"  # trimmed
    assert r["full_name"] == "Asha Sharma"
    assert r["gender"] == "female"
    assert str(r["birth_date"]) == "1990-04-12"
    assert r["phone"] == "555-0100" and r["email"] == "a@x.org"
    assert r["address_city"] == "Pune"
    assert r["marital_status_display"] == "Married"
    assert r["is_deceased"] is False
    assert r["source_version_id"] == "2"
    assert len(r["row_hash"]) == 64


def test_encounter_flatten(spark):
    r = one(flatten_bronze(bronze_df(spark, ENCOUNTER), "Encounter"))
    assert r["patient_id"] == "p1"
    assert r["class_code"] == "AMB"
    assert r["type_display"] == "Office visit"
    assert r["reason_display"] == "Fever"
    assert r["period_start"] < r["period_end"]


def test_observation_flatten(spark):
    r = one(flatten_bronze(bronze_df(spark, OBSERVATION), "Observation"))
    assert r["patient_id"] == "p1" and r["encounter_id"] == "e1"
    assert r["category_code"] == "vital-signs"
    assert r["code_display"] == "Heart rate"
    assert r["value_number"] == 72.0 and r["value_unit"] == "beats/min"
    assert r["component_count"] == 1


def test_condition_flatten_handles_odd_references(spark):
    r = one(flatten_bronze(bronze_df(spark, CONDITION), "Condition"))
    assert r["code_display"] == "Hypertension"
    assert r["clinical_status_code"] == "active"
    assert r["encounter_id"] is None  # urn:uuid reference is not an Encounter/<id>
    assert str(r["onset_ts"]).startswith("2020-01-01")


def test_sparse_patient_does_not_fail(spark):
    sparse = {"resourceType": "Patient", "id": "p9", "meta": {"versionId": "1", "lastUpdated": "2026-11-05T10:00:00.000+00:00"}}
    r = one(flatten_bronze(bronze_df(spark, sparse), "Patient"))
    assert r["patient_id"] == "p9" and r["full_name"] is None and r["birth_date"] is None


def test_latest_version_wins_inside_one_load(spark):
    older = {**PATIENT, "meta": {"versionId": "1", "lastUpdated": "2026-11-04T10:00:00.000+00:00"}, "gender": "male"}
    df = bronze_df(spark, PATIENT).unionByName(bronze_df(spark, older))
    rows = flatten_bronze(df, "Patient").collect()
    assert len(rows) == 1 and rows[0]["gender"] == "female"


# ------------------------------------------------------------------ bronze JSON
def test_bundle_pages_are_split_into_entries(spark, tmp_path):
    page = {"resourceType": "Bundle", "entry": [{"fullUrl": "u1", "resource": PATIENT}, {"fullUrl": "u2", "resource": {**PATIENT, "id": "p2"}}]}
    f = tmp_path / "page_0001.json"
    f.write_text(json.dumps(page))
    df = _json_entries(spark, [str(f)])
    rows = df.collect()
    assert len(rows) == 2
    parsed = json.loads(rows[0]["resource_payload"])
    assert parsed["resourceType"] == "Patient"
    assert rows[0]["source_file"].endswith("page_0001.json")


# --------------------------------------------------------------------------- XML
def test_xml_resource_matches_json_shape():
    xml = """
    <Patient xmlns="http://hl7.org/fhir">
      <id value="p1"/>
      <meta><versionId value="2"/><lastUpdated value="2026-11-05T10:00:00.000+00:00"/></meta>
      <active value="true"/>
      <name><use value="official"/><family value="Sharma"/><given value="Asha"/><given value="K"/></name>
      <telecom><system value="phone"/><value value="555-0100"/></telecom>
      <gender value="female"/>
      <birthDate value="1990-04-12"/>
      <address><city value="Pune"/></address>
    </Patient>"""
    d = xml_resource_to_dict(ET.fromstring(xml))
    assert d["resourceType"] == "Patient" and d["id"] == "p1"
    assert d["active"] is True
    assert d["name"] == [{"use": "official", "family": "Sharma", "given": ["Asha", "K"]}]
    assert d["telecom"][0]["value"] == "555-0100"
    assert d["address"] == [{"city": "Pune"}]
    assert d["meta"]["versionId"] == "2"


def test_xml_single_repeating_element_stays_a_list():
    xml = '<Encounter xmlns="http://hl7.org/fhir"><id value="e1"/><type><coding><code value="1"/></coding></type></Encounter>'
    d = xml_resource_to_dict(ET.fromstring(xml))
    assert isinstance(d["type"], list) and d["type"][0]["coding"][0]["code"] == "1"


# -------------------------------------------------------------------------- SCD2
def staged_rows(spark, rows):
    """rows: (id, name, effective_from)"""
    df = spark.createDataFrame(rows, "patient_id string, full_name string, effective_from timestamp")
    return add_row_hash(df, ["full_name"])


def current_rows(spark, rows):
    """rows: (id, name, valid_from, version)"""
    df = spark.createDataFrame(rows, "patient_id string, full_name string, valid_from timestamp, version_number int")
    return add_row_hash(df, ["full_name"])


T1 = datetime(2026, 11, 5, 10, 0, 0)
T2 = datetime(2026, 11, 6, 10, 0, 0)


def test_scd2_new_changed_unchanged(spark):
    current = current_rows(spark, [("a", "Asha", T1, 1), ("b", "Bala", T1, 1), ("c", "Chen", T1, 3)])
    staged = staged_rows(spark, [("a", "Asha", T2), ("b", "Bala R", T2), ("d", "Dev", T2)])  # a same, b changed, d new

    new_versions, closers = compute_scd2_changes(staged, current, "patient_id")
    new = {r["patient_id"]: r.asDict() for r in new_versions.collect()}
    closed = {r["merge_key"]: r["close_at"] for r in closers.collect()}

    assert set(new) == {"b", "d"}  # unchanged a is skipped, c was not in this load so it stays as is
    assert new["b"]["version_number"] == 2 and new["d"]["version_number"] == 1
    assert new["b"]["is_current"] is True and new["b"]["valid_to"] is None and new["b"]["valid_from"] == T2
    assert closed == {"b": T2}  # old row of b is closed at the time the new version starts


def test_scd2_ignores_older_replayed_version(spark):
    current = current_rows(spark, [("a", "Asha New", T2, 2)])
    staged = staged_rows(spark, [("a", "Asha Old", T1)])  # different content but older than what we hold
    new_versions, closers = compute_scd2_changes(staged, current, "patient_id")
    assert new_versions.count() == 0 and closers.count() == 0


def test_scd2_null_vs_empty_hash_differs(spark):
    df = spark.createDataFrame([("a", None), ("b", "")], "patient_id string, full_name string")
    hashes = [r["row_hash"] for r in add_row_hash(df, ["full_name"]).orderBy("patient_id").collect()]
    assert hashes[0] != hashes[1]
