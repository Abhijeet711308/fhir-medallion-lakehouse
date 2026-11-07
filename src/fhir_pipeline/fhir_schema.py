"""Light-weight description of the FHIR R4 fields we use.

One spec per resource is used for two things:
  * building the Spark DDL string that parses the JSON payload in the silver step
  * converting an XML resource into the same dict shape as the JSON one

Spec rules: "string" / "boolean" / "double" are primitives, a dict is a nested
element, ("array", X) is a repeating element.
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict

# ------------------------------------------------------------------ building blocks
CODING = {"system": "string", "code": "string", "display": "string"}
CODEABLE = {"coding": ("array", CODING), "text": "string"}
REFERENCE = {"reference": "string", "display": "string", "type": "string"}
PERIOD = {"start": "string", "end": "string"}
QUANTITY = {"value": "double", "unit": "string", "system": "string", "code": "string"}
META = {"versionId": "string", "lastUpdated": "string"}

PATIENT = {
    "resourceType": "string",
    "id": "string",
    "meta": META,
    "active": "boolean",
    "identifier": ("array", {"system": "string", "value": "string"}),
    "name": ("array", {"use": "string", "family": "string", "given": ("array", "string")}),
    "telecom": ("array", {"system": "string", "value": "string", "use": "string"}),
    "gender": "string",
    "birthDate": "string",
    "deceasedBoolean": "boolean",
    "deceasedDateTime": "string",
    "address": ("array", {"city": "string", "state": "string", "postalCode": "string", "country": "string"}),
    "maritalStatus": CODEABLE,
    "communication": ("array", {"language": CODEABLE, "preferred": "boolean"}),
}

ENCOUNTER = {
    "resourceType": "string",
    "id": "string",
    "meta": META,
    "status": "string",
    "class": CODING,
    "type": ("array", CODEABLE),
    "subject": REFERENCE,
    "period": PERIOD,
    "reasonCode": ("array", CODEABLE),
    "serviceProvider": REFERENCE,
}

OBSERVATION = {
    "resourceType": "string",
    "id": "string",
    "meta": META,
    "status": "string",
    "category": ("array", CODEABLE),
    "code": CODEABLE,
    "subject": REFERENCE,
    "encounter": REFERENCE,
    "effectiveDateTime": "string",
    "effectivePeriod": PERIOD,
    "issued": "string",
    "valueQuantity": QUANTITY,
    "valueCodeableConcept": CODEABLE,
    "valueString": "string",
    "valueBoolean": "boolean",
    "valueInteger": "double",
    "interpretation": ("array", CODEABLE),
    "component": ("array", {"code": CODEABLE}),
}

CONDITION = {
    "resourceType": "string",
    "id": "string",
    "meta": META,
    "clinicalStatus": CODEABLE,
    "verificationStatus": CODEABLE,
    "category": ("array", CODEABLE),
    "severity": CODEABLE,
    "code": CODEABLE,
    "subject": REFERENCE,
    "encounter": REFERENCE,
    "onsetDateTime": "string",
    "abatementDateTime": "string",
    "recordedDate": "string",
}

SPECS: Dict[str, Dict[str, Any]] = {
    "Patient": PATIENT,
    "Encounter": ENCOUNTER,
    "Observation": OBSERVATION,
    "Condition": CONDITION,
}


# ------------------------------------------------------------------ spec -> DDL
def _ddl_type(t: Any) -> str:
    if isinstance(t, str):
        return t
    if isinstance(t, tuple):
        return f"array<{_ddl_type(t[1])}>"
    fields = ",".join(f"`{name}`:{_ddl_type(sub)}" for name, sub in t.items())
    return f"struct<{fields}>"


def spec_to_ddl(resource: str) -> str:
    """Spark DDL string for from_json, e.g. struct<`id`:string,...>"""
    return _ddl_type(SPECS[resource])


# ------------------------------------------------------------------ XML -> dict
def _local(tag: str) -> str:
    return tag.split("}")[-1]


def _cast(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "boolean":
        return str(value).lower() == "true"
    if kind == "double":
        try:
            return float(value)
        except ValueError:
            return None
    return value


def _convert(elem: ET.Element, t: Any) -> Any:
    if isinstance(t, dict):
        return element_to_dict(elem, t)
    return _cast(elem.get("value"), t)


def element_to_dict(elem: ET.Element, spec: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for name, t in spec.items():
        if name == "resourceType":
            continue
        is_list = isinstance(t, tuple)
        inner = t[1] if is_list else t
        kids = [c for c in elem if _local(c.tag) == name]
        if not kids:
            continue
        values = [_convert(k, inner) for k in kids]
        out[name] = values if is_list else values[0]
    return out


def xml_resource_to_dict(resource_elem: ET.Element) -> Dict[str, Any]:
    """Turn <Patient>...</Patient> into the same dict a JSON payload would give."""
    rtype = _local(resource_elem.tag)
    spec = SPECS.get(rtype)
    if spec is None:
        raise KeyError(f"No spec defined for resource type {rtype}")
    data = {"resourceType": rtype}
    data.update(element_to_dict(resource_elem, spec))
    return data
