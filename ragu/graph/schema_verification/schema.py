"""
NEREL-based knowledge graph schema with domain/range constraints.

Defines which entity types are valid as subject (domain) and object (range)
for each relation type in the NEREL ontology.  The schema is derived
directly from the NEREL annotation guidelines and requires no external
API calls.

The schema serves two purposes:

1. **Triplet validation** — reject ``(subject, relation, object)`` triples
   whose entity types violate the domain/range constraints.
2. **Candidate normalisation** — provide the canonical list of allowed
   entity and relation type names for the FAISS-based candidate linker.
"""

from __future__ import annotations

from typing import Dict, FrozenSet, Set

from ragu.triplet.types import NEREL_ENTITY_TYPES, NEREL_RELATION_TYPES

# ---------------------------------------------------------------------------
# Helper sets
# ---------------------------------------------------------------------------

_PERSONS: frozenset[str] = frozenset({"PERSON"})
_ORGS: frozenset[str] = frozenset({"ORGANIZATION"})
_PERSON_ORG: frozenset[str] = _PERSONS | _ORGS

_DATES: frozenset[str] = frozenset({"DATE", "TIME"})
_NUMBERS: frozenset[str] = frozenset({"AGE", "NUMBER", "ORDINAL"})
_MONEY: frozenset[str] = frozenset({"MONEY"})

_GEO: frozenset[str] = frozenset({
    "CITY", "COUNTRY", "STATE_OR_PROV", "LOCATION", "DISTRICT",
})
_GEO_FACILITY: frozenset[str] = _GEO | frozenset({"FACILITY"})
_EVENTS: frozenset[str] = frozenset({"EVENT"})

_ANY: frozenset[str] = frozenset(NEREL_ENTITY_TYPES)

# ---------------------------------------------------------------------------
# Domain / Range constraints per relation type
#
# Each entry: relation_type -> (allowed_subject_types, allowed_object_types)
# An empty frozenset means "any type is acceptable" (open constraint).
# ---------------------------------------------------------------------------

RELATION_CONSTRAINTS: Dict[str, tuple[FrozenSet[str], FrozenSet[str]]] = {
    "ABBREVIATION": (_ANY, _ANY),
    "AGE_DIED_AT": (_PERSONS, _NUMBERS),
    "AGE_IS": (_PERSONS | _ORGS | _GEO, _NUMBERS),
    "AGENT": (
        _EVENTS | frozenset({"CRIME"}),
        _PERSON_ORG | _GEO,
    ),
    "ALTERNATIVE_NAME": (_ANY, _ANY),
    "AWARDED_WITH": (_PERSON_ORG, frozenset({"AWARD"})),
    "CAUSE_OF_DEATH": (_PERSONS, frozenset({"DISEASE", "EVENT", "CRIME"})),
    "CONVICTED_OF": (_PERSONS, frozenset({"CRIME"})),
    "DATE_DEFUNCT_IN": (
        _ORGS | frozenset({"FACILITY", "CITY", "COUNTRY", "STATE_OR_PROV"}),
        _DATES,
    ),
    "DATE_FOUNDED_IN": (
        _ORGS | _GEO | frozenset({"FACILITY"}),
        _DATES,
    ),
    "DATE_OF_BIRTH": (_PERSONS, _DATES),
    "DATE_OF_CREATION": (
        frozenset({"WORK_OF_ART", "PRODUCT", "LAW", "ORGANIZATION"}),
        _DATES,
    ),
    "DATE_OF_DEATH": (_PERSONS, _DATES),
    "END_TIME": (_EVENTS | _ORGS, _DATES),
    "EXPENDITURE": (_ORGS | _EVENTS | _PERSONS, _MONEY),
    "FOUNDED_BY": (_ORGS | _GEO | frozenset({"FACILITY"}), _PERSONS | _ORGS),
    "HAS_CAUSE": (
        _EVENTS | frozenset({"DISEASE", "CRIME"}),
        _EVENTS | _PERSON_ORG | frozenset({"DISEASE"}),
    ),
    "HEADQUARTERED_IN": (_ORGS, _GEO_FACILITY),
    "IDEOLOGY_OF": (_PERSON_ORG, frozenset({"IDEOLOGY"})),
    "INANIMATE_INVOLVED": (
        _EVENTS | frozenset({"CRIME"}),
        frozenset({"PRODUCT", "FACILITY", "WORK_OF_ART"}),
    ),
    "INCOME": (_PERSON_ORG, _MONEY),
    "KNOWS": (_PERSONS, _PERSONS),
    "LOCATED_IN": (
        frozenset({"FACILITY", "ORGANIZATION", "CITY", "DISTRICT", "LOCATION"}),
        _GEO_FACILITY,
    ),
    "MEDICAL_CONDITION": (_PERSONS, frozenset({"DISEASE"})),
    "MEMBER_OF": (_PERSONS | _ORGS, _ORGS),
    "ORGANIZES": (_PERSON_ORG, _EVENTS),
    "ORIGINS_FROM": (_PERSONS | frozenset({"FAMILY"}), _GEO),
    "OWNER_OF": (
        _PERSON_ORG,
        _ORGS | frozenset({"FACILITY", "PRODUCT"}),
    ),
    "PARENT_OF": (_PERSONS, _PERSONS),
    "PART_OF": (
        _ORGS | _GEO | _EVENTS | frozenset({"FACILITY", "DISTRICT"}),
        _ORGS | _GEO | _EVENTS | frozenset({"FACILITY", "DISTRICT"}),
    ),
    "PARTICIPANT_IN": (_PERSON_ORG | _GEO, _EVENTS),
    "PENALIZED_AS": (_PERSONS | _ORGS, frozenset({"PENALTY"})),
    "PLACE_OF_BIRTH": (_PERSONS, _GEO_FACILITY),
    "PLACE_OF_DEATH": (_PERSONS, _GEO_FACILITY),
    "PLACE_RESIDES_IN": (_PERSONS, _GEO_FACILITY),
    "POINT_IN_TIME": (_EVENTS, _DATES),
    "PRICE_OF": (frozenset({"PRODUCT", "FACILITY"}), _MONEY),
    "PRODUCES": (_PERSON_ORG, frozenset({"PRODUCT", "WORK_OF_ART"})),
    "RELATIVE": (_PERSONS, _PERSONS | frozenset({"FAMILY"})),
    "RELIGION_OF": (_PERSONS | _ORGS, frozenset({"RELIGION"})),
    "SCHOOLS_ATTENDED": (_PERSONS, _ORGS | frozenset({"FACILITY"})),
    "SIBLING": (_PERSONS, _PERSONS),
    "SPOUSE": (_PERSONS, _PERSONS),
    "START_TIME": (_EVENTS | _ORGS, _DATES),
    "SUBEVENT_OF": (_EVENTS, _EVENTS),
    "SUBORDINATE_OF": (_PERSON_ORG, _PERSON_ORG),
    "TAKES_PLACE_IN": (_EVENTS, _GEO_FACILITY),
    "WORKPLACE": (_PERSONS, _ORGS | frozenset({"FACILITY"})),
    "WORKS_AS": (_PERSONS, frozenset({"PROFESSION"})),
}

# Exported canonical sets (for candidate linking)
ALLOWED_ENTITY_TYPES: Set[str] = set(NEREL_ENTITY_TYPES)
ALLOWED_RELATION_TYPES: Set[str] = set(NEREL_RELATION_TYPES)


def validate_entity_type(entity_type: str) -> bool:
    """Return *True* if *entity_type* belongs to the NEREL ontology."""
    return entity_type.upper() in ALLOWED_ENTITY_TYPES


def validate_relation_type(relation_type: str) -> bool:
    """Return *True* if *relation_type* belongs to the NEREL ontology."""
    return relation_type.upper() in ALLOWED_RELATION_TYPES


def get_allowed_subject_types(relation_type: str) -> frozenset[str]:
    """Return the set of entity types allowed as *subject* for *relation_type*."""
    key = relation_type.upper()
    if key in RELATION_CONSTRAINTS:
        return RELATION_CONSTRAINTS[key][0]
    return frozenset()


def get_allowed_object_types(relation_type: str) -> frozenset[str]:
    """Return the set of entity types allowed as *object* for *relation_type*."""
    key = relation_type.upper()
    if key in RELATION_CONSTRAINTS:
        return RELATION_CONSTRAINTS[key][1]
    return frozenset()
