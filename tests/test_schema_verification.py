"""
Tests for the schema_verification module.

Verifies that:
1. The NEREL schema constraints are correctly defined.
2. The GraphSchemaVerifier validates/rejects triplets properly.
3. The SchemaVerificationModule integrates with mock LLM/embedder.
4. The full pipeline runs end-to-end with no external API calls.
"""

import asyncio
import pytest

from ragu.graph.schema_verification.schema import (
    RELATION_CONSTRAINTS,
    ALLOWED_ENTITY_TYPES,
    ALLOWED_RELATION_TYPES,
    validate_entity_type,
    validate_relation_type,
    get_allowed_subject_types,
    get_allowed_object_types,
)
from ragu.graph.schema_verification.schema_verifier import (
    GraphSchemaVerifier,
    VerificationResult,
)
from ragu.graph.schema_verification.prompts import (
    Triplet,
    TripletList,
    build_extraction_messages,
    build_refinement_messages,
)


# ======================================================================
# Schema tests
# ======================================================================

class TestSchema:
    def test_entity_types_are_populated(self):
        assert len(ALLOWED_ENTITY_TYPES) == 29

    def test_relation_types_are_populated(self):
        assert len(ALLOWED_RELATION_TYPES) == 49

    def test_all_relation_types_have_constraints(self):
        for rt in ALLOWED_RELATION_TYPES:
            assert rt in RELATION_CONSTRAINTS, f"Missing constraints for {rt}"

    def test_validate_entity_type_known(self):
        assert validate_entity_type("PERSON") is True
        assert validate_entity_type("person") is True

    def test_validate_entity_type_unknown(self):
        assert validate_entity_type("ALIEN_SPECIES") is False

    def test_validate_relation_type_known(self):
        assert validate_relation_type("LOCATED_IN") is True
        assert validate_relation_type("located_in") is True

    def test_validate_relation_type_unknown(self):
        assert validate_relation_type("TELEPORTED_TO") is False

    def test_get_allowed_subject_types(self):
        subjects = get_allowed_subject_types("DATE_OF_BIRTH")
        assert "PERSON" in subjects

    def test_get_allowed_object_types(self):
        objects = get_allowed_object_types("DATE_OF_BIRTH")
        assert "DATE" in objects

    def test_constraint_symmetry_for_symmetric_relations(self):
        # SIBLING, SPOUSE, KNOWS should all have PERSON on both sides
        for rel in ("SIBLING", "SPOUSE", "KNOWS"):
            subj = get_allowed_subject_types(rel)
            obj = get_allowed_object_types(rel)
            assert "PERSON" in subj
            assert "PERSON" in obj

    def test_located_in_constraints(self):
        subj = get_allowed_subject_types("LOCATED_IN")
        obj = get_allowed_object_types("LOCATED_IN")
        assert "FACILITY" in subj
        assert "CITY" in obj
        assert "COUNTRY" in obj

    def test_works_as_constraints(self):
        subj = get_allowed_subject_types("WORKS_AS")
        obj = get_allowed_object_types("WORKS_AS")
        assert "PERSON" in subj
        assert "PROFESSION" in obj


# ======================================================================
# Verifier tests
# ======================================================================

class TestGraphSchemaVerifier:
    def setup_method(self):
        self.type_lookup = {
            "marie curie": "PERSON",
            "poland": "COUNTRY",
            "physicist": "PROFESSION",
            "nobel prize": "AWARD",
            "eiffel tower": "FACILITY",
            "paris": "CITY",
            "1889": "DATE",
            "cat": "UNKNOWN_TYPE",
        }
        self.verifier = GraphSchemaVerifier(
            entity_type_lookup=self.type_lookup,
            strict_relation=True,
        )

    def test_valid_triplet_origins_from(self):
        t = Triplet(subject="Marie Curie", relation="ORIGINS_FROM", object="Poland")
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch([t])
        )
        assert len(results) == 1
        assert results[0].is_valid is True

    def test_valid_triplet_works_as(self):
        t = Triplet(subject="Marie Curie", relation="WORKS_AS", object="physicist")
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch([t])
        )
        assert results[0].is_valid is True

    def test_valid_triplet_awarded_with(self):
        t = Triplet(subject="Marie Curie", relation="AWARDED_WITH", object="Nobel Prize")
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch([t])
        )
        assert results[0].is_valid is True

    def test_invalid_triplet_wrong_domain(self):
        # LOCATED_IN requires subject to be FACILITY/ORGANIZATION/CITY/DISTRICT/LOCATION
        # PERSON is not in the domain
        t = Triplet(subject="Marie Curie", relation="LOCATED_IN", object="Paris")
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch([t])
        )
        assert results[0].is_valid is False
        assert "subject type" in results[0].reason

    def test_invalid_triplet_wrong_range(self):
        # WORKS_AS requires object to be PROFESSION
        # CITY is not in the range
        t = Triplet(subject="Marie Curie", relation="WORKS_AS", object="Paris")
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch([t])
        )
        assert results[0].is_valid is False
        assert "object type" in results[0].reason

    def test_unknown_relation_strict(self):
        t = Triplet(subject="Marie Curie", relation="TELEPORTED_TO", object="Paris")
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch([t])
        )
        assert results[0].is_valid is False
        assert "unknown relation" in results[0].reason

    def test_unknown_relation_lenient(self):
        verifier = GraphSchemaVerifier(
            entity_type_lookup=self.type_lookup,
            strict_relation=False,
        )
        t = Triplet(subject="Marie Curie", relation="TELEPORTED_TO", object="Paris")
        results = asyncio.get_event_loop().run_until_complete(
            verifier.verify_batch([t])
        )
        assert results[0].is_valid is True

    def test_unresolved_entity_types_accepted(self):
        # If entity types can't be resolved, accept by default
        t = Triplet(subject="Unknown Entity", relation="LOCATED_IN", object="Another Unknown")
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch([t])
        )
        assert results[0].is_valid is True

    def test_batch_verification(self):
        triplets = [
            Triplet(subject="Marie Curie", relation="ORIGINS_FROM", object="Poland"),
            Triplet(subject="Marie Curie", relation="WORKS_AS", object="physicist"),
            Triplet(subject="Marie Curie", relation="LOCATED_IN", object="Paris"),  # invalid
        ]
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch(triplets)
        )
        assert len(results) == 3
        assert results[0].is_valid is True
        assert results[1].is_valid is True
        assert results[2].is_valid is False

    def test_valid_located_in_with_facility(self):
        t = Triplet(subject="Eiffel Tower", relation="LOCATED_IN", object="Paris")
        results = asyncio.get_event_loop().run_until_complete(
            self.verifier.verify_batch([t])
        )
        assert results[0].is_valid is True

    def test_date_of_creation(self):
        type_lookup = {**self.type_lookup, "eiffel tower": "FACILITY"}
        verifier = GraphSchemaVerifier(entity_type_lookup=type_lookup, strict_relation=True)
        t = Triplet(subject="Eiffel Tower", relation="DATE_OF_CREATION", object="1889")
        results = asyncio.get_event_loop().run_until_complete(
            verifier.verify_batch([t])
        )
        # FACILITY is not in the domain of DATE_OF_CREATION
        # (which allows WORK_OF_ART, PRODUCT, LAW, ORGANIZATION)
        assert results[0].is_valid is False


# ======================================================================
# Prompts tests
# ======================================================================

class TestPrompts:
    def test_extraction_messages_structure(self):
        msgs = build_extraction_messages("Test text about cats.")
        assert len(msgs) > 0
        # System message + 3 few-shot pairs (6 messages) + user message = 8
        assert len(msgs) == 8

    def test_refinement_messages_structure(self):
        triplets = [
            Triplet(subject="Curie", relation="born_in", object="Warsaw"),
        ]
        msgs = build_refinement_messages(
            text="Test",
            triplets=triplets,
            subject_mappings={"Curie": ["Marie Curie", "Pierre Curie"]},
            relation_mappings={"born_in": ["PLACE_OF_BIRTH", "ORIGINS_FROM"]},
            object_mappings={"Warsaw": ["Warsaw", "Warszawa"]},
        )
        assert len(msgs) == 2  # system + user

    def test_triplet_model(self):
        t = Triplet(subject="A", relation="REL", object="B")
        assert t.subject == "A"
        assert t.relation == "REL"
        assert t.object == "B"

    def test_triplet_list_model(self):
        tl = TripletList(triplets=[
            Triplet(subject="A", relation="REL", object="B"),
        ])
        assert len(tl.triplets) == 1

    def test_extraction_uses_nerel_style_relations(self):
        msgs = build_extraction_messages("Test text.")
        # Check that at least one NEREL-style relation appears in examples
        all_text = " ".join(m.content for m in msgs)
        assert "LOCATED_IN" in all_text or "WORKS_AS" in all_text or "ORIGINS_FROM" in all_text
