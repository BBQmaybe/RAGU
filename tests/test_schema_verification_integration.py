"""
Integration test for SchemaVerificationModule with mock LLM/embedder.

Verifies the full three-stage pipeline (extraction, FAISS retrieval,
refinement, schema verification) runs end-to-end without any external
API calls.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

from ragu.graph.schema_verification.module import SchemaVerificationModule
from ragu.graph.schema_verification.prompts import Triplet, TripletList
from ragu.graph.types import Entity, Relation


# ======================================================================
# Fixtures
# ======================================================================

def _make_mock_embedder(dim: int = 64):
    """Create a mock embedder that returns random embeddings."""
    embedder = MagicMock()

    async def embed(texts):
        return [np.random.randn(dim).astype(np.float32) for _ in texts]

    embedder.embed = embed
    return embedder


def _make_mock_llm():
    """Create a mock LLM that returns predetermined triplets."""
    llm = MagicMock()

    step = {"count": 0}

    async def complete(messages, response_model=None, **kwargs):
        step["count"] += 1
        if step["count"] == 1:
            # Step 1: extraction
            return TripletList(triplets=[
                Triplet(subject="Marie Curie", relation="ORIGINS_FROM", object="Poland"),
                Triplet(subject="Marie Curie", relation="WORKS_AS", object="physicist"),
                Triplet(subject="Marie Curie", relation="AWARDED_WITH", object="Nobel Prize"),
            ])
        else:
            # Step 2: refinement — return same triplets (already canonical)
            return TripletList(triplets=[
                Triplet(subject="Marie Curie", relation="ORIGINS_FROM", object="Poland"),
                Triplet(subject="Marie Curie", relation="WORKS_AS", object="physicist"),
                Triplet(subject="Marie Curie", relation="AWARDED_WITH", object="Nobel Prize"),
            ])

    llm.complete = complete
    return llm


def _make_entities():
    return [
        Entity(
            entity_name="Marie Curie",
            entity_type="PERSON",
            description="Polish-French physicist and chemist",
            source_chunk_id=["c1"],
            documents_id=["d1"],
        ),
        Entity(
            entity_name="Poland",
            entity_type="COUNTRY",
            description="A country in Central Europe",
            source_chunk_id=["c1"],
            documents_id=["d1"],
        ),
        Entity(
            entity_name="physicist",
            entity_type="PROFESSION",
            description="A scientist specializing in physics",
            source_chunk_id=["c1"],
            documents_id=["d1"],
        ),
        Entity(
            entity_name="Nobel Prize",
            entity_type="AWARD",
            description="International award for outstanding achievements",
            source_chunk_id=["c1"],
            documents_id=["d1"],
        ),
    ]


def _make_relations():
    entities = _make_entities()
    return [
        Relation(
            subject_id=entities[0].id,
            object_id=entities[1].id,
            subject_name="Marie Curie",
            object_name="Poland",
            relation_type="ORIGINS_FROM",
            description="Marie Curie originated from Poland",
            source_chunk_id=["c1"],
        ),
    ]


# ======================================================================
# Tests
# ======================================================================

class TestSchemaVerificationModuleIntegration:

    def test_full_pipeline_end_to_end(self):
        """Run all three stages and verify entities/relations come out."""
        llm = _make_mock_llm()
        embedder = _make_mock_embedder()
        entities = _make_entities()
        relations = _make_relations()

        module = SchemaVerificationModule(
            client=llm,
            embedder=embedder,
            top_k=3,
            verify_schema=True,
            strict_relation=True,
        )

        new_entities, new_relations = asyncio.get_event_loop().run_until_complete(
            module.run(entities, relations)
        )

        # Should produce entities and relations
        assert len(new_entities) > 0
        assert len(new_relations) > 0

        # All triplets should pass (PERSON ORIGINS_FROM COUNTRY,
        # PERSON WORKS_AS PROFESSION, PERSON AWARDED_WITH AWARD)
        assert len(new_relations) == 3

        # Check entity names
        names = {e.entity_name for e in new_entities}
        assert "Marie Curie" in names
        assert "Poland" in names
        assert "physicist" in names
        assert "Nobel Prize" in names

    def test_pipeline_with_empty_entities(self):
        """Empty input should return empty output."""
        llm = _make_mock_llm()
        embedder = _make_mock_embedder()

        module = SchemaVerificationModule(
            client=llm, embedder=embedder, top_k=3,
        )

        new_entities, new_relations = asyncio.get_event_loop().run_until_complete(
            module.run([], [])
        )

        assert new_entities == []
        assert new_relations == []

    def test_pipeline_with_verification_disabled(self):
        """When verify_schema=False, all triplets should pass."""
        llm = _make_mock_llm()
        embedder = _make_mock_embedder()
        entities = _make_entities()
        relations = _make_relations()

        module = SchemaVerificationModule(
            client=llm,
            embedder=embedder,
            top_k=3,
            verify_schema=False,
        )

        new_entities, new_relations = asyncio.get_event_loop().run_until_complete(
            module.run(entities, relations)
        )

        assert len(new_relations) == 3

    def test_pipeline_rejects_invalid_triplets(self):
        """Triplets with invalid domain/range should be filtered out."""
        llm = MagicMock()
        step = {"count": 0}

        async def complete(messages, response_model=None, **kwargs):
            step["count"] += 1
            if step["count"] == 1:
                return TripletList(triplets=[
                    # Valid: PERSON WORKS_AS PROFESSION
                    Triplet(subject="Marie Curie", relation="WORKS_AS", object="physicist"),
                    # Invalid: PERSON LOCATED_IN COUNTRY (PERSON not in LOCATED_IN domain)
                    Triplet(subject="Marie Curie", relation="LOCATED_IN", object="Poland"),
                ])
            else:
                return TripletList(triplets=[
                    Triplet(subject="Marie Curie", relation="WORKS_AS", object="physicist"),
                    Triplet(subject="Marie Curie", relation="LOCATED_IN", object="Poland"),
                ])

        llm.complete = complete
        embedder = _make_mock_embedder()
        entities = _make_entities()
        relations = _make_relations()

        module = SchemaVerificationModule(
            client=llm,
            embedder=embedder,
            top_k=3,
            verify_schema=True,
            strict_relation=True,
        )

        new_entities, new_relations = asyncio.get_event_loop().run_until_complete(
            module.run(entities, relations)
        )

        # Only the valid triplet should survive
        assert len(new_relations) == 1
        assert new_relations[0].relation_type == "WORKS_AS"

    def test_module_inherits_entity_metadata(self):
        """Verified entities should inherit metadata from originals."""
        llm = _make_mock_llm()
        embedder = _make_mock_embedder()
        entities = _make_entities()
        relations = _make_relations()

        module = SchemaVerificationModule(
            client=llm, embedder=embedder, top_k=3,
        )

        new_entities, _ = asyncio.get_event_loop().run_until_complete(
            module.run(entities, relations)
        )

        curie = next(e for e in new_entities if e.entity_name == "Marie Curie")
        assert curie.entity_type == "PERSON"
        assert curie.source_chunk_id == ["c1"]
        assert curie.documents_id == ["d1"]
