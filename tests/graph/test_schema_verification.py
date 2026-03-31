"""Tests for SchemaVerificationModule."""

import json
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import pytest

from ragu.graph.schema_verification import (
    RawTriplet,
    SchemaVerificationModule,
    _CandidateRetriever,
    _RELATION_TYPE_CONSTRAINTS,
)
from ragu.graph.types import Entity, Relation


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_entity(name: str, etype: str, eid: str) -> Entity:
    return Entity(
        id=eid,
        entity_name=name,
        entity_type=etype,
        description=f"{name} description",
        source_chunk_id=["chunk-1"],
    )


def _make_relation(
    subj: Entity, obj: Entity, rtype: str, rid: str
) -> Relation:
    return Relation(
        id=rid,
        subject_id=subj.id,
        object_id=obj.id,
        subject_name=subj.entity_name,
        object_name=obj.entity_name,
        relation_type=rtype,
        description=f"{subj.entity_name} {rtype} {obj.entity_name}",
    )


@pytest.fixture
def alice() -> Entity:
    return _make_entity("Alice", "PERSON", "ent-1")


@pytest.fixture
def berlin() -> Entity:
    return _make_entity("Berlin", "CITY", "ent-2")


@pytest.fixture
def alice_born_berlin(alice, berlin) -> Relation:
    return _make_relation(alice, berlin, "PLACE_OF_BIRTH", "rel-1")


@pytest.fixture
def mock_llm() -> MagicMock:
    llm = MagicMock()
    llm.chat_completion = AsyncMock()
    return llm


@pytest.fixture
def mock_embedder() -> MagicMock:
    dim = 8

    async def embed_text(text: str, **_: Any) -> List[float]:
        rng = np.random.default_rng(abs(hash(text)) % (2 ** 32))
        return rng.random(dim).tolist()

    async def batch_embed_text(
        texts: List[str], **_: Any
    ) -> List[List[float]]:
        return [await embed_text(t) for t in texts]

    emb = MagicMock()
    emb.dim = dim
    emb.embed_text = AsyncMock(side_effect=embed_text)
    emb.batch_embed_text = AsyncMock(side_effect=batch_embed_text)
    return emb


@pytest.fixture
def module(mock_llm, mock_embedder) -> SchemaVerificationModule:
    return SchemaVerificationModule(
        llm=mock_llm,
        embedder=mock_embedder,
        top_k=3,
        batch_size=10,
        enabled=True,
    )


# ---------------------------------------------------------------------------
# _CandidateRetriever
# ---------------------------------------------------------------------------

def test_candidate_retriever_returns_top_k():
    rng = np.random.default_rng(0)
    corpus = ["apple", "banana", "cherry", "date", "elderberry"]
    vecs = rng.random((5, 4)).astype(np.float32)
    retriever = _CandidateRetriever(corpus, vecs)

    results = retriever.top_k(vecs[0], k=3)

    assert len(results) == 3
    assert results[0] == "apple"


def test_candidate_retriever_respects_k_limit():
    corpus = ["a", "b"]
    vecs = np.eye(2, dtype=np.float32)
    retriever = _CandidateRetriever(corpus, vecs)

    results = retriever.top_k(vecs[0], k=5)
    assert len(results) == 2


def test_candidate_retriever_empty_corpus():
    retriever = _CandidateRetriever([], np.zeros((0, 4), dtype=np.float32))
    assert retriever.top_k(np.zeros(4), k=5) == []


# ---------------------------------------------------------------------------
# SchemaVerificationModule.run — disabled
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_module_disabled_returns_input_unchanged(alice, berlin, alice_born_berlin):
    llm = MagicMock()
    emb = MagicMock()
    module = SchemaVerificationModule(llm=llm, embedder=emb, enabled=False)

    ents, rels = await module.run([alice, berlin], [alice_born_berlin])
    assert ents == [alice, berlin]
    assert rels == [alice_born_berlin]


@pytest.mark.asyncio
async def test_module_empty_graph_returns_empty(module):
    ents, rels = await module.run([], [])
    assert ents == []
    assert rels == []


# ---------------------------------------------------------------------------
# SchemaVerificationModule._parse_triplet_json
# ---------------------------------------------------------------------------

def test_parse_triplet_json_valid():
    text = '[{"subject": "Alice", "relation": "KNOWS", "object": "Bob"}]'
    result = SchemaVerificationModule._parse_triplet_json(text)
    assert len(result) == 1
    assert result[0]["subject"] == "Alice"


def test_parse_triplet_json_with_markdown_fence():
    text = '```json\n[{"subject": "X", "relation": "R", "object": "Y"}]\n```'
    result = SchemaVerificationModule._parse_triplet_json(text)
    assert len(result) == 1


def test_parse_triplet_json_empty_array():
    assert SchemaVerificationModule._parse_triplet_json("[]") == []


def test_parse_triplet_json_no_json():
    assert SchemaVerificationModule._parse_triplet_json("No JSON here") == []


def test_parse_triplet_json_skips_incomplete_items():
    text = '[{"subject": "A", "relation": "R"}, {"subject": "X", "relation": "R2", "object": "Y"}]'
    result = SchemaVerificationModule._parse_triplet_json(text)
    assert len(result) == 1
    assert result[0]["object"] == "Y"


# ---------------------------------------------------------------------------
# SchemaVerificationModule._verify_triplets
# ---------------------------------------------------------------------------

def test_verify_triplets_unknown_relation_keeps_triplet(module, alice, berlin):
    triplets = [RawTriplet(subject="Alice", relation="CUSTOM_REL", object="Berlin")]
    verified = module._verify_triplets(triplets, [alice, berlin])
    assert len(verified) == 1


def test_verify_triplets_passes_matching_types(module, alice, berlin):
    # PLACE_OF_BIRTH: PERSON -> CITY ✓
    triplets = [RawTriplet(subject="Alice", relation="PLACE_OF_BIRTH", object="Berlin")]
    verified = module._verify_triplets(triplets, [alice, berlin])
    assert len(verified) == 1


def test_verify_triplets_filters_wrong_subject_type(module, alice, berlin):
    # FOUNDED_BY expects ORGANIZATION as subject; Alice is PERSON → filtered
    triplets = [RawTriplet(subject="Alice", relation="FOUNDED_BY", object="Alice")]
    bob = _make_entity("Bob", "PERSON", "ent-99")
    verified = module._verify_triplets(triplets, [alice, bob])
    assert len(verified) == 0


def test_verify_triplets_filters_wrong_object_type(module, alice, berlin):
    # SPOUSE expects both PERSON; Berlin is CITY → filtered
    triplets = [RawTriplet(subject="Alice", relation="SPOUSE", object="Berlin")]
    verified = module._verify_triplets(triplets, [alice, berlin])
    assert len(verified) == 0


def test_verify_triplets_passes_when_both_types_match(module, alice):
    bob = _make_entity("Bob", "PERSON", "ent-99")
    triplets = [RawTriplet(subject="Alice", relation="SPOUSE", object="Bob")]
    verified = module._verify_triplets(triplets, [alice, bob])
    assert len(verified) == 1


def test_verify_triplets_empty_constraint_side_accepts_any_type(module, alice, berlin):
    # PARTICIPANT_IN: subject side is empty set (any type) → Berlin (CITY) as subject is ok
    triplets = [RawTriplet(subject="Berlin", relation="PARTICIPANT_IN", object="Berlin")]
    event = _make_entity("Berlin", "EVENT", "ent-ev")
    # object must be EVENT
    triplets2 = [RawTriplet(subject="Alice", relation="PARTICIPANT_IN", object="Berlin")]
    verified = module._verify_triplets(triplets2, [alice, event])
    assert len(verified) == 1


def test_verify_triplets_unknown_entity_type_passes(module):
    # Entity not in list → type resolves to "" → empty constraint side → keep
    ghost = _make_entity("Ghost", "UNKNOWN_TYPE", "ent-g")
    triplets = [RawTriplet(subject="Nobody", relation="SPOUSE", object="Ghost")]
    verified = module._verify_triplets(triplets, [ghost])
    # "Nobody" not in entity list → subj_type="" → allowed_subj={"PERSON"} → subj_ok=False → filtered
    assert len(verified) == 0


# ---------------------------------------------------------------------------
# SchemaVerificationModule._rebuild_graph
# ---------------------------------------------------------------------------

def test_rebuild_graph_matches_by_name_and_type(module, alice, berlin, alice_born_berlin):
    verified = [RawTriplet(subject="alice", relation="PLACE_OF_BIRTH", object="berlin")]
    ents, rels = module._rebuild_graph(verified, [alice, berlin], [alice_born_berlin])
    assert len(rels) == 1
    assert rels[0].id == "rel-1"
    assert {e.id for e in ents} == {"ent-1", "ent-2"}


def test_rebuild_graph_unknown_triplet_is_skipped(module, alice, berlin, alice_born_berlin):
    verified = [RawTriplet(subject="nobody", relation="PLACE_OF_BIRTH", object="nowhere")]
    ents, rels = module._rebuild_graph(verified, [alice, berlin], [alice_born_berlin])
    assert ents == []
    assert rels == []


# ---------------------------------------------------------------------------
# SchemaVerificationModule full run (mocked LLM)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_full_run_keeps_verified_triplets(
    module, mock_llm, alice, berlin, alice_born_berlin
):
    # Step 1 returns a triplet matching alice_born_berlin
    extraction_output = json.dumps(
        [{"subject": "Alice", "relation": "PLACE_OF_BIRTH", "object": "Berlin"}]
    )
    # Step 2 returns the same triplet
    refinement_output = json.dumps(
        [{"subject": "Alice", "relation": "PLACE_OF_BIRTH", "object": "Berlin"}]
    )
    mock_llm.chat_completion.side_effect = [extraction_output, refinement_output]

    ents, rels = await module.run([alice, berlin], [alice_born_berlin])

    assert len(rels) == 1
    assert rels[0].id == "rel-1"
    assert {e.id for e in ents} == {"ent-1", "ent-2"}


@pytest.mark.asyncio
async def test_full_run_filters_type_incompatible_triplet(
    module, mock_llm, alice, berlin, alice_born_berlin
):
    # LLM returns SPOUSE for Alice→Berlin; Berlin is CITY, not PERSON → filtered
    extraction_output = json.dumps(
        [{"subject": "Alice", "relation": "SPOUSE", "object": "Berlin"}]
    )
    refinement_output = json.dumps(
        [{"subject": "Alice", "relation": "SPOUSE", "object": "Berlin"}]
    )
    mock_llm.chat_completion.side_effect = [extraction_output, refinement_output]

    ents, rels = await module.run([alice, berlin], [alice_born_berlin])

    assert rels == []
    assert ents == []
