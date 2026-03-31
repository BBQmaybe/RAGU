"""Tests for EntityAligner."""

import json
from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from ragu.graph.entity_aligner import EntityAlignmentModel, EntityAligner
from ragu.graph.types import Entity, Relation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _entity(name: str, etype: str, eid: str, desc: str = "") -> Entity:
    return Entity(
        id=eid,
        entity_name=name,
        entity_type=etype,
        description=desc or f"{name} description",
        source_chunk_id=["chunk-1"],
    )


def _relation(
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


def _merge_result(
    name: str, etype: str, desc: str, should_merge: bool = True
) -> EntityAlignmentModel:
    return EntityAlignmentModel(
        should_merge=should_merge,
        merged_entity_name=name,
        merged_entity_type=etype,
        merged_description=desc,
    )


@pytest.fixture
def mock_llm() -> MagicMock:
    llm = MagicMock()
    llm.chat_completion = AsyncMock()
    return llm


@pytest.fixture
def aligner(mock_llm) -> EntityAligner:
    return EntityAligner(llm=mock_llm, threshold=0.85, enabled=True)


# ---------------------------------------------------------------------------
# EntityAligner._find_candidates
# ---------------------------------------------------------------------------

def test_find_candidates_detects_similar_names(aligner):
    e1 = _entity("Ivan Ivanov", "PERSON", "e1")
    e2 = _entity("Ivan Ivanoff", "PERSON", "e2")
    pairs = aligner._find_candidates([e1, e2])
    assert len(pairs) == 1
    assert pairs[0] == (e1, e2)


def test_find_candidates_ignores_different_types(aligner):
    e1 = _entity("Moscow", "CITY", "e1")
    e2 = _entity("Moscow", "COUNTRY", "e2")
    pairs = aligner._find_candidates([e1, e2])
    assert pairs == []


def test_find_candidates_ignores_low_similarity(aligner):
    e1 = _entity("Alice", "PERSON", "e1")
    e2 = _entity("Robert", "PERSON", "e2")
    pairs = aligner._find_candidates([e1, e2])
    assert pairs == []


def test_find_candidates_exact_match_is_included(aligner):
    e1 = _entity("Chekhov", "PERSON", "e1")
    e2 = _entity("Chekhov", "PERSON", "e2")
    pairs = aligner._find_candidates([e1, e2])
    assert len(pairs) == 1


def test_find_candidates_returns_no_pairs_for_single_entity(aligner):
    e1 = _entity("Solo", "PERSON", "e1")
    assert aligner._find_candidates([e1]) == []


def test_find_candidates_custom_threshold():
    llm = MagicMock()
    low_aligner = EntityAligner(llm=llm, threshold=0.5, enabled=True)
    e1 = _entity("Anton", "PERSON", "e1")
    e2 = _entity("Antony", "PERSON", "e2")
    assert len(low_aligner._find_candidates([e1, e2])) == 1

    high_aligner = EntityAligner(llm=llm, threshold=0.99, enabled=True)
    assert high_aligner._find_candidates([e1, e2]) == []


# ---------------------------------------------------------------------------
# EntityAligner._verify_pair
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_verify_pair_returns_llm_result(aligner, mock_llm):
    e1 = _entity("A. Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhov", "PERSON", "e2")
    expected = _merge_result("Anton Chekhov", "PERSON", "Russian playwright")
    mock_llm.chat_completion.return_value = expected

    result = await aligner._verify_pair(e1, e2)

    assert result.should_merge is True
    assert result.merged_entity_name == "Anton Chekhov"
    mock_llm.chat_completion.assert_called_once()


@pytest.mark.asyncio
async def test_verify_pair_returns_no_merge_on_llm_error(aligner, mock_llm):
    e1 = _entity("A. Chekhov", "PERSON", "e1", desc="playwright")
    e2 = _entity("Anton Chekhov", "PERSON", "e2")
    mock_llm.chat_completion.side_effect = RuntimeError("timeout")

    result = await aligner._verify_pair(e1, e2)

    assert result.should_merge is False
    assert result.merged_entity_name == e1.entity_name
    assert result.merged_description == e1.description


# ---------------------------------------------------------------------------
# EntityAligner._apply_merge
# ---------------------------------------------------------------------------

def test_apply_merge_removes_old_entities(aligner):
    e1 = _entity("A. Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhov", "PERSON", "e2")
    result = _merge_result("Anton Chekhov", "PERSON", "Russian writer")

    new_ents, _ = aligner._apply_merge(e1, e2, result, [e1, e2], [])

    names = {e.entity_name for e in new_ents}
    assert "A. Chekhov" not in names
    assert "Anton Chekhov" in names
    assert len(new_ents) == 1


def test_apply_merge_redirects_edges(aligner):
    e1 = _entity("A. Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhov", "PERSON", "e2")
    city = _entity("Moscow", "CITY", "e3")
    rel1 = _relation(e1, city, "PLACE_OF_BIRTH", "r1")
    rel2 = _relation(e2, city, "PLACE_RESIDES_IN", "r2")
    result = _merge_result("Anton Chekhov", "PERSON", "Russian writer")

    new_ents, new_rels = aligner._apply_merge(
        e1, e2, result, [e1, e2, city], [rel1, rel2]
    )

    merged = next(e for e in new_ents if e.entity_name == "Anton Chekhov")
    assert all(r.subject_id == merged.id for r in new_rels)
    assert all(r.subject_name == "Anton Chekhov" for r in new_rels)
    assert len(new_rels) == 2


def test_apply_merge_discards_self_loops(aligner):
    # e1 and e2 have an edge between themselves — becomes self-loop after merge
    e1 = _entity("A. Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhov", "PERSON", "e2")
    self_loop = _relation(e1, e2, "SIBLING", "r1")
    result = _merge_result("Anton Chekhov", "PERSON", "Russian writer")

    _, new_rels = aligner._apply_merge(e1, e2, result, [e1, e2], [self_loop])

    assert new_rels == []


def test_apply_merge_preserves_unrelated_edges(aligner):
    e1 = _entity("A. Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhov", "PERSON", "e2")
    city = _entity("Moscow", "CITY", "e3")
    other1 = _entity("Tolstoy", "PERSON", "e4")
    unrelated = _relation(city, other1, "LOCATED_IN", "r99")
    result = _merge_result("Anton Chekhov", "PERSON", "Russian writer")

    _, new_rels = aligner._apply_merge(
        e1, e2, result, [e1, e2, city, other1], [unrelated]
    )

    assert len(new_rels) == 1
    assert new_rels[0].subject_id == city.id


def test_apply_merge_merges_source_chunk_ids(aligner):
    e1 = _entity("A. Chekhov", "PERSON", "e1")
    e1.source_chunk_id = ["chunk-1", "chunk-2"]
    e2 = _entity("Anton Chekhov", "PERSON", "e2")
    e2.source_chunk_id = ["chunk-2", "chunk-3"]
    result = _merge_result("Anton Chekhov", "PERSON", "Russian writer")

    new_ents, _ = aligner._apply_merge(e1, e2, result, [e1, e2], [])
    merged = new_ents[0]

    assert set(merged.source_chunk_id) == {"chunk-1", "chunk-2", "chunk-3"}


# ---------------------------------------------------------------------------
# EntityAligner.run — disabled
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_disabled_returns_input_unchanged(mock_llm):
    aligner = EntityAligner(llm=mock_llm, enabled=False)
    e1 = _entity("A. Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhov", "PERSON", "e2")

    ents, rels = await aligner.run([e1, e2], [])

    assert ents == [e1, e2]
    mock_llm.chat_completion.assert_not_called()


@pytest.mark.asyncio
async def test_run_single_entity_returns_unchanged(aligner, mock_llm):
    e1 = _entity("Solo", "PERSON", "e1")
    ents, rels = await aligner.run([e1], [])
    assert ents == [e1]
    mock_llm.chat_completion.assert_not_called()


# ---------------------------------------------------------------------------
# EntityAligner.run — full pipeline
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_run_merges_confirmed_pair(aligner, mock_llm):
    e1 = _entity("Anton Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhoff", "PERSON", "e2")
    city = _entity("Moscow", "CITY", "e3")
    rel = _relation(e1, city, "PLACE_OF_BIRTH", "r1")

    mock_llm.chat_completion.return_value = _merge_result(
        "Anton Chekhov", "PERSON", "Russian writer"
    )

    ents, rels = await aligner.run([e1, e2, city], [rel])

    entity_names = {e.entity_name for e in ents}
    assert "Anton Chekhoff" not in entity_names
    assert "Anton Chekhov" in entity_names
    assert "Moscow" in entity_names
    assert len(rels) == 1


@pytest.mark.asyncio
async def test_run_skips_unconfirmed_pair(aligner, mock_llm):
    e1 = _entity("A. Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhov", "PERSON", "e2")

    mock_llm.chat_completion.return_value = _merge_result(
        "A. Chekhov", "PERSON", "", should_merge=False
    )

    ents, rels = await aligner.run([e1, e2], [])

    assert {e.entity_name for e in ents} == {"A. Chekhov", "Anton Chekhov"}


@pytest.mark.asyncio
async def test_run_skips_already_merged_entity(aligner, mock_llm):
    # Three entities where A~B and B~C (by name); A+B merged first, B+C skipped
    e1 = _entity("Anton Chekhov", "PERSON", "e1")
    e2 = _entity("Anton Chekhof", "PERSON", "e2")
    e3 = _entity("Anton Chekoff", "PERSON", "e3")

    merge_ab = _merge_result("Anton Chekhov", "PERSON", "Russian writer")
    no_merge = _merge_result("Anton Chekhov", "PERSON", "", should_merge=False)

    # LLM called for (e1,e2) → merge; (e1,e3) and (e2,e3) → e2 already gone
    mock_llm.chat_completion.side_effect = [merge_ab, merge_ab, no_merge]

    ents, _ = await aligner.run([e1, e2, e3], [])

    # e1+e2 merged into one; e3 may or may not merge depending on ordering
    # The key invariant: no entity id from merged_ids appears in the final list
    final_ids = {e.id for e in ents}
    assert "e1" not in final_ids
    assert "e2" not in final_ids
