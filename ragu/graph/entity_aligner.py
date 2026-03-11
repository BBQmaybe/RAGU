"""
LLM-verified entity alignment for knowledge graphs.

Pipeline per request:
  1. **Candidate search** — pairwise comparison of entity names via
     :func:`difflib.SequenceMatcher` (zero API calls). Only pairs that
     share the same ``entity_type`` are considered. Default similarity
     threshold is 0.85.
  2. **LLM verification** — all candidate pairs are sent to the LLM in
     parallel via :meth:`BaseLLM.generate`. The model returns an
     :class:`EntityAlignmentModel` indicating whether the pair should be
     merged and, if so, the canonical name/type/description.
  3. **Atomic graph update** — for each confirmed merge the module
     collects incident edges, removes both old entities (cascade-deleting
     their edges), inserts the new merged entity, and redirects the
     collected edges to it (self-loops are discarded).
"""

from __future__ import annotations

import difflib
from dataclasses import dataclass
from itertools import combinations
from typing import List, Optional

from pydantic import BaseModel, Field

from ragu.common.logger import logger
from ragu.common.prompts.messages import ChatMessages, SystemMessage, UserMessage
from ragu.graph.types import Entity, Relation
from ragu.llm.base_llm import BaseLLM
from ragu.storage.index import Index


# ---------------------------------------------------------------------------
# Pydantic response model for structured LLM output
# ---------------------------------------------------------------------------

class EntityAlignmentModel(BaseModel):
    """LLM decision on whether two entities should be merged."""

    should_merge: bool = Field(
        ...,
        description="True if two entities refer to the same real-world object",
    )
    merged_entity_name: str = Field(
        "",
        description="Canonical name for the merged entity (empty when should_merge=False)",
    )
    merged_entity_type: str = Field(
        "",
        description="Entity type for the merged entity (empty when should_merge=False)",
    )
    merged_description: str = Field(
        "",
        description="Merged description combining information from both entities "
        "(empty when should_merge=False)",
    )


# ---------------------------------------------------------------------------
# Prompt template
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = (
    "You are an expert in entity resolution for knowledge graphs. "
    "Given two entity descriptions, decide whether they refer to the "
    "same real-world object. If they do, produce a single canonical "
    "name, type, and merged description."
)

_USER_PROMPT_TEMPLATE = (
    "Entity A:\n"
    "  name: {{ entity_a.entity_name }}\n"
    "  type: {{ entity_a.entity_type }}\n"
    "  description: {{ entity_a.description }}\n\n"
    "Entity B:\n"
    "  name: {{ entity_b.entity_name }}\n"
    "  type: {{ entity_b.entity_type }}\n"
    "  description: {{ entity_b.description }}\n\n"
    "Should these two entities be merged into one? "
    "If yes, provide the merged name, type and description. "
    "Answer in the language of the entities."
)


# ---------------------------------------------------------------------------
# EntityAligner
# ---------------------------------------------------------------------------

@dataclass
class EntityAligner:
    """LLM-verified entity alignment and merging.

    :param client: LLM client used for verification.
    :param index: :class:`Index` instance that owns the graph and vector stores.
    :param similarity_threshold: Minimum string-similarity ratio for a pair
        to be considered a merge candidate (default ``0.85``).
    """

    client: BaseLLM
    index: Index
    similarity_threshold: float = 0.85

    # ---- public API -------------------------------------------------------

    async def run(self, entities: Optional[List[Entity]] = None) -> List[Entity]:
        """Execute the full alignment pipeline.

        :param entities: Entities to consider. When *None* all entities
            currently stored in ``self.index`` are used.
        :return: List of newly created merged entities (may be empty).
        """
        if entities is None:
            entities = await self.index.graph_backend.get_all_nodes()

        if len(entities) < 2:
            return []

        candidates = self._find_candidates(entities)
        if not candidates:
            logger.info("EntityAligner: no merge candidates found.")
            return []

        logger.info(f"EntityAligner: {len(candidates)} candidate pair(s) to verify.")

        verified = await self._verify_pairs(candidates)

        merged_entities: List[Entity] = []
        for (entity_a, entity_b), decision in zip(candidates, verified):
            if decision is None or not decision.should_merge:
                continue
            merged = await self._apply_merge(entity_a, entity_b, decision)
            merged_entities.append(merged)

        logger.info(f"EntityAligner: {len(merged_entities)} merge(s) applied.")
        return merged_entities

    # ---- step 1: candidate search -----------------------------------------

    def _find_candidates(
        self,
        entities: List[Entity],
    ) -> List[tuple[Entity, Entity]]:
        """Return pairs whose names are similar and types match."""
        candidates: List[tuple[Entity, Entity]] = []
        for a, b in combinations(entities, 2):
            if a.entity_type != b.entity_type:
                continue
            ratio = difflib.SequenceMatcher(
                None,
                a.entity_name.lower(),
                b.entity_name.lower(),
            ).ratio()
            if ratio >= self.similarity_threshold:
                candidates.append((a, b))
        return candidates

    # ---- step 2: LLM verification -----------------------------------------

    async def _verify_pairs(
        self,
        candidates: List[tuple[Entity, Entity]],
    ) -> List[Optional[EntityAlignmentModel]]:
        """Send all candidate pairs to the LLM in parallel."""
        from ragu.common.prompts.messages import render

        template = ChatMessages.from_messages([
            SystemMessage(content=_SYSTEM_PROMPT),
            UserMessage(content=_USER_PROMPT_TEMPLATE),
        ])

        conversations: List[ChatMessages] = render(
            template,
            entity_a=[a for a, _ in candidates],
            entity_b=[b for _, b in candidates],
        )

        results: List[Optional[EntityAlignmentModel]] = await self.client.generate(
            conversations=conversations,
            response_model=EntityAlignmentModel,
            progress_bar_desc="Entity alignment verification",
        )
        return results

    # ---- step 3: atomic graph update ---------------------------------------

    async def _apply_merge(
        self,
        entity_a: Entity,
        entity_b: Entity,
        decision: EntityAlignmentModel,
    ) -> Entity:
        """Merge *entity_a* and *entity_b* into a single entity in the index.

        Steps:
          1. Collect all incident edges of both entities.
          2. Delete both old entities (cascading edge removal).
          3. Insert the new merged entity.
          4. Redirect collected edges to the new entity (drop self-loops).
        """
        # 1. collect edges before deletion
        edges_grouped = await self.index.graph_backend.get_all_edges_for_nodes(
            [entity_a.id, entity_b.id],
        )
        all_edges: List[Relation] = []
        for group in edges_grouped:
            all_edges.extend(group)

        # deduplicate edges by id
        seen_ids: set[str] = set()
        unique_edges: List[Relation] = []
        for edge in all_edges:
            if edge.id not in seen_ids:
                seen_ids.add(edge.id)
                unique_edges.append(edge)

        # 2. delete old entities (cascade removes their edges)
        await self.index.delete_entities([entity_a.id, entity_b.id])

        # 3. build and insert merged entity
        all_chunks = sorted(
            set(entity_a.source_chunk_id) | set(entity_b.source_chunk_id)
        )
        all_docs = sorted(
            set(entity_a.documents_id) | set(entity_b.documents_id)
        )

        merged_entity = Entity(
            entity_name=decision.merged_entity_name,
            entity_type=decision.merged_entity_type,
            description=decision.merged_description,
            source_chunk_id=all_chunks,
            documents_id=all_docs,
        )

        await self.index.insert_entities([merged_entity])

        # 4. redirect edges, dropping self-loops
        old_ids = {entity_a.id, entity_b.id}
        redirected: List[Relation] = []
        for edge in unique_edges:
            subj = merged_entity.id if edge.subject_id in old_ids else edge.subject_id
            obj = merged_entity.id if edge.object_id in old_ids else edge.object_id

            if subj == obj:
                continue

            subj_name = (
                merged_entity.entity_name
                if edge.subject_id in old_ids
                else edge.subject_name
            )
            obj_name = (
                merged_entity.entity_name
                if edge.object_id in old_ids
                else edge.object_name
            )

            redirected.append(
                Relation(
                    subject_id=subj,
                    object_id=obj,
                    subject_name=subj_name,
                    object_name=obj_name,
                    relation_type=edge.relation_type,
                    description=edge.description,
                    relation_strength=edge.relation_strength,
                    source_chunk_id=edge.source_chunk_id,
                )
            )

        if redirected:
            await self.index.insert_relations(redirected)

        return merged_entity
