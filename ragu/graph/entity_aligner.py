"""
Entity alignment at inference time.

Detects duplicate or near-duplicate entities retrieved for a query, verifies
candidate pairs through an LLM, and atomically merges confirmed duplicates
in the knowledge graph.

Pipeline
--------
1. Retrieve top-k entities for a query (done upstream in LocalSearchEngine).
2. Compute pairwise name similarity (string-based, no API calls).
3. Filter candidate pairs whose similarity exceeds ``name_similarity_threshold``.
4. Call the LLM concurrently for each candidate pair to confirm the merge.
5. Apply confirmed merges atomically:
   - Collect all relations attached to both entities.
   - Delete the original entities (cascade-removes their edges).
   - Insert the merged entity.
   - Re-insert relations redirected to the merged entity.
6. Return the updated entity list for use in the search context.

Because duplicate entities are rare relative to the total graph size, LLM
calls are infrequent.  Once merged, future queries automatically retrieve the
aligned entity, so the graph converges without repeated LLM calls.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from difflib import SequenceMatcher
from typing import List, Optional, Tuple

from ragu.common.base import RaguGenerativeModule
from ragu.common.global_parameters import Settings
from ragu.common.prompts.messages import ChatMessages, render
from ragu.common.prompts.prompt_storage import RAGUInstruction
from ragu.common.prompts.default_models import EntityAlignmentModel
from ragu.graph.types import Entity, Relation
from ragu.llm.base_llm import BaseLLM

logger = logging.getLogger(__name__)


class EntityAligner(RaguGenerativeModule):
    """
    Aligns (deduplicates) entities at inference time.

    :param client: LLM client used to verify merge candidates.
    :param knowledge_graph: Knowledge graph whose index will be updated in-place.
    :param name_similarity_threshold: Minimum name similarity score (0–1) to
        consider a pair as a merge candidate.  Default: 0.85.
    :param language: Language hint forwarded to the LLM prompt.
    """

    def __init__(
        self,
        client: BaseLLM,
        knowledge_graph,  # KnowledgeGraph — imported lazily to avoid circular deps
        name_similarity_threshold: float = 0.85,
        language: Optional[str] = None,
    ):
        super().__init__(prompts=["entity_alignment"])
        self.client = client
        self.knowledge_graph = knowledge_graph
        self.name_similarity_threshold = name_similarity_threshold
        self.language = language if language is not None else Settings.language

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def align(self, entities: List[Entity]) -> List[Entity]:
        """
        Find and merge duplicate entities in *entities*.

        Modifies the underlying knowledge graph in-place when merges are
        confirmed.  Returns the updated entity list with merged entities
        replaced by the single canonical entity.

        :param entities: Entities retrieved for the current query.
        :return: Deduplicated entity list.
        """
        candidates = self._find_candidates(entities)
        if not candidates:
            return entities

        logger.debug(
            "Entity alignment: found %d candidate pair(s) among %d entities.",
            len(candidates),
            len(entities),
        )

        # Verify all pairs concurrently via LLM.
        merge_decisions: List[EntityAlignmentModel] = await asyncio.gather(*[
            self._verify_pair(a, b) for a, b in candidates
        ])

        # Apply confirmed merges sequentially to avoid concurrent graph mutations.
        merged_ids: set[str] = set()
        new_entities: List[Entity] = []

        for (entity_a, entity_b), decision in zip(candidates, merge_decisions):
            if not decision.should_merge:
                continue

            # Skip if either entity was already merged in an earlier iteration.
            if entity_a.id in merged_ids or entity_b.id in merged_ids:
                logger.debug(
                    "Skipping pair (%s, %s): one entity already merged.",
                    entity_a.entity_name,
                    entity_b.entity_name,
                )
                continue

            merged = self._build_merged_entity(entity_a, entity_b, decision)
            await self._apply_merge(entity_a, entity_b, merged)

            merged_ids.add(entity_a.id)
            merged_ids.add(entity_b.id)
            new_entities.append(merged)

            logger.info(
                "Merged '%s' + '%s' → '%s'.",
                entity_a.entity_name,
                entity_b.entity_name,
                merged.entity_name,
            )

        if not merged_ids:
            return entities

        surviving = [e for e in entities if e.id not in merged_ids]
        return surviving + new_entities

    # ------------------------------------------------------------------
    # Candidate detection
    # ------------------------------------------------------------------

    def _find_candidates(self, entities: List[Entity]) -> List[Tuple[Entity, Entity]]:
        """
        Return entity pairs whose names are sufficiently similar.

        Only pairs with the **same entity_type** are considered, because
        entities of different types cannot refer to the same real-world object
        (e.g., a PERSON and an ORGANIZATION with a similar name).

        :param entities: Entities to compare pairwise.
        :return: List of candidate pairs sorted by descending similarity.
        """
        candidates: List[Tuple[float, Entity, Entity]] = []

        for i, entity_a in enumerate(entities):
            for entity_b in entities[i + 1:]:
                if entity_a.entity_type != entity_b.entity_type:
                    continue
                similarity = self._name_similarity(
                    entity_a.entity_name, entity_b.entity_name
                )
                if similarity >= self.name_similarity_threshold:
                    candidates.append((similarity, entity_a, entity_b))

        candidates.sort(key=lambda t: t[0], reverse=True)
        return [(a, b) for _, a, b in candidates]

    @staticmethod
    def _name_similarity(name1: str, name2: str) -> float:
        """
        Compute normalised string similarity between two entity names.

        Uses :class:`difflib.SequenceMatcher` (Ratcliff/Obershelp algorithm)
        on lowercased names.  Returns a value in [0, 1].

        :param name1: First entity name.
        :param name2: Second entity name.
        :return: Similarity score in [0, 1].
        """
        return SequenceMatcher(None, name1.casefold(), name2.casefold()).ratio()

    # ------------------------------------------------------------------
    # LLM verification
    # ------------------------------------------------------------------

    async def _verify_pair(
        self, entity_a: Entity, entity_b: Entity
    ) -> EntityAlignmentModel:
        """
        Ask the LLM whether *entity_a* and *entity_b* should be merged.

        :param entity_a: First candidate entity.
        :param entity_b: Second candidate entity.
        :return: Structured LLM decision.
        """
        instruction: RAGUInstruction = self.get_prompt("entity_alignment")
        rendered: List[ChatMessages] = render(
            instruction.messages,
            entity_a=entity_a,
            entity_b=entity_b,
            language=self.language,
        )
        results = await self.client.generate(
            conversations=[rendered[0]],
            response_model=instruction.pydantic_model,
        )
        return results[0]

    # ------------------------------------------------------------------
    # Merge helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_merged_entity(
        entity_a: Entity,
        entity_b: Entity,
        decision: EntityAlignmentModel,
    ) -> Entity:
        """
        Construct the merged :class:`Entity` from the LLM decision.

        Metadata (source chunks, documents, clusters) is combined from both
        originals.

        :param entity_a: First original entity.
        :param entity_b: Second original entity.
        :param decision: LLM merge decision with canonical name/type/description.
        :return: New merged entity (with a freshly generated ID).
        """
        combined_chunks = sorted(set(entity_a.source_chunk_id) | set(entity_b.source_chunk_id))
        combined_docs = sorted(set(entity_a.documents_id) | set(entity_b.documents_id))

        # Merge cluster memberships, deduplicating by (level, cluster_id).
        seen_clusters: set[tuple] = set()
        combined_clusters = []
        for cluster in entity_a.clusters + entity_b.clusters:
            if not isinstance(cluster, dict):
                continue
            key = (cluster.get("level"), cluster.get("cluster_id"))
            if key in seen_clusters:
                continue
            seen_clusters.add(key)
            combined_clusters.append(cluster)

        return Entity(
            entity_name=decision.merged_entity_name or entity_a.entity_name,
            entity_type=decision.merged_entity_type or entity_a.entity_type,
            description=decision.merged_description or entity_a.description,
            source_chunk_id=combined_chunks,
            documents_id=combined_docs,
            clusters=combined_clusters,
        )

    async def _apply_merge(
        self,
        entity_a: Entity,
        entity_b: Entity,
        merged: Entity,
    ) -> None:
        """
        Atomically replace *entity_a* and *entity_b* with *merged* in the graph.

        Steps:
        1. Collect all relations attached to entity_a and entity_b.
        2. Delete entity_a and entity_b (cascades their edges from the graph
           and vector DB).
        3. Insert the merged entity.
        4. Re-insert the collected relations with endpoints redirected to merged.

        Self-loop relations (where both subject and object were among the merged
        entities) are dropped because they no longer carry distinct information.

        :param entity_a: First entity to remove.
        :param entity_b: Second entity to remove.
        :param merged: Replacement entity to insert.
        """
        old_ids = {entity_a.id, entity_b.id}

        # Step 1: collect relations before deletion.
        relations_by_node = await (
            self.knowledge_graph.index.graph_backend.get_all_edges_for_nodes(
                [entity_a.id, entity_b.id]
            )
        )
        seen_relation_ids: set[str] = set()
        unique_relations: List[Relation] = []
        for group in relations_by_node:
            for relation in group:
                if relation is None or relation.id in seen_relation_ids:
                    continue
                seen_relation_ids.add(relation.id)
                unique_relations.append(relation)

        # Step 2: delete original entities (cascade removes their edges).
        await self.knowledge_graph.index.delete_entities([entity_a.id, entity_b.id])

        # Step 3: insert the merged entity.
        await self.knowledge_graph.index.insert_entities([merged])

        # Step 4: redirect and re-insert relations.
        redirected: List[Relation] = []
        for rel in unique_relations:
            new_subject_id = merged.id if rel.subject_id in old_ids else rel.subject_id
            new_object_id = merged.id if rel.object_id in old_ids else rel.object_id

            # Drop self-loops that arise purely from the merge.
            if new_subject_id == new_object_id == merged.id:
                continue

            new_subject_name = (
                merged.entity_name if rel.subject_id in old_ids else rel.subject_name
            )
            new_object_name = (
                merged.entity_name if rel.object_id in old_ids else rel.object_name
            )

            redirected.append(
                Relation(
                    subject_id=new_subject_id,
                    object_id=new_object_id,
                    subject_name=new_subject_name,
                    object_name=new_object_name,
                    relation_type=rel.relation_type,
                    description=rel.description,
                    relation_strength=rel.relation_strength,
                    source_chunk_id=rel.source_chunk_id,
                )
            )

        if redirected:
            await self.knowledge_graph.index.insert_relations(redirected)
