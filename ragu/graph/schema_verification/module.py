"""
Main pipeline module: ``SchemaVerificationModule``.

This :class:`GraphBuilderModule` implements a three-stage pipeline for
knowledge graph verification using the internal NEREL schema:

    Stage 1 — **Candidate extraction** (LLM, few-shot)
        Extract raw ``(subject, relation, object)`` triplets from the
        context reconstructed from entity/relation descriptions.

    Retrieval — **Candidate linking** (FAISS + embedder)
        For every element of every triplet retrieve top-k canonical
        names from the NEREL vocabulary and existing graph entities
        via the :class:`SchemaAwareCandidateLinker`.

    Stage 2 — **Refinement** (LLM)
        Ask the LLM to select the best canonical label from each top-k
        list, producing refined triplets.

    Stage 3 — **Schema verification** (NEREL constraints)
        Filter out triplets whose subject/object types are incompatible
        with the domain/range constraints defined in the NEREL schema.

Finally the verified triplets are mapped back to RAGU
:class:`Entity` / :class:`Relation` objects so that the rest of the
pipeline (community detection, storage, search) continues transparently.
"""

from __future__ import annotations

import asyncio
import json
from typing import List, Tuple

from ragu.common.logger import logger
from ragu.embedder.base_embedder import BaseEmbedder
from ragu.graph.graph_builder_pipeline import GraphBuilderModule
from ragu.graph.types import Entity, Relation
from ragu.graph.schema_verification.candidate_linker import SchemaAwareCandidateLinker
from ragu.graph.schema_verification.schema_verifier import GraphSchemaVerifier
from ragu.graph.schema_verification.prompts import (
    Triplet,
    TripletList,
    build_extraction_messages,
    build_refinement_messages,
)
from ragu.llm.base_llm import BaseLLM


class SchemaVerificationModule(GraphBuilderModule):
    """
    A :class:`GraphBuilderModule` that normalises extracted entities /
    relations against the NEREL schema and verifies them using internal
    domain/range constraints.

    Plug this module into any RAGU pipeline via
    ``additional_modules=[SchemaVerificationModule(...)]``.

    :param client: An LLM client implementing :class:`BaseLLM`.
    :param embedder: An embedder implementing :class:`BaseEmbedder`.
    :param enabled: When *False* (default), ``run()`` is a no-op and
        returns the input unchanged.  Set to *True* to activate the
        verification pipeline.
    :param top_k: Number of canonical candidates per triplet element.
    :param verify_schema: Whether to run Stage 3 (schema verification).
    :param strict_relation: When *True*, reject triplets with unknown
        relation types.  When *False*, unknown relations pass through.
    """

    def __init__(
        self,
        client: BaseLLM,
        embedder: BaseEmbedder,
        enabled: bool = False,
        top_k: int = 5,
        verify_schema: bool = True,
        strict_relation: bool = True,
    ) -> None:
        super().__init__()
        self.client = client
        self.embedder = embedder
        self.enabled = enabled
        self.top_k = top_k
        self.verify_schema = verify_schema

        self._linker = SchemaAwareCandidateLinker(
            embedder=embedder,
            top_k=top_k,
        )
        self._verifier = GraphSchemaVerifier(
            strict_relation=strict_relation,
        )

    # ==================================================================
    # GraphBuilderModule interface
    # ==================================================================

    async def run(
        self,
        entities: List[Entity],
        relations: List[Relation],
        **kwargs,
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Execute the full three-stage schema verification pipeline.

        :param entities: Entities extracted by the upstream pipeline.
        :param relations: Relations extracted by the upstream pipeline.
        :return: Filtered and normalised (entities, relations).
        """
        if not self.enabled:
            logger.debug(
                "[SchemaVerification] Module disabled (enabled=False); "
                "passing through unchanged"
            )
            return entities, relations

        if not entities:
            return entities, relations

        logger.info(
            f"[SchemaVerification] Starting with {len(entities)} entities, "
            f"{len(relations)} relations"
        )

        # ----------------------------------------------------------
        # 0. Build lookup tables from existing entities
        # ----------------------------------------------------------
        entity_names = [e.entity_name for e in entities]
        self._linker.update_entity_candidates(entity_names)

        type_lookup = {
            e.entity_name.lower(): e.entity_type
            for e in entities
            if e.entity_type
        }
        self._verifier.update_type_lookup(type_lookup)

        # ----------------------------------------------------------
        # 0b. Reconstruct textual context
        # ----------------------------------------------------------
        context = self._reconstruct_context(entities, relations)

        # ----------------------------------------------------------
        # Stage 1 — Candidate extraction (few-shot LLM)
        # ----------------------------------------------------------
        raw_triplets = await self._step1_extract(context)
        if not raw_triplets:
            logger.warning(
                "[SchemaVerification] Step 1 produced no triplets; "
                "returning input unchanged"
            )
            return entities, relations

        logger.info(
            f"[SchemaVerification] Step 1: extracted {len(raw_triplets)} "
            f"raw triplets"
        )

        # ----------------------------------------------------------
        # Retrieval — FAISS top-k canonical candidates
        # ----------------------------------------------------------
        (
            subject_map,
            relation_map,
            object_map,
        ) = await self._retrieval(raw_triplets)

        logger.info("[SchemaVerification] Retrieval: candidate mappings built")

        # ----------------------------------------------------------
        # Stage 2 — Refinement (LLM)
        # ----------------------------------------------------------
        refined_triplets = await self._step2_refine(
            context, raw_triplets, subject_map, relation_map, object_map
        )
        if not refined_triplets:
            logger.warning(
                "[SchemaVerification] Step 2 produced no refined triplets; "
                "returning input unchanged"
            )
            return entities, relations

        logger.info(
            f"[SchemaVerification] Step 2: refined to "
            f"{len(refined_triplets)} triplets"
        )

        # ----------------------------------------------------------
        # Stage 3 — Schema verification
        # ----------------------------------------------------------
        if self.verify_schema:
            verified_triplets = await self._step3_verify(refined_triplets)
            logger.info(
                f"[SchemaVerification] Step 3: {len(verified_triplets)}/"
                f"{len(refined_triplets)} triplets passed verification"
            )
        else:
            verified_triplets = refined_triplets

        if not verified_triplets:
            logger.warning(
                "[SchemaVerification] No triplets survived verification; "
                "returning input unchanged"
            )
            return entities, relations

        # ----------------------------------------------------------
        # Map verified triplets back to Entity / Relation objects
        # ----------------------------------------------------------
        new_entities, new_relations = self._to_ragu_objects(
            verified_triplets, entities
        )

        logger.info(
            f"[SchemaVerification] Final: {len(new_entities)} entities, "
            f"{len(new_relations)} relations"
        )
        return new_entities, new_relations

    # ==================================================================
    # Stage implementations
    # ==================================================================

    @staticmethod
    def _reconstruct_context(
        entities: List[Entity],
        relations: List[Relation],
    ) -> str:
        """Build a textual paragraph from entity / relation descriptions."""
        parts: list[str] = []
        for e in entities:
            if e.description:
                parts.append(f"{e.entity_name}: {e.description}")
        for r in relations:
            if r.description:
                parts.append(
                    f"{r.subject_name} -> {r.object_name}: {r.description}"
                )
        return "\n".join(parts) if parts else ""

    # -- Step 1 ---------------------------------------------------------

    async def _step1_extract(self, context: str) -> list[Triplet]:
        """Few-shot LLM extraction -> list of raw triplets."""
        messages = build_extraction_messages(context)
        try:
            result = await self.client.complete(
                messages=messages,
                response_model=TripletList,
            )
        except Exception as exc:
            logger.error(f"[SchemaVerification] Step 1 LLM call failed: {exc}")
            return []

        if result is None:
            return []
        if isinstance(result, TripletList):
            return result.triplets
        return self._parse_triplets_text(result)

    @staticmethod
    def _parse_triplets_text(text) -> list[Triplet]:
        """Best-effort JSON parse from free-text LLM output."""
        if not isinstance(text, str):
            return []
        try:
            data = json.loads(text)
            if isinstance(data, dict) and "triplets" in data:
                data = data["triplets"]
            if isinstance(data, list):
                return [Triplet(**t) for t in data]
        except (json.JSONDecodeError, TypeError, KeyError):
            pass
        return []

    # -- Retrieval -------------------------------------------------------

    async def _retrieval(
        self,
        triplets: list[Triplet],
    ) -> tuple[
        dict[str, list[str]],
        dict[str, list[str]],
        dict[str, list[str]],
    ]:
        """Run FAISS-backed candidate linking for subjects, relations, objects."""
        subjects = [t.subject for t in triplets]
        relations = [t.relation for t in triplets]
        objects = [t.object for t in triplets]

        subject_map, relation_map, object_map = await asyncio.gather(
            self._linker.find_entity_candidates(subjects),
            self._linker.find_relation_candidates(relations),
            self._linker.find_entity_candidates(objects),
        )
        return subject_map, relation_map, object_map

    # -- Step 2 ---------------------------------------------------------

    async def _step2_refine(
        self,
        context: str,
        triplets: list[Triplet],
        subject_map: dict[str, list[str]],
        relation_map: dict[str, list[str]],
        object_map: dict[str, list[str]],
    ) -> list[Triplet]:
        """LLM refinement: select canonical names from top-k lists."""
        messages = build_refinement_messages(
            text=context,
            triplets=triplets,
            subject_mappings=subject_map,
            relation_mappings=relation_map,
            object_mappings=object_map,
        )
        try:
            result = await self.client.complete(
                messages=messages,
                response_model=TripletList,
            )
        except Exception as exc:
            logger.error(f"[SchemaVerification] Step 2 LLM call failed: {exc}")
            return triplets  # fallback: keep raw

        if result is None:
            return triplets
        if isinstance(result, TripletList):
            return result.triplets
        parsed = self._parse_triplets_text(result)
        return parsed if parsed else triplets

    # -- Step 3 ---------------------------------------------------------

    async def _step3_verify(
        self,
        triplets: list[Triplet],
    ) -> list[Triplet]:
        """Schema-based filtering via NEREL domain/range constraints."""
        try:
            results = await self._verifier.verify_batch(triplets)
        except Exception as exc:
            logger.error(
                f"[SchemaVerification] Schema verification failed: {exc}; "
                f"accepting all triplets"
            )
            return triplets
        return [vr.triplet for vr in results if vr.is_valid]

    # ==================================================================
    # Mapping back to RAGU Entity / Relation objects
    # ==================================================================

    @staticmethod
    def _to_ragu_objects(
        triplets: list[Triplet],
        original_entities: List[Entity],
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Convert verified :class:`Triplet` objects into RAGU
        :class:`Entity` and :class:`Relation` dataclasses.

        Preserves ``source_chunk_id`` and ``documents_id`` from original
        entities when the name matches (case-insensitive).
        """
        # Build a lookup of original entities for metadata inheritance
        original_by_name: dict[str, Entity] = {}
        for e in original_entities:
            original_by_name[e.entity_name.lower()] = e

        # Collect unique entity names from triplets
        entity_names: dict[str, Entity] = {}

        def _ensure_entity(name: str) -> Entity:
            key = name.lower()
            if key in entity_names:
                return entity_names[key]
            orig = original_by_name.get(key)
            ent = Entity(
                entity_name=name,
                entity_type=orig.entity_type if orig else "UNKNOWN",
                description=orig.description if orig else "",
                source_chunk_id=list(orig.source_chunk_id) if orig else [],
                documents_id=list(orig.documents_id) if orig else [],
                clusters=[],
            )
            entity_names[key] = ent
            return ent

        entities_out: list[Entity] = []
        relations_out: list[Relation] = []

        for t in triplets:
            subj = _ensure_entity(t.subject)
            obj = _ensure_entity(t.object)
            rel = Relation(
                subject_id=subj.id,
                object_id=obj.id,
                subject_name=t.subject,
                object_name=t.object,
                relation_type=t.relation,
                description=f"{t.subject} {t.relation} {t.object}",
                relation_strength=1.0,
                source_chunk_id=list(
                    set(subj.source_chunk_id) | set(obj.source_chunk_id)
                ),
            )
            relations_out.append(rel)

        entities_out = list(entity_names.values())
        return entities_out, relations_out
