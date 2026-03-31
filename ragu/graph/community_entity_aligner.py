"""
Community-based entity alignment for the RAGU knowledge graph pipeline.

Three-stage pipeline:
  Stage 1 — Community detection (Leiden / Louvain):
             The graph is partitioned into dense subgraphs via the Leiden
             algorithm.  Duplicate entities tend to cluster together because
             they share context and neighbours, so searching for duplicates
             within a community is much cheaper than searching the full graph.
  Stage 2 — Cosine-similarity candidate selection:
             Within each community every entity is embedded (name + description).
             All same-type pairs whose cosine similarity >= ``cosine_threshold``
             become alignment candidates.
  Stage 3 — LLM verification:
             Candidate pairs are verified in parallel using structured LLM
             output (``EntityAlignmentModel``).  Confirmed pairs are merged
             atomically: edges redirected, self-loops discarded.

Usage as a pipeline module (disabled by default)::

    aligner = CommunityEntityAligner(
        llm=llm,
        embedder=embedder,
        enabled=True,
    )
    knowledge_graph = KnowledgeGraph(..., additional_modules=[aligner])
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict, List, Set, Tuple

import networkx as nx
import numpy as np

from ragu.common.logger import logger
from ragu.graph.entity_aligner import EntityAligner, EntityAlignmentModel
from ragu.graph.graph_builder_pipeline import GraphBuilderModule
from ragu.graph.types import Entity, Relation
from ragu.models.embedder import Embedder
from ragu.models.llm import LLM


try:
    from graspologic.partition import leiden as _leiden  # type: ignore
    _LEIDEN_AVAILABLE = True
except ImportError:
    _leiden = None  # type: ignore
    _LEIDEN_AVAILABLE = False
    logger.warning(
        "graspologic.partition.leiden is not available; "
        "CommunityEntityAligner will treat all entities as a single community. "
        "Install with: pip install graspologic"
    )


# ---------------------------------------------------------------------------
# Main module
# ---------------------------------------------------------------------------

class CommunityEntityAligner(GraphBuilderModule):
    """
    Graph-builder module that deduplicates entities using community detection,
    cosine similarity, and LLM verification.

    By default this module is **disabled** (``enabled=False``) to ensure
    existing pipelines are unaffected. Set ``enabled=True`` to activate.

    Pipeline executed when enabled:

      Stage 1 — Build an undirected weighted graph from entities/relations,
                then apply Leiden community detection to find dense subgraphs.
                When graspologic is unavailable, the whole entity set is treated
                as one community.

      Stage 2 — For each community embed every entity as
                ``"<name>: <description>"`` and compute an N×N cosine-similarity
                matrix.  Pairs with the same ``entity_type`` and cosine
                similarity >= ``cosine_threshold`` become candidates.

      Stage 3 — All candidate pairs are sent to the LLM in parallel using
                structured output (``EntityAlignmentModel``).  Each confirmed
                pair is merged atomically: old entities removed, merged entity
                inserted, relations redirected, self-loops discarded.

    :param llm: LLM instance used for pair verification (structured output).
    :param embedder: Embedder instance used to produce entity vectors.
    :param cosine_threshold: Minimum cosine similarity to consider a pair as
        a candidate (default 0.85).
    :param min_community_size: Communities smaller than this value are skipped
        in Stage 2 (default 2).
    :param enabled: Activate the module (default False).
    """

    def __init__(
        self,
        llm: LLM,
        embedder: Embedder,
        cosine_threshold: float = 0.85,
        min_community_size: int = 2,
        enabled: bool = False,
    ) -> None:
        super().__init__()
        self.llm = llm
        self.embedder = embedder
        self.cosine_threshold = cosine_threshold
        self.min_community_size = min_community_size
        self.enabled = enabled
        # Reuse EntityAligner internals for LLM verification and atomic merge.
        self._aligner = EntityAligner(llm=llm, enabled=True)

    async def run(
        self,
        entities: List[Entity],
        relations: List[Relation],
        **kwargs: Any,
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Execute the community-based alignment pipeline.

        When disabled, returns input unchanged.  When enabled, runs all three
        stages and returns the deduplicated entities and relations.

        :param entities: Input entity list.
        :param relations: Input relation list.
        :return: Deduplicated (entities, relations).
        """
        if not self.enabled:
            return entities, relations

        if len(entities) < 2:
            return entities, relations

        logger.info(
            f"CommunityEntityAligner: {len(entities)} entities, "
            f"{len(relations)} relations."
        )

        # Stage 1: community detection
        entity_map: Dict[str, Entity] = {e.id: e for e in entities}
        communities = self._detect_communities(entities, relations)
        logger.info(f"Stage 1 complete: {len(communities)} communities detected.")

        # Stage 2: cosine-similarity candidates within communities
        candidate_pairs = await self._find_cosine_candidates(communities, entity_map)
        if not candidate_pairs:
            logger.info("CommunityEntityAligner: no candidate pairs found.")
            return entities, relations

        logger.info(f"Stage 2 complete: {len(candidate_pairs)} candidate pairs.")

        # Stage 3: LLM verification (all pairs in parallel)
        results: List[EntityAlignmentModel] = list(
            await asyncio.gather(*[
                self._aligner._verify_pair(e1, e2)
                for e1, e2 in candidate_pairs
            ])
        )

        # Apply confirmed merges sequentially to avoid ID conflicts
        current_entities = list(entities)
        current_relations = list(relations)
        merged_ids: Set[str] = set()
        n_merged = 0

        for (e1, e2), result in zip(candidate_pairs, results):
            if not result.should_merge:
                continue
            # Skip if one entity was already consumed by a prior merge
            if e1.id in merged_ids or e2.id in merged_ids:
                logger.debug(
                    f"Skipping {e1.entity_name!r} + {e2.entity_name!r}: "
                    "one entity was already consumed."
                )
                continue
            current_entities, current_relations = self._aligner._apply_merge(
                e1, e2, result, current_entities, current_relations
            )
            merged_ids.add(e1.id)
            merged_ids.add(e2.id)
            n_merged += 1

        logger.info(
            f"CommunityEntityAligner done: {n_merged} pairs merged; "
            f"{len(current_entities)} entities, {len(current_relations)} relations."
        )
        return current_entities, current_relations

    # ------------------------------------------------------------------
    # Stage 1: community detection
    # ------------------------------------------------------------------

    def _detect_communities(
        self,
        entities: List[Entity],
        relations: List[Relation],
    ) -> List[List[str]]:
        """
        Partition entities into communities using the Leiden algorithm.

        Builds an undirected weighted graph (edge weight = sum of
        ``relation_strength`` values for parallel relations).  Applies Leiden
        partitioning; falls back to a single community if graspologic is
        unavailable or if the graph has no edges.

        :param entities: All entities in the graph.
        :param relations: All relations in the graph.
        :return: List of communities; each community is a list of entity IDs.
        """
        entity_ids: Set[str] = {e.id for e in entities}

        G = nx.Graph()
        G.add_nodes_from(entity_ids)

        for rel in relations:
            if rel.subject_id not in entity_ids or rel.object_id not in entity_ids:
                continue
            if rel.subject_id == rel.object_id:
                continue
            u, v = rel.subject_id, rel.object_id
            if G.has_edge(u, v):
                G[u][v]["weight"] = G[u][v].get("weight", 1.0) + float(rel.relation_strength)
            else:
                G.add_edge(u, v, weight=float(rel.relation_strength))

        # Leiden requires at least one edge
        if not _LEIDEN_AVAILABLE or G.number_of_edges() == 0:
            logger.debug(
                "CommunityEntityAligner: Leiden unavailable or no edges — "
                "using single community."
            )
            return [list(entity_ids)]

        try:
            partition, quality = _leiden(G)
            community_map: Dict[int, List[str]] = {}
            for node_id, comm_id in partition.items():
                community_map.setdefault(int(comm_id), []).append(str(node_id))

            communities = [
                ids for ids in community_map.values()
                if len(ids) >= self.min_community_size
            ]

            logger.debug(
                f"Leiden quality={quality:.4f}; "
                f"{len(community_map)} raw communities, "
                f"{len(communities)} with >= {self.min_community_size} members."
            )

            # Isolated nodes (singletons) don't produce candidates — safe to drop
            return communities if communities else [list(entity_ids)]

        except Exception as exc:
            logger.warning(
                f"CommunityEntityAligner: Leiden failed ({type(exc).__name__}: {exc}); "
                "falling back to single community."
            )
            return [list(entity_ids)]

    # ------------------------------------------------------------------
    # Stage 2: cosine-similarity candidate selection
    # ------------------------------------------------------------------

    async def _find_cosine_candidates(
        self,
        communities: List[List[str]],
        entity_map: Dict[str, Entity],
    ) -> List[Tuple[Entity, Entity]]:
        """
        Embed entities in each community and return pairs above the cosine threshold.

        Each entity is represented as ``"<name>: <description[:300]>"``.
        Only same-``entity_type`` pairs are considered.  Duplicate pairs across
        communities are deduplicated by (min_id, max_id) key.

        :param communities: List of entity-ID lists from Stage 1.
        :param entity_map: Mapping entity_id → Entity.
        :return: Candidate pairs for LLM verification.
        """
        all_candidates: List[Tuple[Entity, Entity]] = []
        seen_pairs: Set[Tuple[str, str]] = set()

        for comm_ids in communities:
            comm_entities = [
                entity_map[eid] for eid in comm_ids if eid in entity_map
            ]
            if len(comm_entities) < 2:
                continue

            texts = [
                f"{e.entity_name}: {(e.description or '').strip()[:300]}"
                for e in comm_entities
            ]

            try:
                raw_vecs = await self.embedder.batch_embed_text(
                    texts, desc="CommunityEntityAligner: embedding"
                )
            except Exception as exc:
                logger.warning(
                    f"CommunityEntityAligner: embedding failed for community of "
                    f"{len(comm_entities)} entities: {exc}"
                )
                continue

            vecs = np.array(raw_vecs, dtype=np.float32)
            # L2-normalise → dot product equals cosine similarity
            norms = np.linalg.norm(vecs, axis=1, keepdims=True)
            norms = np.maximum(norms, 1e-9)
            normed = vecs / norms
            sim = normed @ normed.T  # shape (N, N)

            n = len(comm_entities)
            for i in range(n):
                for j in range(i + 1, n):
                    e1, e2 = comm_entities[i], comm_entities[j]

                    # Only align entities of the same type
                    if e1.entity_type != e2.entity_type:
                        continue

                    pair_key = (min(e1.id, e2.id), max(e1.id, e2.id))
                    if pair_key in seen_pairs:
                        continue

                    if float(sim[i, j]) >= self.cosine_threshold:
                        all_candidates.append((e1, e2))
                        seen_pairs.add(pair_key)
                        logger.debug(
                            f"Candidate: {e1.entity_name!r} ↔ {e2.entity_name!r}  "
                            f"cosine={sim[i, j]:.3f}"
                        )

        return all_candidates
