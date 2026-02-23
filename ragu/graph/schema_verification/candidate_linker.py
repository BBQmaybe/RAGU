"""
FAISS-based candidate linker using the internal NEREL schema.

For every extracted name (subject, relation, or object) the linker:

1. Builds a candidate vocabulary from the **existing entities** already
   in the pipeline (for entity linking) or from the **NEREL relation
   types** (for relation linking).
2. Embeds the extracted name **and** all candidates via the project's
   :class:`BaseEmbedder`.
3. Builds a FAISS flat-IP index and retrieves the **top-k** nearest
   neighbours by cosine similarity.
4. Returns a mapping ``extracted_name -> [top-k canonical labels]``.

Uses a purely internal vocabulary (NEREL types + existing graph entities)
with no external network dependencies.
"""

from __future__ import annotations

from typing import List

import faiss
import numpy as np

from ragu.common.logger import logger
from ragu.embedder.base_embedder import BaseEmbedder
from ragu.graph.schema_verification.schema import (
    ALLOWED_ENTITY_TYPES,
    ALLOWED_RELATION_TYPES,
)


class SchemaAwareCandidateLinker:
    """
    Retrieve top-k canonical labels from the internal NEREL vocabulary.

    For **entities** the candidates are drawn from:
    - The NEREL entity type names (canonical vocabulary).
    - The names of entities already extracted in the pipeline (if provided).

    For **relations** the candidates come from the NEREL relation type names.

    :param embedder: Any :class:`BaseEmbedder` implementation.
    :param top_k: Number of candidates to return per element.
    :param existing_entity_names: Optional list of entity names already
        in the graph, used as additional candidates during entity linking.
    """

    def __init__(
        self,
        embedder: BaseEmbedder,
        top_k: int = 5,
        existing_entity_names: list[str] | None = None,
    ) -> None:
        self.embedder = embedder
        self.top_k = top_k
        self._entity_candidates: list[str] = list(ALLOWED_ENTITY_TYPES)
        if existing_entity_names:
            seen = {n.lower() for n in self._entity_candidates}
            for name in existing_entity_names:
                if name.lower() not in seen:
                    self._entity_candidates.append(name)
                    seen.add(name.lower())
        self._relation_candidates: list[str] = list(ALLOWED_RELATION_TYPES)

    def update_entity_candidates(self, entity_names: list[str]) -> None:
        """Add new entity names to the candidate vocabulary."""
        seen = {n.lower() for n in self._entity_candidates}
        for name in entity_names:
            if name.lower() not in seen:
                self._entity_candidates.append(name)
                seen.add(name.lower())

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def find_entity_candidates(
        self,
        names: List[str],
    ) -> dict[str, list[str]]:
        """
        For each unique entity name return up to *top_k* candidates
        from the internal vocabulary, ranked by cosine similarity.

        :param names: Extracted entity names (may contain duplicates).
        :return: Mapping ``{name: [candidate_label, ...]}``.
        """
        unique = list(dict.fromkeys(names))
        return await self._link(unique, self._entity_candidates)

    async def find_relation_candidates(
        self,
        relations: List[str],
    ) -> dict[str, list[str]]:
        """
        For each unique relation type return up to *top_k* NEREL
        relation type names, ranked by cosine similarity.

        :param relations: Extracted relation types (may contain duplicates).
        :return: Mapping ``{relation: [candidate_label, ...]}``.
        """
        unique = list(dict.fromkeys(relations))
        return await self._link(unique, self._relation_candidates)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _link(
        self,
        queries: list[str],
        candidates: list[str],
    ) -> dict[str, list[str]]:
        """
        Core linking routine.

        1. Batch-embed all texts (queries + candidate labels).
        2. For each query build a tiny FAISS index over candidates and
           retrieve the top-k by inner-product (cosine on L2-normalised
           vectors).
        """
        if not queries or not candidates:
            return {q: [] for q in queries}

        # Collect all texts that need embedding (deduplicated)
        text_set: dict[str, int] = {}
        for q in queries:
            if q not in text_set:
                text_set[q] = len(text_set)
        for lbl in candidates:
            if lbl not in text_set:
                text_set[lbl] = len(text_set)

        all_texts = list(text_set.keys())
        try:
            embeddings_raw = await self.embedder.embed(all_texts)
        except Exception as exc:
            logger.error(f"[CandidateLinker] Embedder failed: {exc}")
            return {q: [] for q in queries}

        # Build a dense numpy matrix; replace None embeddings with zeros
        dim: int | None = None
        for emb in embeddings_raw:
            if emb is not None:
                dim = len(emb)
                break
        if dim is None:
            logger.warning("Embedder returned no valid embeddings")
            return {q: [] for q in queries}

        emb_matrix = np.zeros((len(all_texts), dim), dtype=np.float32)
        for i, emb in enumerate(embeddings_raw):
            if emb is not None:
                emb_matrix[i] = np.asarray(emb, dtype=np.float32)

        # L2-normalise so inner product == cosine similarity
        norms = np.linalg.norm(emb_matrix, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        emb_matrix = emb_matrix / norms

        # Build a single FAISS index from all candidates
        cand_indices = [text_set[lbl] for lbl in candidates if lbl in text_set]
        if not cand_indices:
            return {q: [] for q in queries}

        cand_matrix = emb_matrix[cand_indices]
        index = faiss.IndexFlatIP(dim)
        index.add(cand_matrix)

        # Per-query retrieval
        result: dict[str, list[str]] = {}
        for q in queries:
            query_vec = emb_matrix[text_set[q]].reshape(1, -1)
            k = min(self.top_k, len(cand_indices))
            distances, indices = index.search(query_vec, k)

            ranked: list[str] = []
            for idx in indices[0]:
                if 0 <= idx < len(cand_indices):
                    original_idx = cand_indices[idx]
                    ranked.append(all_texts[original_idx])
            result[q] = ranked

        return result
