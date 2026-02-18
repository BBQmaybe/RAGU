"""
FAISS-based candidate linker for Wikidata entity / relation linking.

For every extracted name (subject, relation, or object) the linker:

1. Queries the Wikidata search API to obtain a broad set of candidate labels.
2. Embeds the extracted name **and** all candidate labels in a single batch
   via the project's :class:`BaseEmbedder`.
3. Builds a **FAISS flat-IP index** from the candidate embeddings and
   retrieves the **top-k** nearest neighbours by cosine similarity.
4. Returns a mapping ``extracted_name → [top-k canonical labels]``.

The architecture deliberately mirrors the paper's approach (FAISS +
Contriever) while plugging into RAGU's embedder abstraction so that *any*
embedder — including a Contriever-backed one — can be substituted.
"""

from __future__ import annotations

import asyncio
from typing import List

import faiss
import numpy as np

from ragu.common.logger import logger
from ragu.embedder.base_embedder import BaseEmbedder
from ragu.graph.wikidata_verification.wikidata_client import WikidataClient


class WikidataCandidateLinker:
    """
    Retrieve top-k canonical Wikidata labels for a set of extracted names.

    :param embedder: Any :class:`BaseEmbedder` implementation (OpenAI,
        Contriever, Sentence-Transformers, …).
    :param wikidata_client: Client for Wikidata REST / SPARQL endpoints.
    :param top_k: Number of canonical candidates to return per element.
    :param language: Language code for Wikidata search.
    :param search_limit: How many raw candidates to request from the
        Wikidata search API *before* FAISS re-ranking.
    """

    def __init__(
        self,
        embedder: BaseEmbedder,
        wikidata_client: WikidataClient,
        top_k: int = 5,
        language: str = "en",
        search_limit: int = 15,
    ) -> None:
        self.embedder = embedder
        self.wikidata_client = wikidata_client
        self.top_k = top_k
        self.language = language
        self.search_limit = search_limit

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def find_entity_candidates(
        self,
        names: List[str],
    ) -> dict[str, list[str]]:
        """
        For each unique entity name return up to *top_k* canonical Wikidata
        item labels, ranked by cosine similarity to the original name.

        :param names: Extracted entity names (may contain duplicates).
        :return: Mapping ``{name: [candidate_label, …]}``.
        """
        unique = list(dict.fromkeys(names))
        return await self._link(
            unique,
            search_fn=self.wikidata_client.search_entities,
        )

    async def find_relation_candidates(
        self,
        relations: List[str],
    ) -> dict[str, list[str]]:
        """
        For each unique relation type return up to *top_k* canonical
        Wikidata property labels, ranked by cosine similarity.

        :param relations: Extracted relation types (may contain duplicates).
        :return: Mapping ``{relation: [candidate_label, …]}``.
        """
        unique = list(dict.fromkeys(relations))
        return await self._link(
            unique,
            search_fn=self.wikidata_client.search_properties,
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _link(
        self,
        queries: list[str],
        search_fn,
    ) -> dict[str, list[str]]:
        """
        Core linking routine shared by entity and relation retrieval.

        1. Fetch Wikidata candidates for every *query* in parallel.
        2. Batch-embed all texts (queries + all candidate labels).
        3. For each query build a tiny FAISS index over its candidates and
           retrieve the top-k by inner-product (cosine on L2-normalised
           vectors).
        """
        if not queries:
            return {}

        # Step 1 — gather raw Wikidata candidates concurrently
        tasks = [
            search_fn(q, language=self.language, limit=self.search_limit)
            for q in queries
        ]
        raw_results: list[list[dict[str, str]]] = await asyncio.gather(
            *tasks, return_exceptions=True
        )
        # Materialise: for each query, keep deduplicated labels
        candidates_per_query: list[list[str]] = []
        for q, res in zip(queries, raw_results):
            if isinstance(res, Exception):
                logger.warning(f"Candidate search failed for '{q}': {res}")
                candidates_per_query.append([])
                continue
            labels: list[str] = []
            seen: set[str] = set()
            for item in res:
                lbl = item.get("label", "").strip()
                if lbl and lbl.lower() not in seen:
                    seen.add(lbl.lower())
                    labels.append(lbl)
            candidates_per_query.append(labels)

        # Step 2 — collect all texts that need embedding (deduplicated)
        text_set: dict[str, int] = {}
        for q in queries:
            if q not in text_set:
                text_set[q] = len(text_set)
        for labels in candidates_per_query:
            for lbl in labels:
                if lbl not in text_set:
                    text_set[lbl] = len(text_set)

        all_texts = list(text_set.keys())
        if not all_texts:
            return {q: [] for q in queries}

        embeddings_raw = await self.embedder.embed(all_texts)

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

        # Step 3 — per-query FAISS retrieval
        result: dict[str, list[str]] = {}
        for q, labels in zip(queries, candidates_per_query):
            if not labels:
                result[q] = []
                continue

            query_vec = emb_matrix[text_set[q]].reshape(1, -1)

            # Candidate matrix
            cand_indices = [text_set[lbl] for lbl in labels if lbl in text_set]
            if not cand_indices:
                result[q] = []
                continue
            cand_matrix = emb_matrix[cand_indices]

            # Build a tiny flat inner-product index
            index = faiss.IndexFlatIP(dim)
            index.add(cand_matrix)  # type: ignore[arg-type]

            k = min(self.top_k, len(cand_indices))
            distances, indices = index.search(query_vec, k)  # type: ignore[arg-type]

            ranked: list[str] = []
            for idx in indices[0]:
                if 0 <= idx < len(cand_indices):
                    original_idx = cand_indices[idx]
                    ranked.append(all_texts[original_idx])
            result[q] = ranked

        return result
