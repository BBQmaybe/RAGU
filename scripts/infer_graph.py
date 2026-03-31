#!/usr/bin/env python3
"""
Knowledge-graph inference: answer a natural-language question using an
already-built RAGU graph, with lazy entity alignment applied on every query.

Lazy alignment pipeline (per query)
------------------------------------
1. Embed the query → retrieve top-K entities from the entity VDB.
2. Find candidate duplicate pairs among those K entities via difflib
   (0 API calls, 0 embedder calls — operates on names only).
3. Verify each candidate pair with LLM in parallel.
4. For every confirmed merge: atomically update the graph
       delete old entities (cascades their relations) →
       insert merged entity →
       re-insert redirected relations (self-loops discarded).
5. Pass the (updated) entity set to LocalSearchEngine → return answer.

The graph is improved iteratively: once a pair is merged, subsequent
queries find the canonical entity without triggering the LLM again.

Usage:
    python scripts/infer_graph.py \\
        --storage-dir chehov_horse1 \\
        --query "Кто является главным героем произведения?"

    # interactive mode (reads queries from stdin)
    python scripts/infer_graph.py --storage-dir chehov_horse1

Environment variables:
    OPENAI_API_KEY          — required.
    OPENAI_BASE_URL         — optional (default: https://api.openai.com/v1).
    OPENAI_MODEL            — optional (default: gpt-4o-mini).
    OPENAI_EMBEDDING_MODEL  — optional (default: text-embedding-3-large).
    OPENAI_EMBEDDING_DIM    — optional (default: 3072).
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import os
import sys
from typing import Dict, List, Optional, Set

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ragu.common.global_parameters import Settings
from ragu.common.logger import logger
from ragu.graph.entity_aligner import EntityAligner, EntityAlignmentModel
from ragu.graph.knowledge_graph import KnowledgeGraph
from ragu.graph.types import Entity, Relation
from ragu.models.embedder import EmbedderOpenAI
from ragu.models.llm import LLM, LLMOpenAI
from ragu.models.openai import CachedAsyncOpenAI
from ragu.search_engine.local_search import LocalSearchEngine
from ragu.storage import Embedding


# ---------------------------------------------------------------------------
# Lazy entity aligner
# ---------------------------------------------------------------------------

class LazyEntityAligner:
    """
    Entity aligner that operates at inference time on the small set of
    entities retrieved for a specific query.

    Unlike the batch ``EntityAligner`` (which processes the whole graph
    upfront), this class:

    - Compares only the top-K entities retrieved for each query
      (typically 20–50), making name-similarity checks trivially cheap.
    - Sends confirmed pairs to the LLM in parallel (few calls per query).
    - Atomically patches the live ``KnowledgeGraph``:
          delete old entities (cascades their relations)
          → insert merged entity
          → re-insert redirected relations (self-loops dropped).

    Because changes are persisted, subsequent queries over the same or
    similar topics find the already-merged entity without triggering
    further LLM calls.

    :param llm: LLM for pair verification.
    :param knowledge_graph: Live ``KnowledgeGraph`` instance to patch.
    :param threshold: difflib ratio threshold for candidate detection (default 0.85).
    """

    def __init__(
        self,
        llm: LLM,
        knowledge_graph: KnowledgeGraph,
        threshold: float = 0.85,
    ) -> None:
        self._kg = knowledge_graph
        # Reuse EntityAligner internals for difflib + LLM verification.
        self._aligner = EntityAligner(llm=llm, threshold=threshold, enabled=True)

    async def align_retrieved(self, entities: List[Entity]) -> List[Entity]:
        """
        Align near-duplicate entities within ``entities`` and persist changes.

        :param entities: Entities retrieved from the VDB for the current query.
        :return: Updated entity list with merged entities substituted in-place.
        """
        if len(entities) < 2:
            return entities

        # Step 1: candidate detection — difflib, 0 API/embedder calls.
        candidate_pairs = self._aligner._find_candidates(entities)
        if not candidate_pairs:
            return entities

        logger.info(
            f"LazyEntityAligner: {len(candidate_pairs)} candidate pair(s) to verify."
        )

        # Step 2: LLM verification — all pairs in parallel.
        for e1, e2 in candidate_pairs:
            ratio = difflib.SequenceMatcher(
                None, e1.entity_name.lower(), e2.entity_name.lower()
            ).ratio()
            logger.info(
                f"LazyEntityAligner: candidate  {e1.entity_name!r} ({e1.entity_type})"
                f"  ↔  {e2.entity_name!r} ({e2.entity_type})  ratio={ratio:.2f}"
            )

        results: List[EntityAlignmentModel] = list(
            await asyncio.gather(*[
                self._aligner._verify_pair(e1, e2) for e1, e2 in candidate_pairs
            ])
        )

        merged_ids: Set[str] = set()
        # maps old_entity_id → new merged Entity (for return value update)
        replacement: Dict[str, Entity] = {}

        for (e1, e2), result in zip(candidate_pairs, results):
            logger.info(
                f"LazyEntityAligner: LLM decision  {e1.entity_name!r} ↔ {e2.entity_name!r}"
                f"  →  should_merge={result.should_merge}"
                + (f"  merged_name={result.merged_entity_name!r}" if result.should_merge else "")
            )
            if not result.should_merge:
                continue
            if e1.id in merged_ids or e2.id in merged_ids:
                logger.debug(
                    f"Skipping {e1.entity_name!r}+{e2.entity_name!r}: "
                    "one was already consumed."
                )
                continue

            merged = await self._apply_atomic_merge(e1, e2, result)
            if merged is None:
                continue  # merge failed; graph is unchanged

            merged_ids.update({e1.id, e2.id})
            replacement[e1.id] = merged
            replacement[e2.id] = merged

        if not replacement:
            return entities

        # Rebuild the returned entity list substituting merged entries.
        seen_merged: Set[str] = set()
        updated: List[Entity] = []
        for e in entities:
            if e.id in replacement:
                merged_e = replacement[e.id]
                if merged_e.id not in seen_merged:
                    updated.append(merged_e)
                    seen_merged.add(merged_e.id)
            else:
                updated.append(e)
        return updated

    async def _apply_atomic_merge(
        self,
        e1: Entity,
        e2: Entity,
        result: EntityAlignmentModel,
    ) -> Optional[Entity]:
        """
        Delete e1/e2 from the live graph, insert a merged entity, redirect edges.

        :return: The new merged ``Entity`` on success, ``None`` on error.
        """
        index = self._kg.index
        old_ids = {e1.id, e2.id}

        # Collect all incident edges BEFORE cascading delete.
        try:
            groups = await index.graph_backend.get_all_edges_for_nodes(
                [e1.id, e2.id]
            )
        except Exception as exc:
            logger.warning(f"LazyEntityAligner: failed to collect edges: {exc}")
            return None

        # Deduplicate edges by relation ID.
        seen_rel_ids: Set[str] = set()
        all_edges: List[Relation] = []
        for group in groups:
            for rel in group:
                if rel.id not in seen_rel_ids:
                    seen_rel_ids.add(rel.id)
                    all_edges.append(rel)

        merged = Entity(
            entity_name=result.merged_entity_name,
            entity_type=result.merged_entity_type,
            description=result.merged_description,
            source_chunk_id=sorted({
                c for e in (e1, e2) for c in e.source_chunk_id
            }),
            documents_id=sorted({
                d for e in (e1, e2) for d in e.documents_id
            }),
        )

        try:
            # 1. Remove old entities (cascades relation deletion in graph + VDB).
            logger.info(
                f"LazyEntityAligner: deleting {e1.entity_name!r} (id={e1.id}) "
                f"and {e2.entity_name!r} (id={e2.id}), {len(all_edges)} edges collected"
            )
            await index.delete_entities([e1.id, e2.id])

            # 2. Insert merged entity (graph + VDB).
            logger.info(
                f"LazyEntityAligner: inserting merged entity "
                f"{merged.entity_name!r} (id={merged.id})"
            )
            await index.insert_entities([merged])

            # 3. Re-insert redirected relations (self-loops discarded).
            redirected: List[Relation] = []
            for rel in all_edges:
                subj_id = merged.id if rel.subject_id in old_ids else rel.subject_id
                obj_id = merged.id if rel.object_id in old_ids else rel.object_id
                if subj_id == obj_id:
                    logger.debug(
                        f"Discarding self-loop: "
                        f"({rel.subject_name}, {rel.relation_type}, {rel.object_name})"
                    )
                    continue
                subj_name = (
                    result.merged_entity_name
                    if rel.subject_id in old_ids
                    else rel.subject_name
                )
                obj_name = (
                    result.merged_entity_name
                    if rel.object_id in old_ids
                    else rel.object_name
                )
                redirected.append(Relation(
                    subject_id=subj_id,
                    object_id=obj_id,
                    subject_name=subj_name,
                    object_name=obj_name,
                    relation_type=rel.relation_type,
                    description=rel.description,
                    relation_strength=rel.relation_strength,
                    source_chunk_id=list(rel.source_chunk_id),
                ))

            if redirected:
                logger.info(
                    f"LazyEntityAligner: inserting {len(redirected)} redirected relation(s)"
                )
                await index.insert_relations(redirected)

        except Exception as exc:
            logger.warning(
                f"LazyEntityAligner: atomic merge FAILED for "
                f"({e1.entity_name!r}, {e2.entity_name!r}): {type(exc).__name__}: {exc}"
            )
            return None

        logger.info(
            f"LazyEntityAligner: merged {e1.entity_name!r} + "
            f"{e2.entity_name!r} → {merged.entity_name!r}"
        )
        return merged


# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------

async def _answer(
    query: str,
    search_engine: LocalSearchEngine,
    aligner: LazyEntityAligner,
    top_k: int,
    use_summary: bool,
    use_chunks: bool,
) -> str:
    """Retrieve top-K entities, align them lazily, then answer the query."""
    # Retrieve top-K entities (same path as LocalSearchEngine.a_search).
    embedding = await search_engine.embedder.embed_text(query)
    hits = await search_engine.knowledge_graph.index.entity_vector_db.query(
        Embedding(embedding), top_k=top_k
    )
    entities = await search_engine.knowledge_graph.index.get_entities(
        [h.id for h in hits]
    )
    entities = [e for e in entities if e is not None]

    # Lazy alignment — updates graph in-place if merges are needed.
    entities = await aligner.align_retrieved(entities)

    # Let LocalSearchEngine build context from (now-aligned) entities and answer.
    return await search_engine.a_query(
        query, top_k=top_k, use_summary=use_summary, use_chunks=use_chunks
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_client() -> CachedAsyncOpenAI:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY is not set.", file=sys.stderr)
        sys.exit(1)
    return CachedAsyncOpenAI(
        base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        api_key=api_key,
        rate_min_delay=2,
        rate_max_simultaneous=5,
        retry_times_sec=(2, 2, 2),
    )


async def _run(args: argparse.Namespace) -> None:
    Settings.storage_folder = args.storage_dir

    client = _build_client()
    llm = LLMOpenAI(client, os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
    embedder = EmbedderOpenAI(
        client,
        os.environ.get("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"),
        dim=int(os.environ.get("OPENAI_EMBEDDING_DIM", "3072")),
    )

    knowledge_graph = KnowledgeGraph(llm=llm, embedder=embedder)
    search_engine = LocalSearchEngine(
        llm=llm,
        knowledge_graph=knowledge_graph,
        embedder=embedder,
        language=args.language,
    )
    aligner = LazyEntityAligner(
        llm=llm,
        knowledge_graph=knowledge_graph,
        threshold=args.threshold,
    )

    if args.query:
        answer = await _answer(
            query=args.query,
            search_engine=search_engine,
            aligner=aligner,
            top_k=args.top_k,
            use_summary=args.use_summary,
            use_chunks=args.use_chunks,
        )
        print(answer)
    else:
        # Interactive mode.
        print(f"Knowledge graph loaded from '{args.storage_dir}'.")
        print("Enter your query (Ctrl-D or empty line to exit):\n")
        while True:
            try:
                query = input("Query> ").strip()
            except EOFError:
                break
            if not query:
                break
            answer = await _answer(
                query=query,
                search_engine=search_engine,
                aligner=aligner,
                top_k=args.top_k,
                use_summary=args.use_summary,
                use_chunks=args.use_chunks,
            )
            print(f"\nAnswer:\n{answer}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Answer a question over a RAGU knowledge graph with lazy entity alignment.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--storage-dir", required=True,
        help="RAGU storage folder (e.g. chehov_horse1).",
    )
    parser.add_argument(
        "--query", default=None,
        help="Question to answer. Omit for interactive mode.",
    )
    parser.add_argument(
        "--top-k", type=int, default=20, metavar="K",
        help="Number of entities to retrieve from the VDB per query.",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.85, metavar="T",
        help="difflib similarity threshold for entity alignment candidates.",
    )
    parser.add_argument(
        "--language", default=None,
        help="Response language (e.g. 'russian'). Defaults to Settings.language.",
    )
    parser.add_argument(
        "--use-summary", action="store_true",
        help="Include community summaries in the search context.",
    )
    parser.add_argument(
        "--use-chunks", action="store_true",
        help="Include text chunks in the search context.",
    )

    args = parser.parse_args()

    if not os.path.isdir(args.storage_dir):
        print(
            f"Error: storage directory not found: {args.storage_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
