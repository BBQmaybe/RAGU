"""
Entity Alignment at Inference Time — Usage Example
====================================================

Demonstrates how to plug EntityAligner into LocalSearchEngine so that
near-duplicate entities are automatically detected and merged on the fly
as queries arrive, without a separate offline deduplication step.

Flow
----
1. Build (or load) a knowledge graph from documents.
2. Create an EntityAligner with a similarity threshold.
3. Pass it to LocalSearchEngine.
4. Each call to a_query() / a_search() will:
   a. Retrieve top-k entities via vector search.
   b. Compute pairwise name similarity (SequenceMatcher, zero LLM cost).
   c. Send only the candidate pairs to the LLM for merge verification.
   d. Atomically update the graph with confirmed merges.
   e. Use the aligned entities to assemble the answer context.

Subsequent queries that touch the same entities benefit automatically because
the graph has already been aligned — no repeated LLM calls are needed.
"""

import asyncio

from ragu import (
    ArtifactsExtractorLLM,
    BuilderArguments,
    EntityAligner,
    KnowledgeGraph,
    LocalSearchEngine,
    Settings,
    SimpleChunker,
)
from ragu.embedder import OpenAIEmbedder
from ragu.llm import OpenAIClient
from ragu.utils.ragu_utils import read_text_from_files


EMBEDDER_MODEL_NAME = "..."
LLM_MODEL_NAME = "..."
BASE_URL = "..."
API_KEY = "..."


async def main():
    # ── 1. Global settings ─────────────────────────────────────────────────
    Settings.storage_folder = "ragu_working_dir/entity_alignment_example"
    Settings.language = "russian"

    # ── 2. Infrastructure: LLM client + embedder ───────────────────────────
    client = OpenAIClient(
        model_name=LLM_MODEL_NAME,
        base_url=BASE_URL,
        api_token=API_KEY,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        cache_flush_every=10,
    )

    embedder = OpenAIEmbedder(
        model_name=EMBEDDER_MODEL_NAME,
        base_url=BASE_URL,
        api_token=API_KEY,
        dim=3072,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        use_cache=True,
    )

    # ── 3. Build knowledge graph ────────────────────────────────────────────
    docs = read_text_from_files("examples/data/ru")

    knowledge_graph = KnowledgeGraph(
        client=client,
        embedder=embedder,
        chunker=SimpleChunker(max_chunk_size=1000),
        artifact_extractor=ArtifactsExtractorLLM(client=client, do_validation=False),
        builder_settings=BuilderArguments(
            use_llm_summarization=True,
            vectorize_chunks=True,
        ),
    )
    await knowledge_graph.build_from_docs(docs)

    # ── 4. Create EntityAligner ─────────────────────────────────────────────
    #
    # name_similarity_threshold — minimum SequenceMatcher ratio (0–1) to
    # treat two entity names as a candidate pair and send them to the LLM.
    #
    # Conservative value (0.92+) → fewer LLM calls, may miss soft aliases
    # ("Иван Петров" vs "Петров И.П.")
    #
    # Liberal value (0.75)       → more LLM calls, catches more variants
    #
    aligner = EntityAligner(
        client=client,
        knowledge_graph=knowledge_graph,
        name_similarity_threshold=0.85,  # tune for your dataset
    )

    # ── 5. Wire aligner into LocalSearchEngine ──────────────────────────────
    #
    # entity_aligner=None  →  standard behaviour, no alignment
    # entity_aligner=aligner → alignment runs before context assembly
    #
    search_engine = LocalSearchEngine(
        client=client,
        knowledge_graph=knowledge_graph,
        embedder=embedder,
        tokenizer_model="gpt-4o-mini",
        entity_aligner=aligner,
    )

    # ── 6. Query loop ───────────────────────────────────────────────────────
    questions = [
        "Кто написал гимн Норвегии?",
        "Шум, издаваемый ЭТИМИ ПАУКООБРАЗНЫМИ, слышен за пять километров.",
        "Как переводится роман 'Камо грядеши, Господи?'",
    ]

    for question in questions:
        print(f"Q: {question}")
        answer = await search_engine.a_query(question)
        print(f"A: {answer}\n")


# ── Advanced: inspect alignment candidates without modifying the graph ──────
async def dry_run_alignment(knowledge_graph: KnowledgeGraph, client, query: str):
    """
    Shows alignment candidates for a query without touching the graph.

    Useful for tuning `name_similarity_threshold` before deploying alignment
    in production.
    """
    from ragu.graph.entity_aligner import EntityAligner

    aligner = EntityAligner(
        client=client,
        knowledge_graph=knowledge_graph,
        name_similarity_threshold=0.80,
    )

    # Retrieve top-k entities exactly as LocalSearchEngine does
    entities_id = await knowledge_graph.index.entity_vector_db.query(query, top_k=20)
    entities = [
        await knowledge_graph.get_entity(e["__id__"]) for e in entities_id
    ]
    entities = [e for e in entities if e is not None]

    # Inspect candidates (string similarity only, no LLM call)
    candidates = aligner._find_candidates(entities)
    if not candidates:
        print("No alignment candidates found.")
        return

    print(f"Found {len(candidates)} candidate pair(s):\n")
    for entity_a, entity_b in candidates:
        sim = EntityAligner._name_similarity(entity_a.entity_name, entity_b.entity_name)
        print(
            f"  [{sim:.2f}]  '{entity_a.entity_name}' ({entity_a.entity_type})"
            f"  ↔  '{entity_b.entity_name}' ({entity_b.entity_type})"
        )


if __name__ == "__main__":
    asyncio.run(main())
