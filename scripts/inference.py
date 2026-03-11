"""
Inference (search/query) over a previously built RAGU knowledge graph.

Supports three search modes:
  - local  — entity-centric RAG (default, best for specific questions)
  - global — community-summary RAG (best for broad/thematic questions)
  - naive  — plain vector-chunk RAG (no graph, just embeddings)

Usage:
    python inference.py --query "Кто такой Чехов?"
    python inference.py --query "Основные темы рассказа" --mode global
    python inference.py --query "Что случилось с лошадью?" --mode local --top-k 10

    # Run entity alignment before querying
    python inference.py --query "..." --align --align-threshold 0.8
"""

import argparse
import asyncio

from ragu import (
    KnowledgeGraph,
    BuilderArguments,
    LocalSearchEngine,
    GlobalSearchEngine,
    NaiveSearchEngine,
    Settings,
)
from ragu.llm import OpenAIClient
from ragu.embedder import OpenAIEmbedder

# ── defaults (override via CLI args or environment) ──────────────────────

LLM_MODEL_NAME = "gpt-4o-mini"
LLM_BASE_URL = "https://api.openai.com/v1"
LLM_API_KEY = "api"
EMBEDDER_MODEL_NAME = "text-embedding-3-large"


def _build_client(args) -> OpenAIClient:
    return OpenAIClient(
        model_name=args.llm_model or LLM_MODEL_NAME,
        base_url=args.llm_base_url or LLM_BASE_URL,
        api_token=args.llm_api_key or LLM_API_KEY,
        max_requests_per_second=1,
        max_requests_per_minute=60,
    )


def _build_embedder(args) -> OpenAIEmbedder:
    return OpenAIEmbedder(
        model_name=args.embedder_model or EMBEDDER_MODEL_NAME,
        base_url=args.llm_base_url or LLM_BASE_URL,
        api_token=args.llm_api_key or LLM_API_KEY,
        dim=3072,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        use_cache=True,
    )


async def main(args):
    Settings.storage_folder = args.storage_folder
    Settings.language = args.language

    client = _build_client(args)
    embedder = _build_embedder(args)

    # Load an already-built KG (no chunker / extractor needed for inference)
    kg = KnowledgeGraph(
        client=client,
        embedder=embedder,
        builder_settings=BuilderArguments(
            vectorize_chunks=True,
        ),
    )

    # ── optional: run entity alignment before querying ───────────────
    if args.align:
        print(f"Running entity alignment (threshold={args.align_threshold}) ...")
        merged = await kg.align_entities(similarity_threshold=args.align_threshold)
        print(f"Merged {len(merged)} entity pair(s).")

    # ── choose search engine ─────────────────────────────────────────
    if args.mode == "local":
        engine = LocalSearchEngine(
            client=client,
            knowledge_graph=kg,
            embedder=embedder,
        )
    elif args.mode == "global":
        engine = GlobalSearchEngine(
            client=client,
            knowledge_graph=kg,
            embedder=embedder,
        )
    elif args.mode == "naive":
        engine = NaiveSearchEngine(
            client=client,
            knowledge_graph=kg,
            embedder=embedder,
        )
    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    print(f"\n[{args.mode}] Query: {args.query}\n")

    answer = await engine.a_query(args.query, top_k=args.top_k)
    print("Answer:")
    print(answer)


def parse_args():
    p = argparse.ArgumentParser(description="RAGU inference CLI")
    p.add_argument("--query", "-q", required=True, help="Question to ask")
    p.add_argument(
        "--mode", "-m",
        choices=["local", "global", "naive"],
        default="local",
        help="Search mode (default: local)",
    )
    p.add_argument("--top-k", type=int, default=20, help="Number of top entities/chunks to retrieve")
    p.add_argument("--storage-folder", default="ragu_working_dir_chehov_horse1", help="RAGU working directory")
    p.add_argument("--language", default="russian", help="Output language")

    # Entity alignment
    p.add_argument("--align", action="store_true", help="Run entity alignment before querying")
    p.add_argument("--align-threshold", type=float, default=0.85, help="Alignment similarity threshold")

    # LLM / embedder overrides
    p.add_argument("--llm-model", default=None)
    p.add_argument("--llm-base-url", default=None)
    p.add_argument("--llm-api-key", default=None)
    p.add_argument("--embedder-model", default=None)

    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
