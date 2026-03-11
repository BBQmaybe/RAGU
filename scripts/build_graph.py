"""
Build a RAGU knowledge graph from text files.

Usage:
    # Without entity alignment (default)
    python build_graph.py --text-dir text

    # With entity alignment enabled
    python build_graph.py --text-dir text --align

    # With custom alignment threshold
    python build_graph.py --text-dir text --align --align-threshold 0.80
"""

import argparse
import asyncio

from ragu import (
    SimpleChunker,
    KnowledgeGraph,
    BuilderArguments,
    Settings,
    ArtifactsExtractorLLM,
)
from ragu.llm import OpenAIClient
from ragu.embedder import OpenAIEmbedder
from ragu.utils.ragu_utils import read_text_from_files

# ── defaults ─────────────────────────────────────────────────────────────

LLM_MODEL_NAME = "gpt-4o-mini"
LLM_BASE_URL = "https://api.openai.com/v1"
LLM_API_KEY = "api"
EMBEDDER_MODEL_NAME = "text-embedding-3-large"


async def main(args):
    Settings.storage_folder = args.storage_folder
    Settings.language = args.language

    docs = read_text_from_files(args.text_dir)

    chunker = SimpleChunker(max_chunk_size=1000)

    client = OpenAIClient(
        model_name=args.llm_model or LLM_MODEL_NAME,
        base_url=args.llm_base_url or LLM_BASE_URL,
        api_token=args.llm_api_key or LLM_API_KEY,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        cache_flush_every=10,
    )

    artifact_extractor = ArtifactsExtractorLLM(
        client=client,
        do_validation=False,
    )

    embedder = OpenAIEmbedder(
        model_name=args.embedder_model or EMBEDDER_MODEL_NAME,
        base_url=args.llm_base_url or LLM_BASE_URL,
        api_token=args.llm_api_key or LLM_API_KEY,
        dim=3072,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        use_cache=True,
    )

    builder_settings = BuilderArguments(
        use_llm_summarization=True,
        vectorize_chunks=True,
        use_entity_alignment=args.align,
        entity_alignment_threshold=args.align_threshold,
    )

    knowledge_graph = await KnowledgeGraph(
        client=client,
        embedder=embedder,
        chunker=chunker,
        artifact_extractor=artifact_extractor,
        builder_settings=builder_settings,
    ).build_from_docs(docs)

    print("Graph built successfully.")
    if args.align:
        print(f"Entity alignment was enabled (threshold={args.align_threshold}).")


def parse_args():
    p = argparse.ArgumentParser(description="Build RAGU knowledge graph")
    p.add_argument("--text-dir", default="text", help="Directory with text files")
    p.add_argument("--storage-folder", default="ragu_working_dir_chehov_horse1")
    p.add_argument("--language", default="russian")

    # Entity alignment
    p.add_argument("--align", action="store_true", help="Enable entity alignment during build")
    p.add_argument("--align-threshold", type=float, default=0.85, help="Alignment similarity threshold")

    # LLM / embedder overrides
    p.add_argument("--llm-model", default=None)
    p.add_argument("--llm-base-url", default=None)
    p.add_argument("--llm-api-key", default=None)
    p.add_argument("--embedder-model", default=None)

    return p.parse_args()


if __name__ == "__main__":
    asyncio.run(main(parse_args()))
