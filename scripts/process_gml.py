#!/usr/bin/env python3
"""
CLI for processing a GML knowledge graph with SchemaVerificationModule.

Reads an input GML, runs two-step extraction + type-based verification,
and saves the resulting graph to a new GML file.

Usage:
    python scripts/process_gml.py \\
        --input  path/to/input.gml \\
        --output-dir path/to/out \\
        --output-name processed.gml

Environment variables required:
    OPENAI_API_KEY          — API key for the LLM / embedding service.

Optional environment variables:
    OPENAI_BASE_URL         — Override the base URL (default: OpenAI v1).
    OPENAI_MODEL            — Chat model name (default: gpt-4o-mini).
    OPENAI_EMBEDDING_MODEL  — Embedding model (default: text-embedding-3-large).
    OPENAI_EMBEDDING_DIM    — Embedding dimension (default: 3072).
"""

import argparse
import asyncio
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Ensure the project root is importable when running the script directly.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ragu.graph.schema_verification import SchemaVerificationModule
from ragu.models.embedder import EmbedderOpenAI
from ragu.models.llm import LLMOpenAI
from ragu.models.openai import CachedAsyncOpenAI
from ragu.storage.graph_storage_adapters.networkx_adapter import NetworkXStorage


async def _process(
    input_path: str,
    output_path: str,
    top_k: int,
    batch_size: int,
) -> None:
    # ---- load input graph ------------------------------------------------
    storage_in = NetworkXStorage(filename=input_path)
    entities = await storage_in.get_all_nodes()
    relations = await storage_in.get_all_edges()

    n_ent_before = len(entities)
    n_rel_before = len(relations)
    print(f"Input : {n_ent_before} nodes, {n_rel_before} edges  ({input_path})")

    # ---- set up LLM + Embedder -------------------------------------------
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("Error: OPENAI_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    client = CachedAsyncOpenAI(
        base_url=os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1"),
        api_key=api_key,
        rate_min_delay=2,
        rate_max_simultaneous=5,
        retry_times_sec=(2, 2, 2),
    )
    llm = LLMOpenAI(client, os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
    embedder = EmbedderOpenAI(
        client,
        os.environ.get("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"),
        dim=int(os.environ.get("OPENAI_EMBEDDING_DIM", "3072")),
    )

    # ---- run schema verification -----------------------------------------
    module = SchemaVerificationModule(
        llm=llm,
        embedder=embedder,
        top_k=top_k,
        batch_size=batch_size,
        enabled=True,
    )
    result_entities, result_relations = await module.run(entities, relations)

    n_ent_after = len(result_entities)
    n_rel_after = len(result_relations)

    print(f"Output: {n_ent_after} nodes, {n_rel_after} edges  ({output_path})")
    print(
        f"Removed: {n_ent_before - n_ent_after} nodes, "
        f"{n_rel_before - n_rel_after} edges"
    )

    # ---- save output graph -----------------------------------------------
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    storage_out = NetworkXStorage(filename=output_path)
    await storage_out.upsert_nodes(result_entities)
    await storage_out.upsert_edges(result_relations)
    await storage_out.index_done_callback()

    print(f"Saved to: {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process a GML knowledge graph with schema verification.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--input", required=True, help="Path to input GML file.")
    parser.add_argument("--output-dir", required=True, help="Directory for output GML.")
    parser.add_argument("--output-name", default="processed.gml", help="Output file name.")
    parser.add_argument(
        "--top-k", type=int, default=5, metavar="K",
        help="Candidate count per triplet component for FAISS retrieval.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=40, metavar="N",
        help="Max relations per LLM call.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    output_path = os.path.join(args.output_dir, args.output_name)

    asyncio.run(
        _process(
            input_path=args.input,
            output_path=output_path,
            top_k=args.top_k,
            batch_size=args.batch_size,
        )
    )


if __name__ == "__main__":
    main()
