#!/usr/bin/env python3
"""
CLI entry point for the schema-based verification pipeline.

Demonstrates how to plug :class:`SchemaVerificationModule` into a RAGU
:class:`KnowledgeGraph` build and — optionally — run the three-stage
pipeline on a standalone piece of text *without* the full RAGU graph
construction.

All verification uses the internal NEREL schema (domain/range constraints)
and requires no external API calls.

Usage
-----

**Mode 1 — Standalone (text -> verified triplets JSON)**::

    python scripts/run_schema_verification.py standalone \\
        --text "Marie Curie was a Polish-French physicist who won the Nobel Prize." \\
        --llm-model gpt-3.5-turbo \\
        --embedder-model text-embedding-3-small \\
        --base-url https://api.openai.com/v1 \\
        --api-key sk-... \\
        --top-k 5

**Mode 2 — Full pipeline (documents -> knowledge graph with schema
verification as an additional module)**::

    python scripts/run_schema_verification.py pipeline \\
        --docs-dir examples/data/en \\
        --llm-model gpt-3.5-turbo \\
        --embedder-model text-embedding-3-small \\
        --base-url https://api.openai.com/v1 \\
        --api-key sk-... \\
        --language english \\
        --storage-folder ragu_working_dir/schema_verified \\
        --top-k 5

Requirements
~~~~~~~~~~~~
::

    pip install faiss-cpu

This is in addition to the core RAGU dependencies.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

# -- make the repo root importable when executed from scripts/ -----------
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ragu.common.logger import logger
from ragu.embedder.openai_embedder import OpenAIEmbedder
from ragu.graph.schema_verification import SchemaVerificationModule
from ragu.graph.schema_verification.prompts import (
    TripletList,
    build_extraction_messages,
    build_refinement_messages,
)
from ragu.llm.openai_client import OpenAIClient


# ======================================================================
# Standalone mode — run the three stages on a single text
# ======================================================================

async def run_standalone(args: argparse.Namespace) -> None:
    """Execute the full three-stage pipeline on *args.text*."""

    client = OpenAIClient(
        model_name=args.llm_model,
        base_url=args.base_url,
        api_token=args.api_key,
        max_requests_per_second=1,
        max_requests_per_minute=30,
    )
    embedder = OpenAIEmbedder(
        model_name=args.embedder_model,
        base_url=args.base_url,
        api_token=args.api_key,
        dim=args.embedder_dim,
        max_requests_per_second=1,
        max_requests_per_minute=30,
        use_cache=True,
    )

    module = SchemaVerificationModule(
        client=client,
        embedder=embedder,
        enabled=True,
        top_k=args.top_k,
        verify_schema=not args.skip_verification,
        strict_relation=not args.lenient_relations,
    )

    text = args.text

    # -- Step 1 -- extraction --
    logger.info("[Standalone] Step 1: extracting candidate triplets")
    messages = build_extraction_messages(text)
    raw_result = await client.complete(messages=messages, response_model=TripletList)
    if raw_result is None or not isinstance(raw_result, TripletList):
        logger.error("Step 1 returned no triplets")
        return
    raw_triplets = raw_result.triplets
    logger.info(f"[Standalone] Step 1 produced {len(raw_triplets)} triplets")
    _print_triplets("Raw triplets (Step 1)", raw_triplets)

    # -- Retrieval -- FAISS top-k --
    logger.info("[Standalone] Retrieval: finding schema candidates")
    subject_map, relation_map, object_map = await module._retrieval(raw_triplets)

    logger.info("[Standalone] Candidate mappings:")
    for name, cands in {**subject_map, **object_map}.items():
        logger.info(f"  {name} -> {cands}")
    for name, cands in relation_map.items():
        logger.info(f"  (rel) {name} -> {cands}")

    # -- Step 2 -- refinement --
    logger.info("[Standalone] Step 2: LLM refinement")
    messages = build_refinement_messages(
        text=text,
        triplets=raw_triplets,
        subject_mappings=subject_map,
        relation_mappings=relation_map,
        object_mappings=object_map,
    )
    refined_result = await client.complete(messages=messages, response_model=TripletList)
    if refined_result is None or not isinstance(refined_result, TripletList):
        logger.error("Step 2 returned no triplets")
        return
    refined_triplets = refined_result.triplets
    logger.info(f"[Standalone] Step 2 produced {len(refined_triplets)} triplets")
    _print_triplets("Refined triplets (Step 2)", refined_triplets)

    # -- Step 3 -- schema verification --
    if not args.skip_verification:
        logger.info("[Standalone] Step 3: schema verification")
        results = await module._verifier.verify_batch(refined_triplets)
        verified = [vr.triplet for vr in results if vr.is_valid]
        for vr in results:
            status = "PASS" if vr.is_valid else "FAIL"
            logger.info(
                f"  [{status}] ({vr.triplet.subject}, {vr.triplet.relation}, "
                f"{vr.triplet.object}) -- {vr.reason}"
            )
        logger.info(
            f"[Standalone] Step 3: {len(verified)}/{len(refined_triplets)} "
            f"triplets passed"
        )
    else:
        verified = refined_triplets
        logger.info("[Standalone] Step 3 skipped (--skip-verification)")

    _print_triplets("FINAL verified triplets", verified)

    # -- JSON output --
    output = [t.model_dump() for t in verified]
    print("\n=== JSON output ===")
    print(json.dumps(output, ensure_ascii=False, indent=2))

    if args.output:
        Path(args.output).write_text(
            json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        logger.info(f"Written to {args.output}")


# ======================================================================
# Pipeline mode — full RAGU build with the module
# ======================================================================

async def run_pipeline(args: argparse.Namespace) -> None:
    """Build a RAGU knowledge graph with schema verification module."""

    from ragu import (
        ArtifactsExtractorLLM,
        BuilderArguments,
        KnowledgeGraph,
        Settings,
        SimpleChunker,
    )
    from ragu.utils.ragu_utils import read_text_from_files

    Settings.storage_folder = args.storage_folder
    Settings.language = args.language

    docs = read_text_from_files(args.docs_dir)
    if not docs:
        logger.error(f"No documents found in {args.docs_dir}")
        return

    logger.info(f"Loaded {len(docs)} document(s) from {args.docs_dir}")

    client = OpenAIClient(
        model_name=args.llm_model,
        base_url=args.base_url,
        api_token=args.api_key,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        cache_flush_every=10,
    )
    embedder = OpenAIEmbedder(
        model_name=args.embedder_model,
        base_url=args.base_url,
        api_token=args.api_key,
        dim=args.embedder_dim,
        max_requests_per_second=1,
        max_requests_per_minute=60,
        use_cache=True,
    )

    # The schema verification module (no external API calls)
    schema_module = SchemaVerificationModule(
        client=client,
        embedder=embedder,
        enabled=True,
        top_k=args.top_k,
        verify_schema=not args.skip_verification,
        strict_relation=not args.lenient_relations,
    )

    chunker = SimpleChunker(max_chunk_size=1000)
    artifact_extractor = ArtifactsExtractorLLM(client=client, do_validation=False)

    builder_settings = BuilderArguments(
        use_llm_summarization=True,
        vectorize_chunks=True,
        remove_isolated_nodes=True,
    )

    knowledge_graph = KnowledgeGraph(
        client=client,
        embedder=embedder,
        chunker=chunker,
        artifact_extractor=artifact_extractor,
        builder_settings=builder_settings,
        additional_modules=[schema_module],
        language=args.language,
    )

    await knowledge_graph.build_from_docs(docs)
    logger.info(
        f"Knowledge graph built and stored in {Settings.storage_folder}"
    )


# ======================================================================
# Helpers
# ======================================================================

def _print_triplets(title: str, triplets) -> None:
    logger.info(f"\n{'=' * 60}")
    logger.info(f"  {title} ({len(triplets)})")
    logger.info(f"{'=' * 60}")
    for i, t in enumerate(triplets, 1):
        logger.info(f"  {i}. ({t.subject},  {t.relation},  {t.object})")
    logger.info("")


# ======================================================================
# Argument parser
# ======================================================================

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Run the schema-based verification pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = p.add_subparsers(dest="mode", required=True)

    # -- shared arguments ------------------------------------------------
    def _add_common(sp: argparse.ArgumentParser) -> None:
        sp.add_argument("--llm-model", default="gpt-3.5-turbo")
        sp.add_argument("--embedder-model", default="text-embedding-3-small")
        sp.add_argument("--embedder-dim", type=int, default=1536)
        sp.add_argument("--base-url", default="https://api.openai.com/v1")
        sp.add_argument("--api-key", required=True, help="OpenAI API key")
        sp.add_argument("--top-k", type=int, default=5)
        sp.add_argument("--language", default="english")
        sp.add_argument(
            "--skip-verification", action="store_true",
            help="Skip schema verification (Stage 3)",
        )
        sp.add_argument(
            "--lenient-relations", action="store_true",
            help="Accept unknown relation types instead of rejecting them",
        )

    # -- standalone sub-command ------------------------------------------
    sp_sa = sub.add_parser("standalone", help="Run on a single text")
    _add_common(sp_sa)
    sp_sa.add_argument("--text", required=True, help="Input text")
    sp_sa.add_argument("--output", default=None, help="Save JSON to file")

    # -- pipeline sub-command --------------------------------------------
    sp_pl = sub.add_parser("pipeline", help="Full RAGU build with module")
    _add_common(sp_pl)
    sp_pl.add_argument("--docs-dir", required=True, help="Path to docs dir")
    sp_pl.add_argument(
        "--storage-folder",
        default="ragu_working_dir/schema_verified",
    )

    return p


# ======================================================================
# Main
# ======================================================================

def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.mode == "standalone":
        asyncio.run(run_standalone(args))
    elif args.mode == "pipeline":
        asyncio.run(run_pipeline(args))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
