#!/usr/bin/env python3
"""
Compare three entity-alignment methods on a pre-built knowledge graph.

Methods evaluated:
  1. lexical       — LexicalEntityAligner: four weighted lexical signals, zero API calls.
  2. difflib+llm   — EntityAligner: difflib candidates + parallel LLM verification.
  3. community+llm — CommunityEntityAligner: Leiden clustering + cosine similarity + LLM.

Input:
  --raw   GML with the unaligned (raw extracted) graph.
  --gold  GML with the ground-truth merged graph.

Output:
  Aligned GML files written to --output-dir (default: ./alignment_results).
  Pair-level precision / recall / accuracy / F1 printed to stdout.

  Evaluation follows the same pair-level scheme as scripts/evaluate_merging.py:
    TP  pair that should merge and did merge
    FP  pair that merged but should not have
    FN  pair that should merge but did not
    TN  pair that correctly stayed separate

Usage:
    python test_all.py \\
        --raw  bm1/knowledge_graph.gml \\
        --gold benchmark_ru/gold.gml
"""

from __future__ import annotations

import argparse
import asyncio
import difflib
import os
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from ragu.common.logger import logger
from ragu.graph.community_entity_aligner import CommunityEntityAligner
from ragu.graph.entity_aligner import EntityAligner
from ragu.graph.lexical_entity_aligner import LexicalEntityAligner
from ragu.graph.types import Entity, Relation
from ragu.models.embedder import EmbedderOpenAI
from ragu.models.llm import LLMOpenAI
from ragu.models.openai import CachedAsyncOpenAI
from ragu.storage.graph_storage_adapters.networkx_adapter import NetworkXStorage


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------

@dataclass
class AlignmentResult:
    name: str
    entities: List[Entity]
    relations: List[Relation]


@dataclass
class Metrics:
    tp: int
    fp: int
    fn: int
    tn: int
    precision: float
    recall: float
    accuracy: float
    f1: float
    skipped: int


@dataclass
class TripleMetrics:
    """
    Triple-level graph-difference metric.

    A canonical triple for a relation is (gold_subj_id, relation_type, gold_obj_id),
    obtained by resolving subject/object names through the gold entity map.
    This captures whether the correct entities are connected by the correct relation
    types after alignment — not just whether the right merges were performed.
    """
    aligned_triples: int
    gold_triples: int
    matched: int
    precision: float
    recall: float
    f1: float
    unresolved: int


# ---------------------------------------------------------------------------
# GML I/O
# ---------------------------------------------------------------------------

async def _load_graph(path: str) -> Tuple[List[Entity], List[Relation]]:
    storage = NetworkXStorage(filename=path)
    entities = await storage.get_all_nodes()
    relations = await storage.get_all_edges()
    return entities, relations


async def _save_graph(entities: List[Entity], relations: List[Relation], path: str) -> None:
    if os.path.exists(path):
        os.remove(path)
    storage = NetworkXStorage(filename=path)
    await storage.upsert_nodes(entities)
    await storage.upsert_edges(relations)
    await storage.index_done_callback()


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def _build_name_map(entities: List[Entity]) -> Dict[str, str]:
    return {e.entity_name: e.id for e in entities}


def _resolve(
    name: str,
    name_map: Dict[str, str],
    threshold: float,
    fuzzy: bool,
) -> Optional[str]:
    if name in name_map:
        return name_map[name]
    if not fuzzy:
        return None
    best_ratio = 0.0
    best_id: Optional[str] = None
    name_lower = name.lower()
    for cand_name, cand_id in name_map.items():
        ratio = difflib.SequenceMatcher(None, name_lower, cand_name.lower()).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_id = cand_id
    return best_id if best_ratio >= threshold else None


def compute_metrics(
    raw_entities: List[Entity],
    aligned_entities: List[Entity],
    gold_entities: List[Entity],
    threshold: float = 0.75,
    fuzzy: bool = True,
) -> Metrics:
    aligned_map = _build_name_map(aligned_entities)
    gold_map    = _build_name_map(gold_entities)
    universe    = sorted({e.entity_name for e in raw_entities})

    tp = fp = fn = tn = skipped = 0
    for i in range(len(universe)):
        for j in range(i + 1, len(universe)):
            a, b = universe[i], universe[j]

            g_a = _resolve(a, gold_map,    threshold, fuzzy)
            g_b = _resolve(b, gold_map,    threshold, fuzzy)
            e_a = _resolve(a, aligned_map, threshold, fuzzy)
            e_b = _resolve(b, aligned_map, threshold, fuzzy)

            if None in (g_a, g_b, e_a, e_b):
                skipped += 1
                continue

            should_merge = g_a == g_b
            was_merged   = e_a == e_b

            if should_merge and was_merged:
                tp += 1
            elif not should_merge and was_merged:
                fp += 1
            elif should_merge and not was_merged:
                fn += 1
            else:
                tn += 1

    total     = tp + fp + fn + tn
    precision = tp / (tp + fp)            if (tp + fp) > 0           else 0.0
    recall    = tp / (tp + fn)            if (tp + fn) > 0           else 0.0
    accuracy  = (tp + tn) / total         if total > 0               else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
    return Metrics(
        tp=tp, fp=fp, fn=fn, tn=tn,
        precision=precision, recall=recall, accuracy=accuracy, f1=f1,
        skipped=skipped,
    )


def compute_triple_metrics(
    aligned_relations: List[Relation],
    gold_entities: List[Entity],
    gold_relations: List[Relation],
    threshold: float = 0.75,
    fuzzy: bool = True,
) -> TripleMetrics:
    """
    Compare relation triples between the aligned graph and the gold graph.

    Each relation is represented as a canonical triple:
        (gold_subject_id, relation_type, gold_object_id)

    For gold relations the IDs are taken directly from the graph.
    For aligned relations each name is resolved to a gold entity via fuzzy matching.
    Precision / recall / F1 are computed on the resulting sets of canonical triples.
    Unresolved counts how many aligned relations had subject or object that could
    not be matched to any gold entity (and were therefore excluded from precision).
    """
    gold_map = _build_name_map(gold_entities)

    gold_triple_set: set = set()
    for r in gold_relations:
        gold_triple_set.add((r.subject_id, r.relation_type, r.object_id))

    aligned_canonical: set = set()
    unresolved = 0
    for r in aligned_relations:
        s = _resolve(r.subject_name, gold_map, threshold, fuzzy)
        o = _resolve(r.object_name,  gold_map, threshold, fuzzy)
        if s is None or o is None:
            unresolved += 1
            continue
        aligned_canonical.add((s, r.relation_type, o))

    matched   = len(aligned_canonical & gold_triple_set)
    precision = matched / len(aligned_canonical) if aligned_canonical else 0.0
    recall    = matched / len(gold_triple_set)   if gold_triple_set   else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0

    return TripleMetrics(
        aligned_triples=len(aligned_canonical),
        gold_triples=len(gold_triple_set),
        matched=matched,
        precision=precision,
        recall=recall,
        f1=f1,
        unresolved=unresolved,
    )


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

_PAIR_COLUMNS = [
    ("Method",    "name",      "s",   20),
    ("Entities",  "entities",  "d",   10),
    ("TP",        "tp",        "d",    5),
    ("FP",        "fp",        "d",    5),
    ("FN",        "fn",        "d",    5),
    ("TN",        "tn",        "d",    7),
    ("Precision", "precision", ".4f", 10),
    ("Recall",    "recall",    ".4f",  8),
    ("Accuracy",  "accuracy",  ".4f", 10),
    ("F1",        "f1",        ".4f",  8),
]

_TRIPLE_COLUMNS = [
    ("Method",       "name",            "s",   20),
    ("Triples",      "aligned_triples", "d",   10),
    ("Gold triples", "gold_triples",    "d",   12),
    ("Matched",      "matched",         "d",    8),
    ("Precision",    "precision",       ".4f", 10),
    ("Recall",       "recall",          ".4f",  8),
    ("F1",           "f1",              ".4f",  8),
    ("Unresolved",   "unresolved",      "d",   12),
]


def _print_table(names: List[str], rows: List[Dict], columns: list) -> None:
    header = " | ".join(f"{h:<{w}}" for h, _, _, w in columns)
    sep    = "-+-".join("-" * w        for _, _, _, w in columns)
    print(header)
    print(sep)
    for name, row in zip(names, rows):
        row["name"] = name
        print(" | ".join(f"{format(row[k], fmt):<{w}}" for _, k, fmt, w in columns))


def _print_report(
    results: List[AlignmentResult],
    metrics: List[Metrics],
    triple_metrics: List[TripleMetrics],
) -> None:
    print("--- Entity pair merging (pair-level) ---")
    _print_table(
        [r.name for r in results],
        [
            {
                "entities":  len(r.entities),
                "tp":        m.tp,
                "fp":        m.fp,
                "fn":        m.fn,
                "tn":        m.tn,
                "precision": m.precision,
                "recall":    m.recall,
                "accuracy":  m.accuracy,
                "f1":        m.f1,
            }
            for r, m in zip(results, metrics)
        ],
        _PAIR_COLUMNS,
    )

    skipped = [(r.name, m.skipped) for r, m in zip(results, metrics) if m.skipped]
    if skipped:
        print()
        for name, n in skipped:
            print(f"  {name}: {n} pair(s) skipped (name unresolvable in gold or aligned)")

    print()
    print("--- Triple-level graph difference ---")
    _print_table(
        [r.name for r in results],
        [
            {
                "aligned_triples": t.aligned_triples,
                "gold_triples":    t.gold_triples,
                "matched":         t.matched,
                "precision":       t.precision,
                "recall":          t.recall,
                "f1":              t.f1,
                "unresolved":      t.unresolved,
            }
            for t in triple_metrics
        ],
        _TRIPLE_COLUMNS,
    )


# ---------------------------------------------------------------------------
# Runner
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
        rate_max_simultaneous=10,
        retry_times_sec=(2, 2, 2, 2, 2),
    )


async def _run(args: argparse.Namespace) -> None:
    print(f"Loading raw graph  : {args.raw}")
    raw_entities, raw_relations = await _load_graph(args.raw)
    print(f"  {len(raw_entities)} entities, {len(raw_relations)} relations")

    print(f"Loading gold graph : {args.gold}")
    gold_entities, gold_relations = await _load_graph(args.gold)
    print(f"  {len(gold_entities)} entities, {len(gold_relations)} relations")
    print()

    client   = _build_client()
    llm      = LLMOpenAI(client, os.environ.get("OPENAI_MODEL", "gpt-4o-mini"))
    embedder = EmbedderOpenAI(
        client,
        os.environ.get("OPENAI_EMBEDDING_MODEL", "text-embedding-3-large"),
        dim=int(os.environ.get("OPENAI_EMBEDDING_DIM", "3072")),
    )

    aligners = [
        LexicalEntityAligner(composite_threshold=args.lexical_threshold, enabled=True),
        EntityAligner(llm=llm, threshold=args.difflib_threshold, enabled=True),
        CommunityEntityAligner(
            llm=llm,
            embedder=embedder,
            cosine_threshold=args.cosine_threshold,
            min_community_size=2,
            enabled=True,
        ),
    ]
    method_names = ["lexical", "difflib+llm", "community+llm"]

    os.makedirs(args.output_dir, exist_ok=True)

    results: List[AlignmentResult] = []
    metrics: List[Metrics]         = []
    triple_metrics: List[TripleMetrics] = []

    for aligner, name in zip(aligners, method_names):
        print(f"Running {name} ...")
        entities, relations = await aligner.run(list(raw_entities), list(raw_relations))
        print(f"  → {len(entities)} entities, {len(relations)} relations")

        out_path = os.path.join(args.output_dir, f"{name.replace('+', '_')}.gml")
        await _save_graph(entities, relations, out_path)
        print(f"  saved → {out_path}")

        m = compute_metrics(
            raw_entities, entities, gold_entities,
            threshold=args.fuzzy_threshold,
            fuzzy=not args.no_fuzzy,
        )
        tm = compute_triple_metrics(
            relations, gold_entities, gold_relations,
            threshold=args.fuzzy_threshold,
            fuzzy=not args.no_fuzzy,
        )
        results.append(AlignmentResult(name=name, entities=entities, relations=relations))
        metrics.append(m)
        triple_metrics.append(tm)

    print()
    _print_report(results, metrics, triple_metrics)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare three entity-alignment methods against a gold-standard graph.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--raw",  required=True, metavar="FILE",
                        help="Raw (unaligned) GML graph.")
    parser.add_argument("--gold", required=True, metavar="FILE",
                        help="Ground-truth GML graph.")
    parser.add_argument("--output-dir", default="alignment_results", metavar="DIR",
                        help="Directory to write aligned GML files.")
    parser.add_argument("--fuzzy-threshold", type=float, default=0.75, metavar="T",
                        help="Name-matching ratio threshold for evaluation.")
    parser.add_argument("--no-fuzzy", action="store_true",
                        help="Disable fuzzy name matching in evaluation.")
    parser.add_argument("--lexical-threshold", type=float, default=0.75, metavar="T",
                        help="Composite score threshold for LexicalEntityAligner.")
    parser.add_argument("--difflib-threshold", type=float, default=0.85, metavar="T",
                        help="SequenceMatcher ratio threshold for EntityAligner.")
    parser.add_argument("--cosine-threshold", type=float, default=0.85, metavar="T",
                        help="Cosine similarity threshold for CommunityEntityAligner.")
    args = parser.parse_args()

    for path in (args.raw, args.gold):
        if not os.path.isfile(path):
            print(f"Error: file not found: {path}", file=sys.stderr)
            sys.exit(1)

    asyncio.run(_run(args))


if __name__ == "__main__":
    main()




