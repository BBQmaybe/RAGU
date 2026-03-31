#!/usr/bin/env python3
"""
Evaluate entity-merging quality of a knowledge graph against a ground-truth benchmark.

Algorithm
---------
1. Load both GML graphs and extract entity_name → node_label mappings.
2. Build a name universe: the union of all entity names from both graphs.
3. For every unordered pair of names (a, b) in the universe:
     - Find the best-matching node in the BENCHMARK graph for a and b.
     - Find the best-matching node in the EVALUATED graph for a and b.
     - should_merge  = (a and b map to the same benchmark node)
     - was_merged    = (a and b map to the same evaluated node)
     - Classify as TP / FP / FN / TN accordingly.
4. Compute and print precision, recall, accuracy, F1.

Name matching
-------------
Exact match is tried first. If not found and --no-fuzzy is NOT set, the name
is matched to the closest candidate (by difflib SequenceMatcher ratio) above
--threshold. Pairs where either graph yields no mapping are skipped.

Usage
-----
    python scripts/evaluate_merging.py \\
        --benchmark path/to/benchmark.gml \\
        --evaluated path/to/result.gml

    # Stricter fuzzy threshold:
    python scripts/evaluate_merging.py \\
        --benchmark benchmark.gml --evaluated result.gml --threshold 0.85

    # Exact names only (no fuzzy):
    python scripts/evaluate_merging.py \\
        --benchmark benchmark.gml --evaluated result.gml --no-fuzzy
"""

from __future__ import annotations

import argparse
import difflib
import sys
from typing import Dict, List, Optional

import networkx as nx


# ---------------------------------------------------------------------------
# Graph loading
# ---------------------------------------------------------------------------

def load_graph(path: str) -> Dict[str, str]:
    """
    Load a GML knowledge graph and return ``{entity_name: node_label}`` mapping.

    ``node_label`` is the ``label`` attribute (e.g. ``ent-abc123``). If two
    nodes share the same ``entity_name``, the first encountered wins.

    :param path: Path to the GML file.
    :return: Mapping from entity_name to its node label (cluster ID).
    """
    try:
        G = nx.read_gml(path)
    except Exception as exc:
        print(f"Error loading '{path}': {exc}", file=sys.stderr)
        sys.exit(1)

    mapping: Dict[str, str] = {}
    for node_id, data in G.nodes(data=True):
        name = data.get("entity_name", "")
        label = str(data.get("label", node_id))
        if name and name not in mapping:
            mapping[name] = label
    return mapping


# ---------------------------------------------------------------------------
# Name → cluster resolution
# ---------------------------------------------------------------------------

def resolve(
    name: str,
    graph: Dict[str, str],
    threshold: float,
    fuzzy: bool,
) -> Optional[str]:
    """
    Return the cluster ID (node label) that best represents ``name`` in ``graph``.

    :param name: Entity name to look up.
    :param graph: ``{entity_name: cluster_id}`` mapping for one graph.
    :param threshold: Minimum SequenceMatcher ratio for a fuzzy match to count.
    :param fuzzy: If False, only exact matches are accepted.
    :return: Cluster ID string, or None if no acceptable match was found.
    """
    # Exact match first
    if name in graph:
        return graph[name]

    if not fuzzy:
        return None

    # Fuzzy match: find the candidate with the highest similarity ratio
    best_ratio = 0.0
    best_label: Optional[str] = None
    name_lower = name.lower()
    for cand_name, cand_label in graph.items():
        ratio = difflib.SequenceMatcher(None, name_lower, cand_name.lower()).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_label = cand_label

    return best_label if best_ratio >= threshold else None


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------

def evaluate(
    benchmark_path: str,
    evaluated_path: str,
    threshold: float = 0.75,
    fuzzy: bool = True,
    verbose: bool = False,
) -> None:
    """
    Compute and print entity-merging metrics.

    :param benchmark_path: Path to the ground-truth GML file.
    :param evaluated_path: Path to the evaluated GML file.
    :param threshold: Fuzzy matching ratio threshold.
    :param fuzzy: Whether to use fuzzy name matching.
    :param verbose: If True, print every pair classification.
    """
    benchmark = load_graph(benchmark_path)
    evaluated = load_graph(evaluated_path)

    universe: List[str] = sorted(set(benchmark) | set(evaluated))
    n = len(universe)

    print(f"Benchmark graph   : {len(benchmark)} entities")
    print(f"Evaluated graph   : {len(evaluated)} entities")
    print(f"Name universe     : {n} unique names")
    print(f"Total pairs       : {n * (n - 1) // 2}")
    print(f"Fuzzy matching    : {'enabled (threshold=%.2f)' % threshold if fuzzy else 'disabled'}")
    print()

    TP = FP = FN = TN = skipped = 0

    for i in range(n):
        for j in range(i + 1, n):
            a = universe[i]
            b = universe[j]

            b_a = resolve(a, benchmark, threshold, fuzzy)
            b_b = resolve(b, benchmark, threshold, fuzzy)
            e_a = resolve(a, evaluated, threshold, fuzzy)
            e_b = resolve(b, evaluated, threshold, fuzzy)

            # Skip pairs where a mapping couldn't be found in either graph
            if None in (b_a, b_b, e_a, e_b):
                skipped += 1
                continue

            should_merge = (b_a == b_b)
            was_merged   = (e_a == e_b)

            if should_merge and was_merged:
                label = "TP"
                TP += 1
            elif not should_merge and was_merged:
                label = "FP"
                FP += 1
            elif should_merge and not was_merged:
                label = "FN"
                FN += 1
            else:
                label = "TN"
                TN += 1

            if verbose:
                print(f"[{label}] ({a!r}, {b!r})")

    total = TP + FP + FN + TN

    precision = TP / (TP + FP) if (TP + FP) > 0 else 0.0
    recall    = TP / (TP + FN) if (TP + FN) > 0 else 0.0
    accuracy  = (TP + TN) / total if total > 0 else 0.0
    f1        = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    print(f"TP        : {TP}")
    print(f"FP        : {FP}")
    print(f"FN        : {FN}")
    print(f"TN        : {TN}")
    if skipped:
        print(f"Skipped   : {skipped}  (pairs where name couldn't be resolved in one graph)")
    print()
    print(f"Precision : {precision:.4f}")
    print(f"Recall    : {recall:.4f}")
    print(f"Accuracy  : {accuracy:.4f}")
    print(f"F1        : {f1:.4f}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate entity-merging quality against a ground-truth benchmark.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--benchmark", required=True, metavar="FILE",
        help="Ground-truth GML file (correctly merged graph).",
    )
    parser.add_argument(
        "--evaluated", required=True, metavar="FILE",
        help="GML file produced by the method being evaluated.",
    )
    parser.add_argument(
        "--threshold", type=float, default=0.75, metavar="T",
        help="Minimum fuzzy name-matching ratio (0.0–1.0).",
    )
    parser.add_argument(
        "--no-fuzzy", action="store_true",
        help="Disable fuzzy matching; use only exact entity_name comparison.",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Print classification for every pair (TP/FP/FN/TN).",
    )
    args = parser.parse_args()

    evaluate(
        benchmark_path=args.benchmark,
        evaluated_path=args.evaluated,
        threshold=args.threshold,
        fuzzy=not args.no_fuzzy,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
