#!/usr/bin/env python3
"""
CLI tool for schema-based GML graph processing.

Reads a RAGU knowledge graph in GML format, validates and normalises
it against the NEREL schema (domain/range constraints for 49 relation
types, 29 entity types), and writes a cleaned graph to a new GML file.

No LLM, no embedder, no network access required — operates entirely
on the graph file.

Usage
-----
::

    python scripts/process_gml.py \\
        --input path/to/input.gml \\
        --output-dir path/to/out \\
        --output-name processed.gml

Options::

    --strict-relations     Remove edges with unknown relation types (default: on)
    --no-strict-relations  Keep edges with unknown relation types
    --strict-entity-types  Remove edges violating domain/range (default: on)
    --no-strict-entity-types  Keep edges even if entity types mismatch
    --normalise            Try to fix unknown types via fuzzy match (default: on)
    --no-normalise         Skip type normalisation
    --keep-orphans         Keep nodes without edges after filtering
    --cutoff 0.4           Minimum similarity for fuzzy type matching (0..1)
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# -- make the repo root importable when executed from scripts/ -----------
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from ragu.graph.schema_verification.gml_processor import (
    GmlGraphProcessor,
    ProcessingReport,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Process a RAGU knowledge graph GML file with NEREL schema verification",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--input", required=True,
        help="Path to the input .gml file",
    )
    parser.add_argument(
        "--output-dir", default=None,
        help="Directory to write the output .gml (default: same as input)",
    )
    parser.add_argument(
        "--output-name", default="processed.gml",
        help="Output file name (default: processed.gml)",
    )
    parser.add_argument(
        "--strict-relations", action="store_true", default=True,
        dest="strict_relations",
        help="Remove edges with unknown relation types (default)",
    )
    parser.add_argument(
        "--no-strict-relations", action="store_false",
        dest="strict_relations",
        help="Keep edges with unknown relation types",
    )
    parser.add_argument(
        "--strict-entity-types", action="store_true", default=True,
        dest="strict_entity_types",
        help="Remove edges violating domain/range constraints (default)",
    )
    parser.add_argument(
        "--no-strict-entity-types", action="store_false",
        dest="strict_entity_types",
        help="Keep edges even if entity types violate constraints",
    )
    parser.add_argument(
        "--normalise", action="store_true", default=True,
        dest="normalise",
        help="Attempt fuzzy normalisation of unknown types (default)",
    )
    parser.add_argument(
        "--no-normalise", action="store_false",
        dest="normalise",
        help="Skip type normalisation",
    )
    parser.add_argument(
        "--keep-orphans", action="store_true", default=False,
        help="Keep nodes without edges after filtering",
    )
    parser.add_argument(
        "--cutoff", type=float, default=0.4,
        help="Fuzzy matching cutoff for type normalisation (0..1, default 0.4)",
    )

    args = parser.parse_args()

    # Resolve paths
    input_path = os.path.abspath(args.input)
    if not os.path.isfile(input_path):
        print(f"Error: input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    if args.output_dir:
        output_dir = os.path.abspath(args.output_dir)
    else:
        output_dir = os.path.dirname(input_path)

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, args.output_name)

    # Process
    processor = GmlGraphProcessor(
        strict_relations=args.strict_relations,
        strict_entity_types=args.strict_entity_types,
        normalise_types=args.normalise,
        remove_orphans=not args.keep_orphans,
        normalisation_cutoff=args.cutoff,
    )

    report = processor.process(input_path, output_path)

    # Print report
    print(report.pretty())
    print(f"\n  Output written to: {output_path}")


if __name__ == "__main__":
    main()
