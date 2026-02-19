"""
GML-to-GML graph processor.

Reads an existing RAGU knowledge graph from a ``.gml`` file, applies
schema-based verification and normalisation, and writes the processed
graph back as a new ``.gml``.

Processing steps
----------------

1. **Load** — read nodes/edges from the input GML via NetworkX.
2. **Validate relation types** — check every edge's ``relation_type``
   against the NEREL vocabulary; unknown types are either rejected
   (strict) or kept (lenient).
3. **Validate domain/range** — for edges with a known relation type,
   verify that the subject's ``entity_type`` falls within the allowed
   domain and the object's ``entity_type`` falls within the allowed
   range.
4. **Normalise entity types** — optionally map non-NEREL entity types
   to the closest NEREL type using simple string similarity.
5. **Remove orphans** — drop nodes that lost all their edges after
   filtering.
6. **Write** — persist the cleaned graph as a new ``.gml`` file.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import get_close_matches
from typing import List, Tuple

import networkx as nx

from ragu.common.logger import logger
from ragu.graph.schema_verification.schema import (
    ALLOWED_ENTITY_TYPES,
    ALLOWED_RELATION_TYPES,
    RELATION_CONSTRAINTS,
    validate_entity_type,
    validate_relation_type,
    get_allowed_subject_types,
    get_allowed_object_types,
)


# ======================================================================
# Report dataclass
# ======================================================================

@dataclass
class ProcessingReport:
    """Summary statistics produced after GML processing."""
    nodes_before: int = 0
    edges_before: int = 0
    nodes_after: int = 0
    edges_after: int = 0
    edges_removed_bad_relation: int = 0
    edges_removed_bad_domain: int = 0
    edges_removed_bad_range: int = 0
    nodes_removed_orphan: int = 0
    entity_types_normalised: int = 0
    relation_types_normalised: int = 0
    unknown_entity_types: list[str] = field(default_factory=list)
    unknown_relation_types: list[str] = field(default_factory=list)

    def pretty(self) -> str:
        """Format the report as a human-readable string."""
        lines = [
            "=" * 60,
            "  GML Processing Report",
            "=" * 60,
            f"  Nodes:  {self.nodes_before:>6}  ->  {self.nodes_after:>6}  "
            f"(removed {self.nodes_removed_orphan} orphans)",
            f"  Edges:  {self.edges_before:>6}  ->  {self.edges_after:>6}",
            f"    - removed (unknown relation):   {self.edges_removed_bad_relation}",
            f"    - removed (domain violation):   {self.edges_removed_bad_domain}",
            f"    - removed (range violation):    {self.edges_removed_bad_range}",
            f"  Entity types normalised:          {self.entity_types_normalised}",
            f"  Relation types normalised:        {self.relation_types_normalised}",
        ]
        if self.unknown_entity_types:
            lines.append(
                f"  Unknown entity types encountered: "
                f"{sorted(set(self.unknown_entity_types))}"
            )
        if self.unknown_relation_types:
            lines.append(
                f"  Unknown relation types encountered: "
                f"{sorted(set(self.unknown_relation_types))}"
            )
        lines.append("=" * 60)
        return "\n".join(lines)


# ======================================================================
# Normalisation helpers
# ======================================================================

_NEREL_ENTITY_LIST = sorted(ALLOWED_ENTITY_TYPES)
_NEREL_RELATION_LIST = sorted(ALLOWED_RELATION_TYPES)


def _best_nerel_entity_type(raw: str, cutoff: float = 0.4) -> str | None:
    """
    Find the closest NEREL entity type for *raw* using
    ``difflib.get_close_matches``.  Returns ``None`` if nothing is
    close enough.
    """
    matches = get_close_matches(
        raw.upper(), _NEREL_ENTITY_LIST, n=1, cutoff=cutoff
    )
    return matches[0] if matches else None


def _best_nerel_relation_type(raw: str, cutoff: float = 0.4) -> str | None:
    """
    Find the closest NEREL relation type for *raw*.
    """
    matches = get_close_matches(
        raw.upper(), _NEREL_RELATION_LIST, n=1, cutoff=cutoff
    )
    return matches[0] if matches else None


# ======================================================================
# Core processor
# ======================================================================

class GmlGraphProcessor:
    """
    Reads a RAGU knowledge graph from a ``.gml`` file, verifies and
    normalises it against the NEREL schema, and writes a cleaned copy.

    :param strict_relations: When *True*, edges with unknown relation
        types are removed.  When *False*, they pass through.
    :param strict_entity_types: When *True*, edges whose subject/object
        entity types violate domain/range constraints are removed.
    :param normalise_types: When *True*, attempt to map unknown entity
        and relation types to the nearest NEREL type before validation.
    :param remove_orphans: When *True*, remove nodes that have no edges
        after filtering.
    :param normalisation_cutoff: Minimum similarity score (0..1) for
        the fuzzy type normalisation to accept a match.
    """

    def __init__(
        self,
        strict_relations: bool = True,
        strict_entity_types: bool = True,
        normalise_types: bool = True,
        remove_orphans: bool = True,
        normalisation_cutoff: float = 0.4,
    ) -> None:
        self.strict_relations = strict_relations
        self.strict_entity_types = strict_entity_types
        self.normalise_types = normalise_types
        self.remove_orphans = remove_orphans
        self.normalisation_cutoff = normalisation_cutoff

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def process(self, input_path: str, output_path: str) -> ProcessingReport:
        """
        Read *input_path*, apply verification, write *output_path*.

        :param input_path: Path to the input ``.gml`` file.
        :param output_path: Path where the processed ``.gml`` is saved.
        :return: A :class:`ProcessingReport` with statistics.
        """
        graph = self._load_gml(input_path)
        report = ProcessingReport(
            nodes_before=graph.number_of_nodes(),
            edges_before=graph.number_of_edges(),
        )

        logger.info(
            f"[GmlProcessor] Loaded graph: "
            f"{report.nodes_before} nodes, {report.edges_before} edges"
        )

        # Step 1 — Normalise types (optional)
        if self.normalise_types:
            self._normalise_entity_types(graph, report)
            self._normalise_relation_types(graph, report)

        # Step 2 — Validate edges
        self._validate_edges(graph, report)

        # Step 3 — Remove orphan nodes
        if self.remove_orphans:
            self._remove_orphan_nodes(graph, report)

        report.nodes_after = graph.number_of_nodes()
        report.edges_after = graph.number_of_edges()

        # Step 4 — Write output
        self._write_gml(graph, output_path)

        logger.info(
            f"[GmlProcessor] Output: "
            f"{report.nodes_after} nodes, {report.edges_after} edges "
            f"-> {output_path}"
        )
        return report

    # ------------------------------------------------------------------
    # Internal — loading / writing
    # ------------------------------------------------------------------

    @staticmethod
    def _load_gml(path: str) -> nx.MultiGraph:
        """Load a GML file and ensure it's a MultiGraph."""
        loaded = nx.read_gml(path)
        if isinstance(loaded, nx.MultiGraph):
            return loaded
        return nx.MultiGraph(loaded)

    @staticmethod
    def _write_gml(graph: nx.MultiGraph, path: str) -> None:
        """Write the graph to a GML file."""
        nx.write_gml(graph, path)

    # ------------------------------------------------------------------
    # Internal — normalisation
    # ------------------------------------------------------------------

    def _normalise_entity_types(
        self,
        graph: nx.MultiGraph,
        report: ProcessingReport,
    ) -> None:
        """Try to map unknown entity types to the nearest NEREL type."""
        for node_id in list(graph.nodes()):
            data = graph.nodes[node_id]
            etype = data.get("entity_type", "")
            if not etype:
                continue
            if validate_entity_type(etype):
                # Already a valid NEREL type — canonicalise casing
                data["entity_type"] = etype.upper()
                continue
            # Unknown type — try fuzzy match
            best = _best_nerel_entity_type(etype, self.normalisation_cutoff)
            if best:
                logger.debug(
                    f"[GmlProcessor] Entity type normalised: "
                    f"'{etype}' -> '{best}' (node {node_id})"
                )
                data["entity_type"] = best
                report.entity_types_normalised += 1
            else:
                report.unknown_entity_types.append(etype)

    def _normalise_relation_types(
        self,
        graph: nx.MultiGraph,
        report: ProcessingReport,
    ) -> None:
        """Try to map unknown relation types to the nearest NEREL type."""
        for u, v, key, data in list(graph.edges(keys=True, data=True)):
            rtype = data.get("relation_type", "")
            if not rtype:
                continue
            if validate_relation_type(rtype):
                data["relation_type"] = rtype.upper()
                continue
            best = _best_nerel_relation_type(rtype, self.normalisation_cutoff)
            if best:
                logger.debug(
                    f"[GmlProcessor] Relation type normalised: "
                    f"'{rtype}' -> '{best}' (edge {u}->{v})"
                )
                data["relation_type"] = best
                report.relation_types_normalised += 1
            else:
                report.unknown_relation_types.append(rtype)

    # ------------------------------------------------------------------
    # Internal — validation
    # ------------------------------------------------------------------

    def _validate_edges(
        self,
        graph: nx.MultiGraph,
        report: ProcessingReport,
    ) -> None:
        """Remove edges that violate the NEREL schema."""
        edges_to_remove: list[tuple[str, str, str]] = []

        for u, v, key, data in list(graph.edges(keys=True, data=True)):
            rtype = (data.get("relation_type") or "").upper()

            # Check 1: relation type must be known (if strict)
            if rtype and not validate_relation_type(rtype):
                if self.strict_relations:
                    edges_to_remove.append((u, v, key))
                    report.edges_removed_bad_relation += 1
                    logger.debug(
                        f"[GmlProcessor] Edge removed (unknown relation "
                        f"'{rtype}'): {u} -> {v}"
                    )
                continue

            if not rtype:
                continue

            if not self.strict_entity_types:
                continue

            # Check 2: domain/range constraints
            # In an undirected MultiGraph the iteration order of (u, v) is
            # not guaranteed to match the original source/target.  Use the
            # subject_name / object_name stored in edge data to resolve
            # the correct entity types from the graph nodes.
            subject_type, object_type = self._resolve_edge_types(
                graph, u, v, data
            )

            allowed_subj = get_allowed_subject_types(rtype)
            allowed_obj = get_allowed_object_types(rtype)

            # If entity type is unknown/empty — skip constraint check
            subject_ok = True
            if allowed_subj and subject_type:
                subject_ok = subject_type in allowed_subj

            object_ok = True
            if allowed_obj and object_type:
                object_ok = object_type in allowed_obj

            if not subject_ok:
                edges_to_remove.append((u, v, key))
                report.edges_removed_bad_domain += 1
                logger.debug(
                    f"[GmlProcessor] Edge removed (domain violation): "
                    f"'{subject_type}' not in {set(allowed_subj)} "
                    f"for '{rtype}': {u} -> {v}"
                )
            elif not object_ok:
                edges_to_remove.append((u, v, key))
                report.edges_removed_bad_range += 1
                logger.debug(
                    f"[GmlProcessor] Edge removed (range violation): "
                    f"'{object_type}' not in {set(allowed_obj)} "
                    f"for '{rtype}': {u} -> {v}"
                )

        for u, v, key in edges_to_remove:
            graph.remove_edge(u, v, key=key)

    # ------------------------------------------------------------------
    # Internal — edge direction resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_edge_types(
        graph: nx.MultiGraph,
        u: str,
        v: str,
        data: dict,
    ) -> tuple[str, str]:
        """
        Determine the subject and object entity types for an edge.

        Because ``nx.MultiGraph`` is undirected, the iteration order of
        ``(u, v)`` may not reflect the original ``source``/``target``
        in the GML.  This method uses the ``subject_name`` and
        ``object_name`` edge attributes to look up the correct entity
        types from the graph nodes.
        """
        subject_name = data.get("subject_name", "")
        object_name = data.get("object_name", "")

        u_name = graph.nodes[u].get("entity_name", "")
        v_name = graph.nodes[v].get("entity_name", "")

        # Determine which graph node is the subject and which is the object
        if subject_name and u_name == subject_name:
            subject_type = (graph.nodes[u].get("entity_type") or "").upper()
            object_type = (graph.nodes[v].get("entity_type") or "").upper()
        elif subject_name and v_name == subject_name:
            subject_type = (graph.nodes[v].get("entity_type") or "").upper()
            object_type = (graph.nodes[u].get("entity_type") or "").upper()
        elif object_name and u_name == object_name:
            subject_type = (graph.nodes[v].get("entity_type") or "").upper()
            object_type = (graph.nodes[u].get("entity_type") or "").upper()
        elif object_name and v_name == object_name:
            subject_type = (graph.nodes[u].get("entity_type") or "").upper()
            object_type = (graph.nodes[v].get("entity_type") or "").upper()
        else:
            # Fallback: assume u=subject, v=object
            subject_type = (graph.nodes[u].get("entity_type") or "").upper()
            object_type = (graph.nodes[v].get("entity_type") or "").upper()

        return subject_type, object_type

    # ------------------------------------------------------------------
    # Internal — orphan removal
    # ------------------------------------------------------------------

    @staticmethod
    def _remove_orphan_nodes(
        graph: nx.MultiGraph,
        report: ProcessingReport,
    ) -> None:
        """Remove nodes that have no remaining edges."""
        orphans = [n for n in graph.nodes() if graph.degree(n) == 0]
        for n in orphans:
            graph.remove_node(n)
        report.nodes_removed_orphan = len(orphans)
        if orphans:
            logger.debug(
                f"[GmlProcessor] Removed {len(orphans)} orphan nodes"
            )
