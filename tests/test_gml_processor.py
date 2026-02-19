"""
Tests for the GML graph processor.

Verifies that:
1. Valid edges pass through unchanged.
2. Invalid relation types are removed (strict) or kept (lenient).
3. Domain/range violations are correctly detected.
4. Entity/relation type normalisation works.
5. Orphan nodes are removed.
6. Edge direction is resolved correctly despite undirected MultiGraph.
"""

import os
import tempfile

import networkx as nx
import pytest

from ragu.graph.schema_verification.gml_processor import (
    GmlGraphProcessor,
    ProcessingReport,
    _best_nerel_entity_type,
    _best_nerel_relation_type,
)


# ======================================================================
# Helpers
# ======================================================================

def _make_test_gml(tmp_path: str, nodes: list[dict], edges: list[dict]) -> str:
    """Build a small MultiGraph, write it as GML, return the path."""
    g = nx.MultiGraph()
    for n in nodes:
        nid = n.pop("id")
        g.add_node(nid, **n)
    for e in edges:
        src = e.pop("source")
        tgt = e.pop("target")
        key = e.pop("key", None)
        g.add_edge(src, tgt, key=key, **e)
    path = os.path.join(tmp_path, "test.gml")
    nx.write_gml(g, path)
    return path


# ======================================================================
# Fuzzy matching tests
# ======================================================================

class TestFuzzyMatching:
    def test_entity_type_exact_uppercase(self):
        assert _best_nerel_entity_type("PERSON") == "PERSON"

    def test_entity_type_close_match(self):
        result = _best_nerel_entity_type("PERSONS")
        assert result == "PERSON"

    def test_entity_type_no_match(self):
        assert _best_nerel_entity_type("XYZABC123") is None

    def test_relation_type_exact(self):
        assert _best_nerel_relation_type("LOCATED_IN") == "LOCATED_IN"

    def test_relation_type_close_match(self):
        result = _best_nerel_relation_type("TELEPORTED_TO")
        # Should match something like LOCATED_IN or another *_TO relation
        assert result is not None

    def test_relation_type_no_match(self):
        assert _best_nerel_relation_type("XYZABC123") is None


# ======================================================================
# Processor tests
# ======================================================================

class TestGmlGraphProcessor:

    def test_valid_edges_pass_through(self, tmp_path):
        """All valid edges should survive processing."""
        nodes = [
            {"id": "n1", "entity_name": "Alice", "entity_type": "PERSON"},
            {"id": "n2", "entity_name": "Bob", "entity_type": "PERSON"},
        ]
        edges = [
            {
                "source": "n1", "target": "n2", "key": "r1",
                "subject_name": "Alice", "object_name": "Bob",
                "relation_type": "KNOWS",
                "description": "Alice knows Bob",
                "relation_strength": 1.0,
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor(strict_relations=True)
        report = processor.process(gml_path, out_path)

        assert report.nodes_before == 2
        assert report.edges_before == 1
        assert report.nodes_after == 2
        assert report.edges_after == 1
        assert report.edges_removed_bad_relation == 0
        assert report.edges_removed_bad_domain == 0

    def test_unknown_relation_removed_in_strict_mode(self, tmp_path):
        nodes = [
            {"id": "n1", "entity_name": "A", "entity_type": "PERSON"},
            {"id": "n2", "entity_name": "B", "entity_type": "PERSON"},
        ]
        edges = [
            {
                "source": "n1", "target": "n2", "key": "r1",
                "subject_name": "A", "object_name": "B",
                "relation_type": "XYZABC_UNKNOWN",
                "description": "test",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor(
            strict_relations=True, normalise_types=False,
        )
        report = processor.process(gml_path, out_path)

        assert report.edges_removed_bad_relation == 1
        assert report.edges_after == 0

    def test_unknown_relation_kept_in_lenient_mode(self, tmp_path):
        nodes = [
            {"id": "n1", "entity_name": "A", "entity_type": "PERSON"},
            {"id": "n2", "entity_name": "B", "entity_type": "PERSON"},
        ]
        edges = [
            {
                "source": "n1", "target": "n2", "key": "r1",
                "subject_name": "A", "object_name": "B",
                "relation_type": "XYZABC_UNKNOWN",
                "description": "test",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor(
            strict_relations=False, normalise_types=False,
        )
        report = processor.process(gml_path, out_path)

        assert report.edges_removed_bad_relation == 0
        assert report.edges_after == 1

    def test_domain_violation_removed(self, tmp_path):
        """AWARD cannot be subject of SPOUSE (needs PERSON)."""
        nodes = [
            {"id": "n1", "entity_name": "Prize", "entity_type": "AWARD"},
            {"id": "n2", "entity_name": "Bob", "entity_type": "PERSON"},
        ]
        edges = [
            {
                "source": "n1", "target": "n2", "key": "r1",
                "subject_name": "Prize", "object_name": "Bob",
                "relation_type": "SPOUSE",
                "description": "test",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor()
        report = processor.process(gml_path, out_path)

        assert report.edges_removed_bad_domain == 1
        assert report.edges_after == 0

    def test_range_violation_removed(self, tmp_path):
        """WORKS_AS range needs PROFESSION, not CITY."""
        nodes = [
            {"id": "n1", "entity_name": "Alice", "entity_type": "PERSON"},
            {"id": "n2", "entity_name": "Paris", "entity_type": "CITY"},
        ]
        edges = [
            {
                "source": "n1", "target": "n2", "key": "r1",
                "subject_name": "Alice", "object_name": "Paris",
                "relation_type": "WORKS_AS",
                "description": "test",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor()
        report = processor.process(gml_path, out_path)

        assert report.edges_removed_bad_range == 1
        assert report.edges_after == 0

    def test_orphan_nodes_removed(self, tmp_path):
        nodes = [
            {"id": "n1", "entity_name": "Alice", "entity_type": "PERSON"},
            {"id": "n2", "entity_name": "Orphan", "entity_type": "PERSON"},
            {"id": "n3", "entity_name": "Bob", "entity_type": "PERSON"},
        ]
        edges = [
            {
                "source": "n1", "target": "n3", "key": "r1",
                "subject_name": "Alice", "object_name": "Bob",
                "relation_type": "KNOWS",
                "description": "test",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor(remove_orphans=True)
        report = processor.process(gml_path, out_path)

        assert report.nodes_removed_orphan == 1
        assert report.nodes_after == 2

    def test_orphan_nodes_kept_when_disabled(self, tmp_path):
        nodes = [
            {"id": "n1", "entity_name": "Alice", "entity_type": "PERSON"},
            {"id": "n2", "entity_name": "Orphan", "entity_type": "PERSON"},
            {"id": "n3", "entity_name": "Bob", "entity_type": "PERSON"},
        ]
        edges = [
            {
                "source": "n1", "target": "n3", "key": "r1",
                "subject_name": "Alice", "object_name": "Bob",
                "relation_type": "KNOWS",
                "description": "test",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor(remove_orphans=False)
        report = processor.process(gml_path, out_path)

        assert report.nodes_removed_orphan == 0
        assert report.nodes_after == 3

    def test_entity_type_normalisation(self, tmp_path):
        """PERSONS should be normalised to PERSON."""
        nodes = [
            {"id": "n1", "entity_name": "Alice", "entity_type": "PERSONS"},
            {"id": "n2", "entity_name": "Bob", "entity_type": "PERSON"},
        ]
        edges = [
            {
                "source": "n1", "target": "n2", "key": "r1",
                "subject_name": "Alice", "object_name": "Bob",
                "relation_type": "KNOWS",
                "description": "test",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor(normalise_types=True)
        report = processor.process(gml_path, out_path)

        assert report.entity_types_normalised == 1
        # Edge should survive because PERSONS was normalised to PERSON
        assert report.edges_after == 1

    def test_edge_direction_resolved_correctly(self, tmp_path):
        """
        LOCATED_IN: subject must be FACILITY/ORG/CITY/DISTRICT/LOCATION.
        Even if MultiGraph iteration swaps u/v, the processor should
        use subject_name to determine the actual subject.
        """
        nodes = [
            {"id": "n1", "entity_name": "Sorbonne", "entity_type": "ORGANIZATION"},
            {"id": "n2", "entity_name": "Paris", "entity_type": "CITY"},
        ]
        edges = [
            {
                "source": "n1", "target": "n2", "key": "r1",
                "subject_name": "Sorbonne", "object_name": "Paris",
                "relation_type": "LOCATED_IN",
                "description": "Sorbonne is in Paris",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor()
        report = processor.process(gml_path, out_path)

        # ORGANIZATION is in LOCATED_IN domain, CITY in range → valid
        assert report.edges_after == 1
        assert report.edges_removed_bad_domain == 0
        assert report.edges_removed_bad_range == 0

    def test_curie_graph_full(self):
        """Integration test on the Curie sample graph."""
        input_path = "tests/test_gml_data/curie_graph.gml"
        if not os.path.exists(input_path):
            pytest.skip("Curie test GML not found")

        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "out.gml")
            processor = GmlGraphProcessor()
            report = processor.process(input_path, out_path)

            assert report.nodes_before == 12
            assert report.edges_before == 13
            # 3 edges removed (FOUNDED_BY domain, TELEPORTED_TO domain, SPOUSE domain)
            assert report.edges_removed_bad_domain == 3
            assert report.edges_removed_bad_range == 0
            assert report.edges_after == 10
            # 2 orphans: Radioactivity (CONCEPT→COUNTRY via normalisation, no edges)
            #            Institut Curie (lost its only FOUNDED_BY edge)
            assert report.nodes_removed_orphan == 2
            assert report.nodes_after == 10

    def test_report_pretty_print(self):
        report = ProcessingReport(
            nodes_before=10, edges_before=15,
            nodes_after=8, edges_after=12,
            edges_removed_bad_relation=1,
            edges_removed_bad_domain=1,
            edges_removed_bad_range=1,
            nodes_removed_orphan=2,
            entity_types_normalised=3,
        )
        text = report.pretty()
        assert "10" in text
        assert "GML Processing Report" in text

    def test_output_gml_is_valid(self, tmp_path):
        """Output GML should be loadable by NetworkX."""
        nodes = [
            {"id": "n1", "entity_name": "Alice", "entity_type": "PERSON"},
            {"id": "n2", "entity_name": "Bob", "entity_type": "PERSON"},
        ]
        edges = [
            {
                "source": "n1", "target": "n2", "key": "r1",
                "subject_name": "Alice", "object_name": "Bob",
                "relation_type": "KNOWS",
                "description": "test",
            },
        ]
        gml_path = _make_test_gml(str(tmp_path), nodes, edges)
        out_path = os.path.join(str(tmp_path), "out.gml")

        processor = GmlGraphProcessor()
        processor.process(gml_path, out_path)

        # Should be loadable without errors
        g = nx.read_gml(out_path)
        assert g.number_of_nodes() == 2
        assert g.number_of_edges() == 1
