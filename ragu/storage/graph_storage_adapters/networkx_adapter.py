from __future__ import annotations

import os
from dataclasses import asdict
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Optional,
)

import networkx as nx

from ragu.graph.types import Entity, Relation
from ragu.storage.base_storage import BaseGraphStorage, EdgeSpec


def _entity_to_attrs(e: Entity) -> Dict[str, Any]:
    return dict(
        entity_name=e.entity_name,
        entity_type=e.entity_type,
        description=e.description,
        source_chunk_id=list(e.source_chunk_id),
        documents_id=list(e.documents_id),
        clusters=list(e.clusters),
    )

class NetworkXStorage(BaseGraphStorage):
    """
    NetworkX-based implementation of BaseGraphStorage.
    """

    def __init__(
        self,
        filename: str,
        **kwargs,
    ):
        """
        Initialize a new NetworkXStorage.

        :param filename: Path to a `.gml` file used for persistence.
        """
        loaded = nx.read_gml(filename) if os.path.exists(filename) else nx.MultiGraph()
        self._graph: nx.MultiGraph = loaded if isinstance(loaded, nx.MultiGraph) else nx.MultiGraph(loaded)
        self._where_to_save = filename

    @staticmethod
    def _entity_from_node(entity_id: str, metadata: Dict[str, Any]) -> Entity:
        return Entity(
            id=entity_id,
            entity_name=metadata.get("entity_name"),
            entity_type=metadata.get("entity_type"),
            description=metadata.get("description", ""),
            source_chunk_id=list(metadata.get("source_chunk_id", [])),
            documents_id=list(metadata.get("documents_id", [])),
            clusters=metadata.get("clusters", []),
        )

    @staticmethod
    def _relation_from_edge(u: str, v: str, key: Any, metadata: Dict[str, Any]) -> Relation:
        subject_id = str(u)
        object_id = str(v)
        return Relation(
            subject_id=subject_id,
            object_id=object_id,
            subject_name=metadata.get("subject_name", subject_id),
            object_name=metadata.get("object_name", object_id),
            relation_type=metadata.get("relation_type", "UNKNOWN"),
            description=metadata.get("description", ""),
            relation_strength=float(metadata.get("relation_strength", 1.0)),
            source_chunk_id=list(metadata.get("source_chunk_id", [])),
            id=str(metadata.get("id", key)),
        )

    async def index_done_callback(self) -> None:
        """
        Persist the current graph state to disk in GML format.
        """
        nx.write_gml(self._graph, self._where_to_save)

    async def query_done_callback(self) -> None:
        """
        Callback executed after a query is completed.
        Reserved for potential post-processing hooks.
        """
        pass

    async def index_start_callback(self) -> None:
        """
        Callback executed before indexing starts.
        Reserved for potential setup hooks.
        """
        pass

    async def get_node_edges(self, source_node_id: str) -> List[Relation]:
        """
        Retrieve all edges connected to a given node.

        Each returned :class:`Relation` includes associated metadata
        and node display names when available. Missing nodes are tolerated.

        :param source_node_id: ID of the node whose edges to fetch.
        :return: List of relations connected to the node.
        """
        if not self._graph.has_node(source_node_id):
            return []

        relations: List[Relation] = []
        for u, v, key, metadata in self._graph.edges(source_node_id, keys=True, data=True):
            relation = self._relation_from_edge(str(u), str(v), key, metadata)
            relations.append(relation)

        return relations

    async def edges_degrees(self, edge_specs: List[EdgeSpec]) -> List[int]:
        """
        Retrieve degree values for multiple edges.

        For each edge spec, returns ``degree(subject_id) + degree(object_id)``.
        Returns ``0`` when the relation or either endpoint is missing.

        :param edge_specs: Edge specifications to evaluate.
        :return: Degree sums aligned with ``edge_specs``.
        """
        degrees: List[int] = []
        for subject_id, object_id, relation_id in edge_specs:
            degree = (
                (self._graph.degree(subject_id) if self._graph.has_node(subject_id) else 0)
                + (self._graph.degree(object_id) if self._graph.has_node(object_id) else 0)
            )
            degrees.append(degree)
        return degrees

    async def upsert_nodes(self, nodes: Iterable[Entity]) -> None:
        """
        Insert or update multiple nodes in the graph.

        :param nodes: Iterable of entities to process.
        """
        for node in nodes:

            attrs = _entity_to_attrs(node)
            self._graph.add_node(node.id, **attrs)

    async def get_nodes(self, node_ids: List[str]) -> List[Optional[Entity]]:
        """
        Retrieve multiple nodes by their IDs.

        :param node_ids: List of node identifiers to fetch.
        :return: List of entities (``None`` for missing nodes).
        """
        results: List[Optional[Entity]] = []
        for node_id in node_ids:
            if not self._graph.has_node(node_id):
                results.append(None)
                continue
            data = self._graph.nodes[node_id]
            results.append(Entity(id=node_id, **data))
        return results

    async def delete_nodes(self, node_ids: List[str]) -> None:
        """
        Delete multiple nodes from the graph.

        Cascade removes all connected edges.

        :param node_ids: List of node identifiers to remove.
        """
        for node_id in node_ids:
            if self._graph.has_node(node_id):
                self._graph.remove_node(node_id)

    async def get_edges(self, edge_specs: List[EdgeSpec]) -> List[Optional[Relation]]:
        """
        Retrieve multiple edges by specs.

        :param edge_specs: List of edge specs ``(subject_id, object_id, relation_id)``.
        :return: List of relations (``None`` for missing edges).
        """
        results: List[Optional[Relation]] = []
        for spec in edge_specs:
            u, v, key = spec

            if not self._graph.has_edge(u, v):
                results.append(None)
                continue

            matches = self._graph.get_edge_data(u, v)
            if key:
                matches = [matches.get(key, {})]
            else:
                matches = list(matches.values())

            for edge_data in matches:
                if not edge_data:
                    continue
                payload = dict(edge_data)
                payload["subject_id"] = u
                payload["object_id"] = v
                relation = Relation(**payload)
                results.append(relation)
        return results

    async def upsert_edges(self, edges: List[Relation]) -> None:
        """
        Insert or update multiple edges in the graph.

        :param edges: List of relations to upsert.
        """
        for edge in edges:
            edge_data = asdict(edge)
            edge_data.pop("subject_id", None)
            edge_data.pop("object_id", None)
            edge_key = edge.id
            self._graph.add_edge(edge.subject_id, edge.object_id, key=edge_key, **edge_data)

    async def delete_edges(self, edge_specs: List[EdgeSpec]) -> None:
        """
        Delete multiple edges from the graph.

        :param edge_specs: List of edge specs (subject_id, object_id, relation_id).
        """
        for spec in edge_specs:
            u, v, key = spec
            if not self._graph.has_edge(u, v):
                raise ValueError(f"There's no edge between {u} and {v}")

            if key is not None:
                self._graph.remove_edge(u, v, key=key)
                continue

            edge_dict = self._graph.get_edge_data(u, v, default={})
            keys_to_remove = list(edge_dict.keys())

            for k in keys_to_remove:
                self._graph.remove_edge(u, v, key=k)

    async def get_all_edges_for_nodes(self, node_ids: List[str]) -> List[List[Relation]]:
        """
        Retrieve edges for each given node.

        Returns one relation list per input node ID. No cross-node deduplication
        is performed.

        :param node_ids: List of node identifiers.
        :return: Grouped relations for each node.
        """
        grouped_relations: List[List[Relation]] = []

        for node_id in node_ids:
            node_relations: List[Relation] = []
            if not self._graph.has_node(node_id):
                grouped_relations.append(node_relations)
                continue

            for u, v, key, metadata in self._graph.edges(node_id, keys=True, data=True):
                relation = self._relation_from_edge(str(u), str(v), key, metadata)
                relation.subject_name = self._graph.nodes.get(u, {}).get("entity_name", relation.subject_id)
                relation.object_name = self._graph.nodes.get(v, {}).get("entity_name", relation.object_id)
                node_relations.append(relation)

            grouped_relations.append(node_relations)

        return grouped_relations

    async def get_all_nodes(self) -> List[Entity]:
        """
        Retrieve all nodes in the graph.

        :return: List of all entities.
        """
        entities: List[Entity] = []
        for node_id in self._graph.nodes():
            entity = self._entity_from_node(node_id, dict(self._graph.nodes[node_id]))
            entities.append(entity)
        return entities

    async def get_all_edges(self) -> List[Relation]:
        """
        Retrieve all edges in the graph.

        :return: List of all relations.
        """
        relations: List[Relation] = []

        for u, v, key, metadata in self._graph.edges(keys=True, data=True):
            relation = self._relation_from_edge(str(u), str(v), key, metadata)
            relation.subject_name = self._graph.nodes.get(u, {}).get("entity_name", relation.subject_id)
            relation.object_name = self._graph.nodes.get(v, {}).get("entity_name", relation.object_id)
            relations.append(relation)

        return relations
