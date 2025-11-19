import asyncio
from typing import List

from ragu.graph.graph_builder_pipeline import InMemoryGraphBuilder
from ragu.graph.types import Entity, Relation, CommunitySummary
from ragu.storage.index import Index

from ragu.common.global_parameters import Settings


# TODO: implement all methods
class KnowledgeGraph:
    def __init__(
            self,
            extraction_pipeline: InMemoryGraphBuilder,
            index: Index,
            make_community_summary: bool = True,
            remove_isolated_nodes: bool = True,
            language: str = "english",
    ):
        self.pipeline = extraction_pipeline
        self.index = index
        self.make_community_summary = make_community_summary
        self.remove_isolated_nodes = remove_isolated_nodes
        self.language = language
        self.pipeline.language = language

        self._id_to_entity_map = {}
        self._id_to_relation_map = {}

        # Initialize storage folder if it doesn't exist
        Settings.init_storage_folder()

    async def build_from_docs(self, docs) -> "KnowledgeGraph":
        entities, relations, chunks = await self.pipeline.extract_graph(docs)

        # Add entities and relations
        await self.add_entity(entities)
        await self.add_relation(relations)

        if self.remove_isolated_nodes:
            await self.index.graph_backend.remove_isolated_nodes()

        # Save chunks
        await self.index.insert_chunks(chunks)
        if self.make_community_summary:
            communities, summaries = await self.high_level_build()
            await self.index._insert_communities(communities)
            await self.index._insert_summaries(summaries)

        return self

    async def high_level_build(self):
        communities = await self.index.graph_backend.cluster()
        summaries = await self.pipeline.get_community_summary(
            communities=communities
        )
        return communities, summaries

    # entity CRUD
    async def add_entity(self, entities: Entity | List[Entity]) -> "KnowledgeGraph":
        if isinstance(entities, Entity):
            entities = [entities]

        batch_entities_to_vdb = []
        for entity in entities:
            if await self.index.graph_backend.has_node(entity.id):
                entity_to_merge: Entity = await self.index.graph_backend.get_node(entity.id)
                entity_to_past = Entity(
                    id=entity_to_merge.id,
                    entity_name=entity_to_merge.entity_name,
                    entity_type=entity_to_merge.entity_type,
                    description=entity_to_merge.description + entity.description,
                    clusters=entity_to_merge.clusters + entity.clusters,
                    source_chunk_id=list(set(entity_to_merge.source_chunk_id + entity.source_chunk_id)),
                )
            else:
                entity_to_past = entity
            batch_entities_to_vdb.append(entity_to_past)
            await self.index.graph_backend.upsert_node(entity_to_past)
        await self.index.make_index(entities=batch_entities_to_vdb)
        return self

    async def get_entity(self, entity_id) -> Entity | None:
        if await self.index.graph_backend.has_node(entity_id):
            return await self.index.graph_backend.get_node(entity_id)
        return None

    async def delete_entity(self, entity_id) -> "KnowledgeGraph":
        self.index.graph_backend.delete_node(entity_id)
        return self

    async def update_entity(self, entity_id, new_entity) -> "KnowledgeGraph":
        ...

    # relation CRUD
    async def add_relation(self, relation: Relation | List[Relation]) -> "KnowledgeGraph":
        if isinstance(relation, Relation):
            relation = [relation]

        relations_to_past = []
        for relation in relation:
            relation_to_merge: Relation = await self.index.graph_backend.get_edge(
                relation.subject_id,
                relation.object_id
            )
            if relation_to_merge:
                relation_to_past = Relation(
                    subject_id=relation_to_merge.subject_id,
                    object_id=relation_to_merge.object_id,
                    subject_name=relation_to_merge.subject_name,
                    object_name=relation_to_merge.object_name,
                    description=relation_to_merge.description + relation.description,
                    relation_strength=sum([relation_to_merge.relation_strength, relation.relation_strength]) * 0.5,
                    source_chunk_id=list(set(relation_to_merge.source_chunk_id + relation.source_chunk_id)),
                )
            else:
                relation_to_past = relation
            relations_to_past.append(relation_to_past)
            await self.index.graph_backend.upsert_edge(relation_to_past)
        await self.index.make_index(relations=relations_to_past)

        return self

    async def get_relation(self, subject_id, object_id) -> Relation | None:
        return await self.index.graph_backend.get_edge(subject_id, object_id)

    async def delete_relation(self, subject_id, object_id) -> "KnowledgeGraph":
        await self.index.graph_backend.delete_edge(subject_id, object_id)

    async def update_relation(self, relation_id, new_relation) -> "KnowledgeGraph":
        ...

    async def get_all_entity_relations(self, entity_id) -> List[Relation] | None:
        if await self.index.graph_backend.has_node(entity_id):
            return await self.index.graph_backend.get_node_edges(entity_id)
        return None

    # summary CRUD
    async def add_summary(self, summary: CommunitySummary | List[CommunitySummary] | None) -> "KnowledgeGraph":
        if summary is None:
            return self
        if isinstance(summary, CommunitySummary):
            summaries: List[CommunitySummary] = [summary]
        elif isinstance(summary, list):
            summaries = [s for s in summary if isinstance(s, CommunitySummary)]
            if not summaries:
                return self
        else:
            return self

        storage = getattr(self.index, "community_summary_kv_storage", None)
        if storage is not None:
            try:
                payload = {s.id: s.summary for s in summaries}
                await storage.upsert(payload)                      # upsert new records
                await storage.index_done_callback()               # persist changes to disk
            except Exception:
                pass
        return self

    async def get_summary(self, summary_id: str | None) -> CommunitySummary | None:
        if not summary_id:
            return None
        storage = getattr(self.index, "community_summary_kv_storage", None)
        if storage is None:
            return None
        try:
            summary_text = await storage.get_by_id(summary_id)
        except Exception:
            summary_text = None
        if summary_text is None:
            return None
        return CommunitySummary(id=summary_id, summary=summary_text)

    async def delete_summary(self, summary_id: str | None) -> "KnowledgeGraph":
        if not summary_id:
            return self
        storage = getattr(self.index, "community_summary_kv_storage", None)
        if storage is not None:
            try:
                existing = await storage.get_by_id(summary_id)
                if existing is not None and hasattr(storage, "data"):
                    storage.data.pop(summary_id, None)
                    await storage.index_done_callback()            # persist deletion
            except Exception:
                pass
        return self

    async def update_summary(self, summary_id: str | None, new_summary: CommunitySummary | str | None) -> "KnowledgeGraph":
        if summary_id is None or new_summary is None:
            return self
        summary_text = new_summary.summary if isinstance(new_summary, CommunitySummary) else str(new_summary)
        storage = getattr(self.index, "community_summary_kv_storage", None)
        if storage is not None:
            try:
                await storage.upsert({summary_id: summary_text})  # upsert updated text
                await storage.index_done_callback()               # persist update
            except Exception:
                pass
        return self

    # fallback similarity using embedder instead of fuzzy matching
    async def _simple_similar_entities_by_query(self, query: str, exclude_id: str | None = None, top_k: int = 5) -> List[Entity]:
        if not query:
            return []
        
        # gather candidate entities from in-memory map or from the NetworkX graph
        candidates: List[Entity] = []
        if self._id_to_entity_map:
            for ent_id, ent in self._id_to_entity_map.items():
                if exclude_id and ent_id == exclude_id:
                    continue
                candidates.append(ent)
        else:
            backend = getattr(self.index, "graph_backend", None)
            graph = getattr(backend, "_graph", None)
            if graph is not None:
                for node_id, attrs in graph.nodes(data=True):
                    if exclude_id and str(node_id) == exclude_id:
                        continue
                    candidates.append(Entity(
                        id=str(node_id),
                        entity_name=attrs.get("entity_name", str(node_id)),
                        entity_type=attrs.get("entity_type", "Unknown"),
                        description=attrs.get("description", ""),
                        source_chunk_id=list(attrs.get("source_chunk_id", [])),
                        documents_id=list(attrs.get("documents_id", [])),
                        clusters=list(attrs.get("clusters", [])),
                    ))
        if not candidates:
            return []
        candidate_texts = [f"{c.entity_name} - {c.description}" for c in candidates]
        texts = [query] + candidate_texts
        try:
            embeddings = await self.index.embedder.embed(texts)
            import numpy as np
            emb_array = np.array(embeddings)
            query_vec = emb_array[0]
            candidate_vecs = emb_array[1:]
            q_norm = np.linalg.norm(query_vec) + 1e-8
            cand_norms = np.linalg.norm(candidate_vecs, axis=1) + 1e-8
            scores = (candidate_vecs @ query_vec) / (cand_norms * q_norm)
            ranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
            return [e for _, e in ranked[:top_k]]
        except Exception:
            # fallback to plain string similarity if embeddings unavailable
            from difflib import SequenceMatcher as SM
            q_lower = query.lower()
            ranked = sorted(
                [(SM(None, q_lower, (c.entity_name + " - " + c.description).lower()).ratio(), c) for c in candidates],
                key=lambda x: x[0],
                reverse=True
            )
            return [e for _, e in ranked[:top_k]]

    async def _simple_similar_relations_by_query(self, query: str, top_k: int = 5) -> List[Relation]:
        if not query:
            return []
        
        candidates: List[Relation] = []
        if self._id_to_relation_map:
            candidates = list(self._id_to_relation_map.values())
        else:
            backend = getattr(self.index, "graph_backend", None)
            graph = getattr(backend, "_graph", None)
            if graph is not None:
                for u, v, attrs in graph.edges(data=True):
                    candidates.append(Relation(
                        subject_id=str(u),
                        object_id=str(v),
                        subject_name=graph.nodes.get(u, {}).get("entity_name", str(u)),
                        object_name=graph.nodes.get(v, {}).get("entity_name", str(v)),
                        description=attrs.get("description", ""),
                        relation_strength=float(attrs.get("relation_strength", 1.0)),
                        source_chunk_id=list(attrs.get("source_chunk_id", [])),
                        id=attrs.get("id"),
                    ))
        if not candidates:
            return []
        candidate_texts = [rel.description or "" for rel in candidates]
        texts = [query] + candidate_texts
        try:
            embeddings = await self.index.embedder.embed(texts)
            import numpy as np
            emb_array = np.array(embeddings)
            query_vec = emb_array[0]
            candidate_vecs = emb_array[1:]
            q_norm = np.linalg.norm(query_vec) + 1e-8
            cand_norms = np.linalg.norm(candidate_vecs, axis=1) + 1e-8
            scores = (candidate_vecs @ query_vec) / (cand_norms * q_norm)
            ranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
            return [r for _, r in ranked[:top_k]]
        except Exception:
            from difflib import SequenceMatcher as SM
            q_lower = query.lower()
            ranked = sorted(
                [(SM(None, q_lower, r.description.lower()).ratio(), r) for r in candidates],
                key=lambda x: x[0],
                reverse=True
            )
            return [r for _, r in ranked[:top_k]]

    # similarity-search API methods
    async def find_similar_entities(self, entity: Entity | None, top_k: int = 5) -> List[Entity]:
        if entity is None:
            return []
        
        query_string = f"{entity.entity_name} - {entity.description}".strip()
        storage = getattr(self.index, "entity_vector_db", None)
        if storage is not None:
            try:
                results = await storage.query(query_string, top_k=top_k + 1)
                entities: List[Entity] = []
                for item in results:
                    ent_id = item.get("id") or item.get("__id__") or item.get("__id__")
                    if ent_id is None or ent_id == entity.id:
                        continue
                    ent = await self.get_entity(ent_id)
                    if ent is not None:
                        entities.append(ent)
                        if len(entities) >= top_k:
                            break
                if entities:
                    return entities
            except Exception:
                pass
        return await self._simple_similar_entities_by_query(query_string, exclude_id=entity.id, top_k=top_k)

    async def find_similar_relations(self, relation: Relation | None, top_k: int = 5) -> List[Relation]:
        if relation is None:
            return []
        query_string = relation.description or ""
        storage = getattr(self.index, "relation_vector_db", None)
        if storage is not None:
            try:
                results = await storage.query(query_string, top_k=top_k + 1)
                rels: List[Relation] = []
                for item in results:
                    rel_id = item.get("id") or item.get("__id__") or item.get("__id__")
                    if rel_id is None or rel_id == relation.id:
                        continue
                    subject_id = item.get("subject")
                    object_id = item.get("object")
                    if subject_id and object_id:
                        rel_obj = await self.get_relation(subject_id, object_id)
                    else:
                        rel_obj = None
                        backend = getattr(self.index, "graph_backend", None)
                        graph = getattr(backend, "_graph", None)
                        if graph is not None:
                            for u, v, attrs in graph.edges(data=True):
                                if attrs.get("id") == rel_id:
                                    rel_obj = Relation(
                                        subject_id=str(u),
                                        object_id=str(v),
                                        subject_name=graph.nodes.get(u, {}).get("entity_name", str(u)),
                                        object_name=graph.nodes.get(v, {}).get("entity_name", str(v)),
                                        description=attrs.get("description", ""),
                                        relation_strength=float(attrs.get("relation_strength", 1.0)),
                                        source_chunk_id=list(attrs.get("source_chunk_id", [])),
                                        id=rel_id,
                                    )
                                    break
                    if rel_obj is not None:
                        rels.append(rel_obj)
                        if len(rels) >= top_k:
                            break
                if rels:
                    return rels
            except Exception:
                pass
        return await self._simple_similar_relations_by_query(query_string, top_k=top_k)

    async def find_similar_entity_by_query(self, query: str | None, top_k: int = 5) -> List[Entity]:
        if not query:
            return []
        
        storage = getattr(self.index, "entity_vector_db", None)
        if storage is not None:
            try:
                results = await storage.query(query, top_k=top_k)
                entities: List[Entity] = []
                for item in results:
                    ent_id = item.get("id") or item.get("__id__") or item.get("__id__")
                    if ent_id is None:
                        continue
                    ent = await self.get_entity(ent_id)
                    if ent is not None:
                        entities.append(ent)
                        if len(entities) >= top_k:
                            break
                if entities:
                    return entities
            except Exception:
                pass
        return await self._simple_similar_entities_by_query(query, exclude_id=None, top_k=top_k)

    async def find_similar_relation_by_query(self, query: str | None, top_k: int = 5) -> List[Relation]:
        if not query:
            return []
        
        storage = getattr(self.index, "relation_vector_db", None)
        if storage is not None:
            try:
                results = await storage.query(query, top_k=top_k)
                rels: List[Relation] = []
                for item in results:
                    rel_id = item.get("id") or item.get("__id__") or item.get("__id__")
                    if rel_id is None:
                        continue
                    subject_id = item.get("subject")
                    object_id = item.get("object")
                    if subject_id and object_id:
                        rel_obj = await self.get_relation(subject_id, object_id)
                    else:
                        rel_obj = None
                        backend = getattr(self.index, "graph_backend", None)
                        graph = getattr(backend, "_graph", None)
                        if graph is not None:
                            for u, v, attrs in graph.edges(data=True):
                                if attrs.get("id") == rel_id:
                                    rel_obj = Relation(
                                        subject_id=str(u),
                                        object_id=str(v),
                                        subject_name=graph.nodes.get(u, {}).get("entity_name", str(u)),
                                        object_name=graph.nodes.get(v, {}).get("entity_name", str(v)),
                                        description=attrs.get("description", ""),
                                        relation_strength=float(attrs.get("relation_strength", 1.0)),
                                        source_chunk_id=list(attrs.get("source_chunk_id", [])),
                                        id=rel_id,
                                    )
                                    break
                    if rel_obj is not None:
                        rels.append(rel_obj)
                        if len(rels) >= top_k:
                            break
                if rels:
                    return rels
            except Exception:
                pass
        return await self._simple_similar_relations_by_query(query, top_k=top_k)

    async def edge_degree(self, subject_id, object_id) -> int | None:
        if await self.index.graph_backend.has_edge(subject_id, object_id):
            return await self.index.graph_backend.get_edge_degree(subject_id, object_id)
        else:
            return None

    async def get_neighbors(self, entity_id) -> List[Entity]:
        if await self.index.graph_backend.has_node(entity_id):
            relations = await self.index.graph_backend.get_node_edges(entity_id)
            neighbors_candidates = await asyncio.gather(*[self.get_entity(relation.object_id) for relation in relations])
            return [neighbor for neighbor in neighbors_candidates if neighbor]
        else:
            return []
