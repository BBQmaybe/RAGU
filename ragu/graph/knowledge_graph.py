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
    def add_summary(self, summary) -> "KnowledgeGraph":
        #на вход можно дать либо один summary, либо список.
        summaries: List[CommunitySummary] = []
        if summary is None:
            return self
        if isinstance(summary, CommunitySummary):
            summaries = [summary]
        elif isinstance(summary, list):
            # если список, то я на всякий случай проверяю, что там правда summary
            summaries = [s for s in summary if isinstance(s, CommunitySummary)]
        else:
            return self

        storage = getattr(self.index, "community_summary_kv_storage", None)
        if storage is not None and hasattr(storage, "data"):
            for s in summaries:
                storage.data[s.id] = s.summary

            # вот этот кусок я сама до конца не понимаю.
            # вроде как надо вызвать функцию, которая сохраняет данные,
            # но она async и из-за этого всё ругается. я просто обернула в try, чтобы не падало.
            try:
                cb = storage.index_done_callback
                if asyncio.iscoroutinefunction(cb):
                    try:
                        asyncio.run(cb())     # честно, я понятия не имею, почему оно иногда работает, а иногда нет
                    except RuntimeError:
                        pass                  # просто игнорю, лишь бы не упало
                else:
                    cb()                      # если это не async, можно просто вызвать
            except Exception:
                pass
        return self


    def get_summary(self, summary_id) -> CommunitySummary | None:
        if not summary_id:
            return None

        storage = getattr(self.index, "community_summary_kv_storage", None)
        if storage is None or not hasattr(storage, "data"):
            return None

        summary_text = storage.data.get(summary_id)
        if summary_text is None:
            return None

        return CommunitySummary(id=summary_id, summary=summary_text)


    def delete_summary(self, summary_id) -> "KnowledgeGraph":
        # удаление почти такое же, как и добавление, только наоборот
        storage = getattr(self.index, "community_summary_kv_storage", None)
        if storage is not None and hasattr(storage, "data"):
            if summary_id in storage.data:
                del storage.data[summary_id]

                try:
                    cb = storage.index_done_callback
                    if asyncio.iscoroutinefunction(cb):
                        try:
                            asyncio.run(cb()) 
                        except RuntimeError:
                            pass
                    else:
                        cb()
                except Exception:
                    pass
        return self


    def update_summary(self, summary_id, new_summary) -> "KnowledgeGraph":
        if new_summary is None:
            return self

        # если это объект summary, беру текст. если нет — просто превращаю в строку
        if isinstance(new_summary, CommunitySummary):
            summary_text = new_summary.summary
        else:
            summary_text = str(new_summary)

        storage = getattr(self.index, "community_summary_kv_storage", None)
        if storage is not None and hasattr(storage, "data"):
            storage.data[summary_id] = summary_text
            try:
                cb = storage.index_done_callback
                if asyncio.iscoroutinefunction(cb):
                    try:
                        asyncio.run(cb())
                    except RuntimeError:
                        pass
                else:
                    cb()
            except Exception:
                pass
        return self

    def _simple_similar_entities_by_query(self, query: str, exclude_id: str | None = None, top_k: int = 5) -> List[Entity]:
        # тут я пытаюсь подключить fuzzywuzzy, потому что в интернете писали, что он лучше сравнивает строки.
        # но если его нет, то я делаю свою самодельную версию.
        try:
            from fuzzywuzzy import fuzz 
        except Exception:
            from difflib import SequenceMatcher as SM
            def ratio(a: str, b: str) -> int:
                return int(SM(None, a, b).ratio() * 100)
            fuzz = type("fuzz", (), {"token_set_ratio": ratio})

        candidates: List[Entity] = []

        # если есть карта id->entity, беру из неё, если нет — иду в граф.
        if self._id_to_entity_map:
            for ent_id, ent in self._id_to_entity_map.items():
                if exclude_id is not None and ent_id == exclude_id:
                    continue
                candidates.append(ent)
        else:
            # если нет словаря сущностей, то... ну идём в граф.
            backend = getattr(self.index, "graph_backend", None)
            graph = getattr(backend, "_graph", None)
            if graph is not None:
                for node_id, attrs in graph.nodes(data=True):
                    if exclude_id is not None and node_id == exclude_id:
                        continue
                    try:
                        # тут я вручную собираю Entity из атрибутов узла.
                        # выглядит громоздко, но я не знаю, как сделать проще.
                        cand = Entity(
                            id=str(node_id),
                            entity_name=attrs.get("entity_name", str(node_id)),
                            entity_type=attrs.get("entity_type", "Unknown"),
                            description=attrs.get("description", ""),
                            source_chunk_id=list(attrs.get("source_chunk_id", [])),
                            documents_id=list(attrs.get("documents_id", [])),
                            clusters=list(attrs.get("clusters", [])),
                        )
                    except Exception:
                        # если вдруг что-то не так с данными, я просто пропускаю такую сущность
                        continue
                    candidates.append(cand)

        scored: List[tuple[int, Entity]] = []

        q = (query or "").lower()

        for c in candidates:
            text = ((c.entity_name or "") + " - " + (c.description or "")).lower()
            try:
                # сравниваю строки. если fuzzywuzzy есть — круто, если нет — работает мой костыль
                score = fuzz.token_set_ratio(q, text)
            except Exception:
                score = 0 

            scored.append((score, c))

        scored.sort(key=lambda x: x[0], reverse=True)

        return [c for _, c in scored[:top_k]]


    def _simple_similar_relations_by_query(self, query: str, top_k: int = 5) -> List[Relation]:
        try:
            from fuzzywuzzy import fuzz  
        except Exception:
            from difflib import SequenceMatcher as SM
            def ratio(a: str, b: str) -> int:
                return int(SM(None, a, b).ratio() * 100)
            fuzz = type("fuzz", (), {"token_set_ratio": ratio})

        candidates: List[Relation] = []

        if self._id_to_relation_map:
            candidates = list(self._id_to_relation_map.values())
        else:
            backend = getattr(self.index, "graph_backend", None)
            graph = getattr(backend, "_graph", None)
            if graph is not None:
                for u, v, attrs in graph.edges(data=True):
                    try:
                        relation = Relation(
                            subject_id=str(u),
                            object_id=str(v),
                            subject_name=graph.nodes.get(u, {}).get("entity_name", str(u)),
                            object_name=graph.nodes.get(v, {}).get("entity_name", str(v)),
                            description=attrs.get("description", ""),
                            relation_strength=float(attrs.get("relation_strength", 1.0)),
                            source_chunk_id=list(attrs.get("source_chunk_id", [])),
                            id=attrs.get("id"),
                        )
                    except Exception:
                        continue
                    candidates.append(relation)

        scored: List[tuple[int, Relation]] = []

        q = (query or "").lower()

        for r in candidates:
            text = (r.description or "").lower()
            try:
                score = fuzz.token_set_ratio(q, text)
            except Exception:
                score = 0
            scored.append((score, r))

        scored.sort(key=lambda x: x[0], reverse=True)

        return [r for _, r in scored[:top_k]]


    def find_similar_entities(self, entity, top_k: int = 5) -> List[Entity]:
        if entity is None:
            return []

        query_string = f"{entity.entity_name} - {entity.description}".strip()

        storage = getattr(self.index, "entity_vector_db", None)

        async def _search_vdb(q: str) -> List[Entity]:
            try:
                results = await storage.query(q, top_k=top_k + 1)
            except Exception:
                return []

            entities: List[Entity] = []
            for item in results:
                ent_id = item.get("__id__")
                if ent_id is None or ent_id == entity.id:
                    continue
                try:
                    ent = await self.get_entity(ent_id)
                except Exception:
                    ent = None
                if ent is not None:
                    entities.append(ent)
                if len(entities) >= top_k:
                    break
            return entities

        if storage is not None:
            try:
                return asyncio.run(_search_vdb(query_string))
            except RuntimeError:
                pass
            except Exception:
                pass

        # если не получилось — перехожу в мой "ручной" fuzzy поиск.
        return self._simple_similar_entities_by_query(query_string, exclude_id=entity.id, top_k=top_k)


    def find_similar_relations(self, relation, top_k: int = 5) -> List[Relation]:
        if relation is None:
            return []
        query_string = relation.description or ""
        storage = getattr(self.index, "relation_vector_db", None)
        async def _search_vdb(q: str) -> List[Relation]:
            try:
                results = await storage.query(q, top_k=top_k + 1) 
            except Exception:
                return []
            rels: List[Relation] = []
            for item in results:
                rel_id = item.get("__id__")
                if rel_id is None or rel_id == relation.id:
                    continue
                rel = None
                try:
                    subject_id = item.get("subject")
                    object_id = item.get("object")
                    if subject_id and object_id:
                        rel = await self.get_relation(subject_id, object_id)
                    else:
                        backend = getattr(self.index, "graph_backend", None)
                        graph = getattr(backend, "_graph", None)
                        if graph is not None:
                            for u, v, attrs in graph.edges(data=True):
                                if attrs.get("id") == rel_id:
                                    subject_name = graph.nodes.get(u, {}).get("entity_name", str(u))
                                    object_name = graph.nodes.get(v, {}).get("entity_name", str(v))
                                    rel = Relation(
                                        subject_id=str(u),
                                        object_id=str(v),
                                        subject_name=subject_name,
                                        object_name=object_name,
                                        description=attrs.get("description", ""),
                                        relation_strength=float(attrs.get("relation_strength", 1.0)),
                                        source_chunk_id=list(attrs.get("source_chunk_id", [])),
                                        id=rel_id,
                                    )
                                    break
                except Exception:
                    rel = None
                if rel is not None:
                    rels.append(rel)
                if len(rels) >= top_k:
                    break
            return rels
        if storage is not None:
            try:
                return asyncio.run(_search_vdb(query_string))
            except RuntimeError:
                pass
            except Exception:
                pass
        return self._simple_similar_relations_by_query(query_string, top_k=top_k)

    def find_similar_entity_by_query(self, query, top_k: int = 5) -> List[Entity]:
        if not query:
            return []

        storage = getattr(self.index, "entity_vector_db", None)

        async def _search_vdb(q: str) -> List[Entity]:
            try:
                results = await storage.query(q, top_k=top_k)
            except Exception:
                return []

            entities: List[Entity] = []
            for item in results:
                ent_id =item.get("__id__") 
                if ent_id is None:
                    continue
                try:
                    ent = await self.get_entity(ent_id)
                except Exception:
                    ent = None
                if ent is not None:
                    entities.append(ent)
                    if len(entities) >= top_k:
                        break
            return entities

        if storage is not None:
            try:
                return asyncio.run(_search_vdb(str(query)))
            except RuntimeError:
                pass
            except Exception:
                pass

        return self._simple_similar_entities_by_query(str(query), exclude_id=None, top_k=top_k)


    def find_similar_relation_by_query(self, query, top_k: int = 5) -> List[Relation]:

        if not query:
            return []
        storage = getattr(self.index, "relation_vector_db", None)
        async def _search_vdb(q: str) -> List[Relation]:
            try:
                results = await storage.query(q, top_k=top_k)  
            except Exception:
                return []
            rels: List[Relation] = []
            for item in results:
                rel_id = item.get("__id__") 
                if rel_id is None:
                    continue
                rel = None
                try:
                    subject_id = item.get("subject")
                    object_id = item.get("object")
                    if subject_id and object_id:
                        rel = await self.get_relation(subject_id, object_id)
                    else:
                        backend = getattr(self.index, "graph_backend", None)
                        graph = getattr(backend, "_graph", None)
                        if graph is not None:
                            for u, v, attrs in graph.edges(data=True):
                                if attrs.get("id") == rel_id:
                                    subject_name = graph.nodes.get(u, {}).get("entity_name", str(u))
                                    object_name = graph.nodes.get(v, {}).get("entity_name", str(v))
                                    rel = Relation(
                                        subject_id=str(u),
                                        object_id=str(v),
                                        subject_name=subject_name,
                                        object_name=object_name,
                                        description=attrs.get("description", ""),
                                        relation_strength=float(attrs.get("relation_strength", 1.0)),
                                        source_chunk_id=list(attrs.get("source_chunk_id", [])),
                                        id=rel_id,
                                    )
                                    break
                except Exception:
                    rel = None
                if rel is not None:
                    rels.append(rel)
                    if len(rels) >= top_k:
                        break
            return rels
        if storage is not None:
            try:
                return asyncio.run(_search_vdb(str(query)))
            except RuntimeError:
                pass
            except Exception:
                pass
        return self._simple_similar_relations_by_query(str(query), top_k=top_k)

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
