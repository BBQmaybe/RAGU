import asyncio
from typing import List

from ragu.graph.graph_builder_pipeline import InMemoryGraphBuilder
from ragu.graph.types import Entity, Relation, CommunitySummary
from ragu.storage.index import Index

from ragu.common.global_parameters import Settings
from ragu.common.logger import logger


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
                await storage.upsert(payload)                    
                await storage.index_done_callback()              
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
                    await storage.index_done_callback()           
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
                await storage.upsert({summary_id: summary_text})  
                await storage.index_done_callback()              
            except Exception:
                pass
        return self

    async def _simple_similar_entities_by_query(
        self,
        query: str,
        exclude_id: str | None = None,
        top_k: int = 5,
    ) -> List[Entity]:
        if not query:
            return []

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
                    candidates.append(
                        Entity(
                            id=str(node_id),
                            entity_name=attrs.get("entity_name", str(node_id)),
                            entity_type=attrs.get("entity_type", "Unknown"),
                            description=attrs.get("description", ""),
                            source_chunk_id=list(attrs.get("source_chunk_id", [])),
                            documents_id=list(attrs.get("documents_id", [])),
                            clusters=list(attrs.get("clusters", [])),
                        )
                    )

        if not candidates:
            return []

        from numpy import array, linalg

        candidate_texts = [f"{c.entity_name} - {c.description}" for c in candidates]
        texts = [query] + candidate_texts
        embeddings = await self.index.embedder.embed(texts)

        emb_array = array(embeddings)
        query_vec = emb_array[0]
        candidate_vecs = emb_array[1:]

        q_norm = linalg.norm(query_vec) + 1e-8
        cand_norms = linalg.norm(candidate_vecs, axis=1) + 1e-8
        scores = (candidate_vecs @ query_vec) / (cand_norms * q_norm)

        ranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
        return [e for _, e in ranked[:top_k]]


    async def _simple_similar_relations_by_query(
        self,
        query: str,
        top_k: int = 5,
    ) -> List[Relation]:
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
                    candidates.append(
                        Relation(
                            subject_id=str(u),
                            object_id=str(v),
                            subject_name=graph.nodes.get(u, {}).get("entity_name", str(u)),
                            object_name=graph.nodes.get(v, {}).get("entity_name", str(v)),
                            description=attrs.get("description", ""),
                            relation_strength=float(attrs.get("relation_strength", 1.0)),
                            source_chunk_id=list(attrs.get("source_chunk_id", [])),
                            id=attrs.get("id"),
                        )
                    )

        if not candidates:
            return []

        from numpy import array, linalg

        candidate_texts = [rel.description or "" for rel in candidates]
        texts = [query] + candidate_texts
        embeddings = await self.index.embedder.embed(texts)

        emb_array = array(embeddings)
        query_vec = emb_array[0]
        candidate_vecs = emb_array[1:]

        q_norm = linalg.norm(query_vec) + 1e-8
        cand_norms = linalg.norm(candidate_vecs, axis=1) + 1e-8
        scores = (candidate_vecs @ query_vec) / (cand_norms * q_norm)

        ranked = sorted(zip(scores, candidates), key=lambda x: x[0], reverse=True)
        return [r for _, r in ranked[:top_k]]


    async def find_similar_entities(
        self,
        entity: Entity | None,
        top_k: int = 5,
    ) -> List[Entity]:
        if entity is None:
            return []

        query_string = f"{entity.entity_name} - {entity.description}".strip()
        storage = getattr(self.index, "entity_vector_db", None)

        if storage is not None:
            results = await storage.query(query_string, top_k=top_k + 1)
            entities: List[Entity] = []
            for item in results:
                ent_id = item.get("__id__")
                if ent_id is None or ent_id == entity.id:
                    continue
                ent = await self.get_entity(ent_id)
                if ent is not None:
                    entities.append(ent)
                    if len(entities) >= top_k:
                        break
            if entities:
                return entities

        return await self._simple_similar_entities_by_query(
            query_string,
            exclude_id=entity.id,
            top_k=top_k,
        )


    async def find_similar_relations(
        self,
        relation: Relation | None,
        top_k: int = 5,
    ) -> List[Relation]:
        if relation is None:
            return []

        query_string = relation.description or ""
        storage = getattr(self.index, "relation_vector_db", None)

        if storage is not None:
            results = await storage.query(query_string, top_k=top_k + 1)
            rels: List[Relation] = []
            for item in results:
                rel_id = item.get("__id__")
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

        return await self._simple_similar_relations_by_query(query_string, top_k=top_k)


    async def find_similar_entity_by_query(
        self,
        query: str | None,
        top_k: int = 5,
    ) -> List[Entity]:
        if not query:
            return []

        storage = getattr(self.index, "entity_vector_db", None)

        if storage is not None:
            results = await storage.query(query, top_k=top_k)
            entities: List[Entity] = []
            for item in results:
                ent_id = item.get("__id__")
                if ent_id is None:
                    continue
                ent = await self.get_entity(ent_id)
                if ent is not None:
                    entities.append(ent)
                    if len(entities) >= top_k:
                        break
            if entities:
                return entities

        return await self._simple_similar_entities_by_query(
            query,
            exclude_id=None,
            top_k=top_k,
        )


    async def find_similar_relation_by_query(
        self,
        query: str | None,
        top_k: int = 5,
    ) -> List[Relation]:
        if not query:
            return []

        storage = getattr(self.index, "relation_vector_db", None)

        if storage is not None:
            results = await storage.query(query, top_k=top_k)
            rels: List[Relation] = []
            for item in results:
                rel_id = item.get("__id__")
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

        return await self._simple_similar_relations_by_query(query, top_k=top_k)


    async def extract_and_refine_triplets(
        self,
        text: str,
        top_k_similar: int = 5,
    ):
        if not text or not text.strip():
            return []

        client = getattr(self.pipeline, "client", None) or getattr(self.pipeline, "llm", None)
        if client is None:
            return []

        from pydantic import BaseModel
        from typing import List as _List
        import json

        class _LLMTriplet(BaseModel):
            subject: str
            relation: str
            object: str

        class _LLMTripletList(BaseModel):
            triplets: _List[_LLMTriplet]

        system_prompt_step1 = (
            "You are an information extraction assistant. "
            "Read the given text and extract all factual knowledge as subject–relation–object "
            "triples. Each triple must be explicitly supported by the text. "
            "Do not hallucinate entities or relations that are not mentioned."
        )

        prompt_step1 = (
            "Task: extract all subject–relation–object triples from the text.\n\n"
            "Return JSON with the following structure:\n"
            '{ "triplets": [ { "subject": \"...\", \"relation\": \"...\", \"object\": \"...\" }, ... ] }\n\n'
            "Example 1:\n"
            'Text: \"Marie Curie discovered radium.\"\n'
            'Output: { \"triplets\": [ { \"subject\": \"Marie Curie\", \"relation\": \"discovered\", \"object\": \"radium\" } ] }\n\n'
            "Example 2:\n"
            'Text: \"Albert Einstein was born in Ulm and worked at the Swiss Patent Office.\"\n'
            'Output: { \"triplets\": [\n'
            '  { \"subject\": \"Albert Einstein\", \"relation\": \"was born in\", \"object\": \"Ulm\" },\n'
            '  { \"subject\": \"Albert Einstein\", \"relation\": \"worked at\", \"object\": \"Swiss Patent Office\" }\n'
            "] }\n\n"
            f"Now process the following text:\n{text}\n\n"
            "Return ONLY valid JSON, no comments, no extra text."
        )

        step1_results = await client.generate(
            prompt_step1,
            system_prompt=system_prompt_step1,
            schema=_LLMTripletList,
            progress_bar_desc="Triplet extraction",
        )

        raw_triplets: list[dict] = []
        if step1_results and isinstance(step1_results[0], _LLMTripletList):
            parsed: _LLMTripletList = step1_results[0]
            for t in parsed.triplets:
                raw_triplets.append(
                    {
                        "subject": t.subject.strip(),
                        "relation": t.relation.strip(),
                        "object": t.object.strip(),
                    }
                )
        elif step1_results and isinstance(step1_results[0], str):
            data = json.loads(step1_results[0])
            for t in data.get("triplets", []):
                raw_triplets.append(
                    {
                        "subject": str(t.get("subject", "")).strip(),
                        "relation": str(t.get("relation", "")).strip(),
                        "object": str(t.get("object", "")).strip(),
                    }
                )

        if not raw_triplets:
            return []

        triplets_with_candidates: list[dict] = []
        entity_cache: dict[str, List[Entity]] = {}
        relation_cache: dict[str, List[Relation]] = {}

        for t in raw_triplets:
            subj_name = t["subject"]
            rel_name = t["relation"]
            obj_name = t["object"]

            if subj_name not in entity_cache:
                entity_cache[subj_name] = await self.find_similar_entity_by_query(
                    subj_name,
                    top_k=top_k_similar,
                )
            if rel_name not in relation_cache:
                relation_cache[rel_name] = await self.find_similar_relation_by_query(
                    rel_name,
                    top_k=top_k_similar,
                )
            if obj_name not in entity_cache:
                entity_cache[obj_name] = await self.find_similar_entity_by_query(
                    obj_name,
                    top_k=top_k_similar,
                )

            subj_cands = entity_cache.get(subj_name, [])
            rel_cands = relation_cache.get(rel_name, [])
            obj_cands = entity_cache.get(obj_name, [])

            triplets_with_candidates.append(
                {
                    "raw_subject": subj_name,
                    "raw_relation": rel_name,
                    "raw_object": obj_name,
                    "subject_candidates": [e.entity_name for e in subj_cands] or [subj_name],
                    "relation_candidates": [r.description for r in rel_cands if r.description] or [rel_name],
                    "object_candidates": [e.entity_name for e in obj_cands] or [obj_name],
                }
            )

        system_prompt_step2 = (
            "You are an expert in knowledge graph canonicalisation. "
            "You are given raw triples extracted from a text and, for each subject, relation, "
            "and object, several candidate canonical names coming from a knowledge graph. "
            "For each triple, choose the combination of subject–relation–object names that "
            "fits both the original text and the local context of the triple."
        )

        prompt2_payload = {
            "text": text,
            "triplets": triplets_with_candidates,
            "instruction": (
                "For each entry in 'triplets', choose one subject from 'subject_candidates', "
                "one relation from 'relation_candidates', and one object from 'object_candidates'. "
                "Output JSON of the form:\n"
                '{ "triplets": [ { "subject": "...", "relation": "...", "object": "..." }, ... ] }\n'
                "The order of output triplets must correspond to the input order."
            ),
        }

        prompt_step2 = (
            "You will receive a JSON object with the original text and raw extracted triples, "
            "along with candidate canonical names from a knowledge graph.\n\n"
            "JSON input:\n"
            f"{json.dumps(prompt2_payload, ensure_ascii=False, indent=2)}\n\n"
            "Now produce ONLY JSON as described in 'instruction'. No explanations."
        )

        step2_results = await client.generate(
            prompt_step2,
            system_prompt=system_prompt_step2,
            schema=_LLMTripletList,
            progress_bar_desc="Triplet refinement",
        )

        refined_triplets: list[dict] = []
        if step2_results and isinstance(step2_results[0], _LLMTripletList):
            parsed2: _LLMTripletList = step2_results[0]
            for t in parsed2.triplets:
                refined_triplets.append(
                    {
                        "subject": t.subject.strip(),
                        "relation": t.relation.strip(),
                        "object": t.object.strip(),
                    }
                )
        elif step2_results and isinstance(step2_results[0], str):
            data2 = json.loads(step2_results[0])
            for t in data2.get("triplets", []):
                refined_triplets.append(
                    {
                        "subject": str(t.get("subject", "")).strip(),
                        "relation": str(t.get("relation", "")).strip(),
                        "object": str(t.get("object", "")).strip(),
                    }
                )

        if not refined_triplets:
            refined_triplets = raw_triplets

        relation_type_constraints: dict[str, set[tuple[str, str]]] = {}

        all_relations: list[Relation] = []
        if self._id_to_relation_map:
            all_relations = list(self._id_to_relation_map.values())
        else:
            backend = getattr(self.index, "graph_backend", None)
            graph = getattr(backend, "_graph", None)
            if graph is not None:
                for u, v, attrs in graph.edges(data=True):
                    rel_obj = Relation(
                        subject_id=str(u),
                        object_id=str(v),
                        subject_name=graph.nodes.get(u, {}).get("entity_name", str(u)),
                        object_name=graph.nodes.get(v, {}).get("entity_name", str(v)),
                        description=attrs.get("description", ""),
                        relation_strength=float(attrs.get("relation_strength", 1.0)),
                        source_chunk_id=list(attrs.get("source_chunk_id", [])),
                        id=attrs.get("id"),
                    )
                    all_relations.append(rel_obj)

        for rel in all_relations:
            desc = rel.description or ""
            subj_ent = await self.get_entity(rel.subject_id)
            obj_ent = await self.get_entity(rel.object_id)
            if not subj_ent or not obj_ent:
                continue
            pair = (subj_ent.entity_type, obj_ent.entity_type)
            relation_type_constraints.setdefault(desc, set()).add(pair)

        final_output: list[dict] = []

        for t in refined_triplets:
            subj_label = t["subject"]
            rel_label = t["relation"]
            obj_label = t["object"]

            subj_best = entity_cache.get(subj_label)
            if subj_best is None:
                subj_best = await self.find_similar_entity_by_query(subj_label, top_k=1)
                entity_cache[subj_label] = subj_best

            obj_best = entity_cache.get(obj_label)
            if obj_best is None:
                obj_best = await self.find_similar_entity_by_query(obj_label, top_k=1)
                entity_cache[obj_label] = obj_best

            subj_type = subj_best[0].entity_type if subj_best else ""
            obj_type = obj_best[0].entity_type if obj_best else ""

            allowed_pairs = relation_type_constraints.get(rel_label, set())
            validity = (subj_type, obj_type) in allowed_pairs

            reason = None
            if not validity:
                if not allowed_pairs:
                    reason = f"No type constraints observed for relation '{rel_label}'."
                else:
                    reason = f"Observed allowed types {allowed_pairs}, but got ({subj_type}, {obj_type})."

            final_output.append(
                {
                    "subject": subj_label,
                    "relation": rel_label,
                    "object": obj_label,
                    "validity": validity,
                    **({"reason": reason} if reason else {}),
                }
            )

        return final_output

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
    

    