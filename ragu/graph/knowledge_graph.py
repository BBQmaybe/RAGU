from __future__ import annotations

from typing import List, Optional

from ragu.chunker.base_chunker import BaseChunker
from ragu.chunker.types import Chunk
from ragu.common.logger import logger
from ragu.common.global_parameters import Settings
from ragu.embedder.base_embedder import BaseEmbedder
from ragu.graph.builder_modules import RemoveIsolatedNodes
from ragu.graph.entity_aligner import EntityAligner
from ragu.graph.graph_builder_pipeline import (
    InMemoryGraphBuilder,
    BuilderArguments,
    GraphBuilderModule
)
from ragu.graph.types import Entity, Relation, CommunitySummary
from ragu.llm.base_llm import BaseLLM
from ragu.storage.index import Index, StorageArguments
from ragu.triplet.base_artifact_extractor import BaseArtifactExtractor
from ragu.storage.base_storage import EdgeSpec


class KnowledgeGraph:
    """
    High-level facade for building, storing, and querying a knowledge graph.

    :param client: LLM client used by extraction and summarization modules.
    :param embedder: Embedder used for vector storage and clustering/similarity steps.
    :param chunker: Optional chunker used to split input documents.
    :param artifact_extractor: Optional extractor used to generate entities/relations from chunks.
    :param builder_settings: Graph-building behavior configuration. Defaults are used if omitted.
    :param storage_settings: Storage backend configuration. Defaults are used if omitted.
    :param additional_modules: Optional post-processing modules for extracted graph items.
    :param language: Optional language override. Defaults to ``Settings.language``.
    """

    def __init__(
        self,
        client: Optional[BaseLLM],
        embedder: Optional[BaseEmbedder],
        chunker: Optional[BaseChunker] = None,
        artifact_extractor: Optional[BaseArtifactExtractor] = None,
        builder_settings: Optional[BuilderArguments] = None,
        storage_settings: Optional[StorageArguments] = None,
        additional_modules: Optional[List[GraphBuilderModule]] = None,
        language: Optional[str] = None,
    ):
        """
        Initialize KnowledgeGraph with pipeline and storage components.

        :param client: LLM client used by extraction and summarization modules.
        :param embedder: Embedder used by vector storage and optional clustering.
        :param chunker: Optional chunker used to split input documents.
        :param artifact_extractor: Optional entity/relation extractor.
        :param builder_settings: Optional graph builder settings.
        :param storage_settings: Optional storage backend settings.
        :param additional_modules: Optional post-processing modules for graph items.
        :param language: Optional language override. Defaults to ``Settings.language``.
        """
        self.client = client
        self.builder_settings = builder_settings or BuilderArguments()
        self.storage_settings = storage_settings or StorageArguments()
        self.language = language or Settings.language

        if not additional_modules:
            additional_modules = []

        if self.builder_settings.remove_isolated_nodes:
            additional_modules.append(RemoveIsolatedNodes())

        self.pipeline = InMemoryGraphBuilder(
            client=client,
            chunker=chunker,
            artifact_extractor=artifact_extractor,
            build_parameters=self.builder_settings,
            embedder=embedder,
            additional_pipeline=additional_modules,
            language=self.language,
        )

        self.index = Index(
            embedder=embedder,
            arguments=self.storage_settings,
        )

        self.make_community_summary = self.builder_settings.make_community_summary
        self.remove_isolated_nodes = self.builder_settings.remove_isolated_nodes
        self.vectorize_chunks = self.builder_settings.vectorize_chunks

    async def build_from_docs(self, docs: List[str]) -> "KnowledgeGraph":
        """
        Build graph and vector context from a list of input documents.

        :param docs: Input documents to process.
        :return: Self for method chaining.
        """
        chunks = self.pipeline.chunker.split(docs) if self.pipeline.chunker else \
            [Chunk(doc, i, doc_id=f"doc_{i}") for i, doc in enumerate(docs)]
        chunks = await self._deduplicate_chunks_by_id(chunks)

        if not chunks:
            logger.warning("Nothing to build.")
            return self

        entities, relations, summaries, communities, chunks = await self.pipeline.extract_graph(chunks)

        is_vector_only = self.builder_settings.build_only_vector_context
        should_store_communities = self.make_community_summary and not is_vector_only

        if should_store_communities and communities:
            entities, communities, summaries = await self.index.reindex_cluster_ids(
                entities,
                communities,
                summaries,
            )

        if not is_vector_only:
            await self.index.insert_entities(entities)
            await self.index.insert_relations(relations)

            if self.builder_settings.use_entity_alignment:
                await self.align_entities(
                    similarity_threshold=self.builder_settings.entity_alignment_threshold,
                )

        should_vectorize = self.vectorize_chunks or is_vector_only
        await self.index.upsert_chunks(chunks, vectorize=should_vectorize)

        if should_store_communities:
            await self.index.upsert_communities(communities)
            await self.index.upsert_summaries(summaries)

        return self

    async def insert_entities(self, entities: Entity | List[Entity]) -> "KnowledgeGraph":
        """
        Add one or more entities to the knowledge graph.

        Entities with duplicate (name, type) will be automatically merged.

        :param entities: Single entity or list of entities to add.
        :return: Self for method chaining.
        """
        if isinstance(entities, Entity):
            entities = [entities]

        await self.index.insert_entities(entities)
        return self

    async def update_entities(self, entities: Entity | List[Entity]) -> "KnowledgeGraph":
        """
        Replace one or more existing entities by ID.

        :param entities: Single entity or list of entities to replace.
        :return: Self for method chaining.
        """
        if isinstance(entities, Entity):
            entities = [entities]

        await self.index.update_entities(entities)
        return self

    async def add_entity(self, entities: Entity | List[Entity]) -> "KnowledgeGraph":
        """
        Backward-compatible alias for :meth:`insert_entities`.

        :param entities: Single entity or list of entities to add.
        :return: Self for method chaining.
        """
        await self.insert_entities(entities)
        return self

    async def get_entity(self, entity_id) -> Entity | None:
        """
        Retrieve one entity by ID.

        :param entity_id: Entity identifier.
        :return: Entity if found, otherwise ``None``.
        """
        entities = await self.index.graph_backend.get_nodes([entity_id])
        return entities[0] if entities else None

    async def delete_entity(self, entity_id: str, cascade: bool = True) -> "KnowledgeGraph":
        """
        Delete an entity from the knowledge graph.

        :param entity_id: ID of the entity to delete.
        :param cascade: Whether to also delete connected relations (default: True).
        :return: Self for method chaining.
        """
        await self.index.delete_entities([entity_id], cascade=cascade)
        return self

    async def update_entity(self, entity_id: str, new_entity: Entity) -> "KnowledgeGraph":
        """
        Replace an entity's data while keeping its ID and graph connections.

        :param entity_id: ID of the entity to update.
        :param new_entity: Entity with updated fields.
        :return: Self for method chaining.
        :raises ValueError: If the entity does not exist.
        """
        existing = await self.index.graph_backend.get_nodes([entity_id])
        if not existing or existing[0] is None:
            raise ValueError(f"Entity '{entity_id}' does not exist")

        new_entity.id = entity_id
        await self.update_entities([new_entity])
        return self

    async def insert_relations(self, relation: Relation | List[Relation]) -> "KnowledgeGraph":
        """
        Add one or more relations to the knowledge graph.

        Relations with duplicate IDs will be automatically merged.
        Validates that referenced entities exist.

        :param relation: Single relation or list of relations to add.
        :return: Self for method chaining.
        """
        if isinstance(relation, Relation):
            relation = [relation]

        await self.index.insert_relations(relation)
        return self

    async def update_relations(self, relation: Relation | List[Relation]) -> "KnowledgeGraph":
        """
        Replace one or more existing relations by ID.

        :param relation: Single relation or list of relations to replace.
        :return: Self for method chaining.
        """
        if isinstance(relation, Relation):
            relation = [relation]

        await self.index.update_relations(relation)
        return self

    async def delete_relation(
        self,
        subject_id: str,
        object_id: str,
        relation_id: str | None = None,
    ) -> "KnowledgeGraph":
        """
        Delete a relation from the knowledge graph.

        :param subject_id: Subject entity ID.
        :param object_id: Object entity ID.
        :param relation_id: Optional relation ID for precise delete.
        :return: Self for method chaining.
        """
        await self.index.delete_relations([(subject_id, object_id, relation_id)])
        return self

    async def edges_degrees(self, edge_specs: List[EdgeSpec]) -> List[int]:
        """
        Get degrees for multiple edges.

        Each returned value is ``degree(source) + degree(target)`` for the
        corresponding edge spec, or ``0`` when relation/endpoints are missing.

        :param edge_specs: Edge specifications ``(subject_id, object_id, relation_id)``.
        :return: Degree sums in the same order as input specs.
        """
        return await self.index.graph_backend.edges_degrees(edge_specs)

    async def add_summary(self, summary: CommunitySummary | List[CommunitySummary]) -> "KnowledgeGraph":
        """
        Add one or more community summaries.

        :param summary: Single summary or list of summaries to add.
        :return: Self for method chaining.
        """
        if isinstance(summary, CommunitySummary):
            summary = [summary]
        await self.index.upsert_summaries(summary)
        return self

    async def get_summary(self, summary_id: str) -> CommunitySummary | None:
        """
        Retrieve a community summary by ID.

        :param summary_id: ID of the summary to retrieve.
        :return: The summary, or ``None`` if not found.
        """
        result = await self.index.community_summary_kv_storage.get_by_id(summary_id)
        if result is None:
            return None
        return CommunitySummary(id=summary_id, summary=result)

    async def delete_summary(self, summary_id: str) -> "KnowledgeGraph":
        """
        Delete a community summary.

        :param summary_id: ID of the summary to delete.
        :return: Self for method chaining.
        """
        await self.index.community_summary_kv_storage.delete([summary_id])
        await self.index.community_summary_kv_storage.index_done_callback()
        return self

    async def update_summary(self, summary_id: str, new_summary: CommunitySummary) -> "KnowledgeGraph":
        """
        Replace a community summary's content.

        :param summary_id: ID of the summary to update.
        :param new_summary: Summary with updated content.
        :return: Self for method chaining.
        """
        new_summary.id = summary_id
        await self.index.upsert_summaries([new_summary])
        return self

    async def align_entities(
        self,
        similarity_threshold: float = 0.85,
    ) -> List[Entity]:
        """
        Run LLM-verified entity alignment on the current graph.

        Finds entity pairs with similar names (same type, string similarity
        above *similarity_threshold*), verifies each pair via LLM, and merges
        confirmed duplicates — redirecting edges and dropping self-loops.

        Can be called both during :meth:`build_from_docs` (when
        ``BuilderArguments.use_entity_alignment=True``) and independently
        at any time on an already-built graph.

        :param similarity_threshold: Minimum :func:`difflib.SequenceMatcher`
            ratio for a pair to be considered a candidate (default ``0.85``).
        :return: List of newly created merged entities (may be empty).
        :raises ValueError: If no LLM client is available.
        """
        if self.client is None:
            raise ValueError(
                "Entity alignment requires an LLM client, but none was provided."
            )

        aligner = EntityAligner(
            client=self.client,
            index=self.index,
            similarity_threshold=similarity_threshold,
        )
        return await aligner.run()

    async def find_similar_entities(self, entity: Entity, top_k: int = 10) -> List[Entity]:
        """
        Find entities semantically similar to the given entity.

        :param entity: Reference entity to search against.
        :param top_k: Maximum number of results.
        :return: Similar entities ordered by relevance.
        """
        query = f"{entity.entity_name} - {entity.description}"
        return await self.index.query_entities(query, top_k=top_k)

    async def find_similar_relations(self, relation: Relation, top_k: int = 10) -> List[Relation]:
        """
        Find relations semantically similar to the given relation.

        :param relation: Reference relation to search against.
        :param top_k: Maximum number of results.
        :return: Similar relations ordered by relevance.
        """
        return await self.index.query_relations(relation.description, top_k=top_k)

    async def find_similar_entity_by_query(self, query: str, top_k: int = 10) -> List[Entity]:
        """
        Find entities matching a free-text query.

        :param query: Search query text.
        :param top_k: Maximum number of results.
        :return: Matching entities ordered by relevance.
        """
        return await self.index.query_entities(query, top_k=top_k)

    async def find_similar_relation_by_query(self, query: str, top_k: int = 10) -> List[Relation]:
        """
        Find relations matching a free-text query.

        :param query: Search query text.
        :param top_k: Maximum number of results.
        :return: Matching relations ordered by relevance.
        """
        return await self.index.query_relations(query, top_k=top_k)

    async def _deduplicate_chunks_by_id(self, chunks: List[Chunk]) -> List[Chunk]:
        """
        Deduplicate chunks by ``chunk.id`` preserving original order.

        :param chunks: Chunks to deduplicate.
        :return: Deduplicated chunk list preserving original order.
        """
        if not chunks:
            return chunks

        already_in_index = await self.index.chunks_kv_storage.all_keys()

        unique_chunks: List[Chunk] = []
        seen_ids: set[str] = set(already_in_index)
        duplicate_count = 0

        for chunk in chunks:
            chunk_id = chunk.id
            if chunk_id in seen_ids:
                duplicate_count += 1
                continue
            seen_ids.add(chunk_id)
            unique_chunks.append(chunk)

        if duplicate_count > 0:
            logger.warning(
                f"Found {duplicate_count}/{len(chunks)} duplicated chunks by id. "
                f"Using {len(unique_chunks)} unique chunks."
            )

        return unique_chunks
