"""
Schema verification module implementing two-step knowledge extraction
with type-based verification for the RAGU knowledge graph pipeline.

Pipeline stages:
  1. Candidate extraction  — LLM extracts (subject, relation, object) triplets
                             from graph context using 3-shot prompting.
  2. FAISS retrieval       — top-K cosine-similar canonical candidates are
                             retrieved for each triplet component.
  3. Refinement            — LLM selects canonical forms from candidates.
  4. Type-based verification — triplets are filtered by local subject/object
                               type constraints per relation.

Usage as a pipeline module (disabled by default):
    module = SchemaVerificationModule(llm=llm, embedder=embedder, enabled=True)
    knowledge_graph = KnowledgeGraph(..., additional_pipeline=[module])

See also: scripts/process_gml.py for standalone GML processing.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set, Tuple, TypedDict

import numpy as np

from ragu.common.logger import logger
from ragu.graph.entity_aligner import AlternativeNameMerger
from ragu.graph.graph_builder_pipeline import GraphBuilderModule
from ragu.graph.types import Entity, Relation
from ragu.models.embedder import Embedder
from ragu.models.llm import LLM

try:
    import faiss as _faiss  # type: ignore
    _FAISS_AVAILABLE = True
except ImportError:
    _faiss = None  # type: ignore
    _FAISS_AVAILABLE = False
    logger.warning(
        "faiss-cpu is not installed; falling back to numpy cosine similarity. "
        "Install with: pip install faiss-cpu"
    )


# ---------------------------------------------------------------------------
# Type constraints for local ontology verification
# ---------------------------------------------------------------------------
# Maps relation_type -> (allowed_subject_types, allowed_object_types).
# An empty set means "any type is accepted" for that side.

_RELATION_TYPE_CONSTRAINTS: Dict[str, Tuple[Set[str], Set[str]]] = {
    "SPOUSE":           ({"PERSON"}, {"PERSON"}),
    "PARENT_OF":        ({"PERSON"}, {"PERSON"}),
    "SIBLING":          ({"PERSON"}, {"PERSON"}),
    "RELATIVE":         ({"PERSON"}, {"PERSON"}),
    "PLACE_OF_BIRTH":   ({"PERSON"}, {"CITY", "LOCATION", "COUNTRY", "STATE_OR_PROV", "DISTRICT"}),
    "PLACE_OF_DEATH":   ({"PERSON"}, {"CITY", "LOCATION", "COUNTRY", "STATE_OR_PROV", "DISTRICT"}),
    "PLACE_RESIDES_IN": ({"PERSON"}, {"CITY", "LOCATION", "COUNTRY", "STATE_OR_PROV", "DISTRICT"}),
    "WORKS_AS":         ({"PERSON"}, {"PROFESSION"}),
    "WORKPLACE":        ({"PERSON"}, {"ORGANIZATION"}),
    "SCHOOLS_ATTENDED": ({"PERSON"}, {"ORGANIZATION"}),
    "MEMBER_OF":        ({"PERSON"}, {"ORGANIZATION"}),
    "AWARDED_WITH":     ({"PERSON", "ORGANIZATION"}, {"AWARD"}),
    "CONVICTED_OF":     ({"PERSON"}, {"CRIME"}),
    "RELIGION_OF":      ({"PERSON"}, {"RELIGION"}),
    "FOUNDED_BY":       ({"ORGANIZATION"}, {"PERSON"}),
    "HEADQUARTERED_IN": ({"ORGANIZATION"}, {"CITY", "LOCATION", "COUNTRY", "STATE_OR_PROV"}),
    "SUBORDINATE_OF":   ({"ORGANIZATION"}, {"ORGANIZATION"}),
    "ORGANIZES":        ({"ORGANIZATION"}, {"EVENT"}),
    "IDEOLOGY_OF":      ({"ORGANIZATION"}, {"IDEOLOGY"}),
    "OWNER_OF":         ({"PERSON", "ORGANIZATION"}, set()),
    "PARTICIPANT_IN":   (set(), {"EVENT"}),
    "LOCATED_IN":       (set(), {"LOCATION", "CITY", "COUNTRY", "STATE_OR_PROV", "DISTRICT"}),
    "ORIGINS_FROM":     (set(), {"COUNTRY", "LOCATION"}),
}


# ---------------------------------------------------------------------------
# Prompts and few-shot examples
# ---------------------------------------------------------------------------

_EXTRACTION_SYSTEM_PROMPT = """\
Ты — эксперт по извлечению структурированных знаний из графов.

Тебе будет предоставлен фрагмент графа знаний в текстовом виде: список сущностей с их \
типами, описаниями и связями. Твоя задача — извлечь из этого контекста все смысловые \
отношения в виде триплетов (субъект, отношение, объект).

Строгие правила:
1. Выводи ТОЛЬКО JSON-массив триплетов. Никакого markdown, объяснений, вводных фраз.
2. Каждый триплет должен содержать ровно три поля: "subject", "relation", "object".
3. Используй имена сущностей и типы отношений точно так, как они указаны в контексте, \
без изменений, сокращений или перефразирования.
4. Не придумывай сущности или связи, которых нет в контексте.
5. Если в контексте нет ни одного отношения — выведи пустой массив [].
6. Не дублируй триплеты.

Формат вывода:
[{"subject": "...", "relation": "...", "object": "..."}, ...]"""

_REFINEMENT_SYSTEM_PROMPT = """\
Ты — эксперт по нормализации графов знаний.

Тебе будет предоставлен список триплетов (субъект, отношение, объект), извлечённых из графа, \
и для каждого компонента каждого триплета — список из нескольких канонических кандидатов из \
исходного графа знаний. Твоя задача — привести каждый триплет к каноническому виду, выбрав \
наилучшее значение из списка кандидатов.

Строгие правила:
1. Для каждого компонента триплета (subject, relation, object) выбирай ТОЛЬКО из \
предоставленного списка кандидатов. Использовать значения, не входящие в список, запрещено.
2. Если исходное извлечённое значение присутствует в списке кандидатов и является наиболее \
точным с учётом контекста — оставь его.
3. Предпочитай полные канонические формы сокращённым \
(«Антон Павлович Чехов» лучше, чем «А. Чехов», если оба в списке).
4. Не изменяй семантику триплета: subject и object должны соответствовать смыслу \
исходного отношения.
5. Выводи ТОЛЬКО JSON-массив триплетов. Никакого markdown, объяснений, вводных фраз.
6. Каждый триплет должен содержать ровно три поля: "subject", "relation", "object".
7. Если ни один кандидат не подходит — выбери наиболее близкий по смыслу из списка.

Формат вывода:
[{"subject": "...", "relation": "...", "object": "..."}, ...]"""

_FEW_SHOT_EXAMPLES = [
    {
        "context": (
            'Сущность: "Антон Чехов" (PERSON) — русский писатель и драматург.\n'
            '  - SPOUSE -> "Ольга Книппер" (PERSON): венчание в 1901 году\n'
            '  - PLACE_OF_BIRTH -> "Таганрог" (CITY): место рождения\n'
            '  - WORKS_AS -> "писатель" (PROFESSION): прозаик и драматург'
        ),
        "output": [
            {"subject": "Антон Чехов", "relation": "SPOUSE", "object": "Ольга Книппер"},
            {"subject": "Антон Чехов", "relation": "PLACE_OF_BIRTH", "object": "Таганрог"},
            {"subject": "Антон Чехов", "relation": "WORKS_AS", "object": "писатель"},
        ],
    },
    {
        "context": (
            'Сущность: "МГУ" (ORGANIZATION) — ведущий российский университет.\n'
            '  - FOUNDED_BY -> "Михаил Ломоносов" (PERSON): основатель в 1755 году\n'
            '  - HEADQUARTERED_IN -> "Москва" (CITY): главный кампус\n'
            'Сущность: "Михаил Ломоносов" (PERSON) — российский учёный-энциклопедист.\n'
            '  - PLACE_OF_BIRTH -> "Архангельская губерния" (LOCATION): место рождения'
        ),
        "output": [
            {"subject": "МГУ", "relation": "FOUNDED_BY", "object": "Михаил Ломоносов"},
            {"subject": "МГУ", "relation": "HEADQUARTERED_IN", "object": "Москва"},
            {"subject": "Михаил Ломоносов", "relation": "PLACE_OF_BIRTH", "object": "Архангельская губерния"},
        ],
    },
    {
        "context": (
            'Сущность: "Первая мировая война" (EVENT) — глобальный конфликт 1914–1918 годов.\n'
            '  - START_TIME -> "1914" (DATE): год начала\n'
            'Сущность: "Российская империя" (COUNTRY) — государство в Европе и Азии.\n'
            '  - PARTICIPANT_IN -> "Первая мировая война" (EVENT): одна из держав Антанты'
        ),
        "output": [
            {"subject": "Первая мировая война", "relation": "START_TIME", "object": "1914"},
            {"subject": "Российская империя", "relation": "PARTICIPANT_IN", "object": "Первая мировая война"},
        ],
    },
]


# ---------------------------------------------------------------------------
# TypedDicts
# ---------------------------------------------------------------------------

class RawTriplet(TypedDict):
    subject: str
    relation: str
    object: str


class _TripletCandidates(TypedDict):
    triplet: RawTriplet
    subject_candidates: List[str]
    relation_candidates: List[str]
    object_candidates: List[str]


# ---------------------------------------------------------------------------
# FAISS-backed cosine retriever (numpy fallback when faiss-cpu absent)
# ---------------------------------------------------------------------------

class _CandidateRetriever:
    """Nearest-neighbour retriever over a fixed corpus of embedded strings."""

    def __init__(self, corpus: List[str], embeddings: np.ndarray) -> None:
        self._corpus = corpus
        self._embeddings = embeddings.astype(np.float32)
        self._index: Any = None
        if corpus:
            self._build()

    def _build(self) -> None:
        _, dim = self._embeddings.shape
        if _FAISS_AVAILABLE:
            self._index = _faiss.IndexFlatIP(dim)
            vecs = self._embeddings.copy()
            _faiss.normalize_L2(vecs)
            self._index.add(vecs)

    def top_k(self, query_vec: np.ndarray, k: int = 5) -> List[str]:
        if not self._corpus:
            return []
        k = min(k, len(self._corpus))

        if _FAISS_AVAILABLE and self._index is not None:
            q = query_vec.astype(np.float32).reshape(1, -1)
            _faiss.normalize_L2(q)
            _, indices = self._index.search(q, k)
            return [self._corpus[i] for i in indices[0] if 0 <= i < len(self._corpus)]

        # numpy fallback
        norms = np.linalg.norm(self._embeddings, axis=1, keepdims=True)
        normed = self._embeddings / np.maximum(norms, 1e-9)
        qn = query_vec.astype(np.float32)
        qn /= max(float(np.linalg.norm(qn)), 1e-9)
        scores = normed @ qn
        top_indices = np.argsort(-scores)[:k]
        return [self._corpus[int(i)] for i in top_indices]


# ---------------------------------------------------------------------------
# Main module
# ---------------------------------------------------------------------------

class SchemaVerificationModule(GraphBuilderModule):
    """
    Graph-builder module implementing two-step extraction with type-based verification.

    By default this module is **disabled** (``enabled=False``) to ensure
    existing pipelines are unaffected. Set ``enabled=True`` to activate.

    Pipeline executed when enabled:
      Step 1  — LLM extracts candidate triplets from serialised graph context
                using 3-shot prompting.
      Between — FAISS cosine retrieval of top-``top_k`` canonical candidates for
                each triplet component (entity names / relation types).
      Step 2  — LLM refines each triplet by choosing from the candidate lists.
      Step 3  — Local type constraints filter out type-incompatible triplets.
      Step 4  — AlternativeNameMerger merges entities connected by
                ALTERNATIVE_NAME (or custom) relations (no API calls).

    :param llm: LLM instance used for extraction and refinement.
    :param embedder: Embedder instance used for building and querying FAISS indexes.
    :param top_k: Candidate count retrieved per triplet component (default 5).
    :param batch_size: Max relations processed per single LLM call (default 40).
    :param enabled: Activate the module (default False).
    :param merge_alternative_names: Run AlternativeNameMerger as the final step
        (default True). Set to False to skip entity merging.
    :param alternative_name_relation_types: Custom set of relation type strings
        treated as "same-as" signals by AlternativeNameMerger.
        Default: ``{"ALTERNATIVE_NAME"}``.
    """

    def __init__(
        self,
        llm: LLM,
        embedder: Embedder,
        top_k: int = 5,
        batch_size: int = 40,
        enabled: bool = False,
        merge_alternative_names: bool = True,
        alternative_name_relation_types: Set[str] | None = None,
    ) -> None:
        super().__init__()
        self.llm = llm
        self.embedder = embedder
        self.top_k = top_k
        self.batch_size = batch_size
        self.enabled = enabled
        self._alt_name_merger = (
            AlternativeNameMerger(relation_types=alternative_name_relation_types)
            if merge_alternative_names
            else None
        )

    async def run(
        self,
        entities: List[Entity],
        relations: List[Relation],
        **kwargs: Any,
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Execute the schema verification pipeline.

        When disabled, returns input unchanged. When enabled, runs Steps 1-3
        and returns only verified, canonically-named entities and relations.

        :param entities: Input entity list.
        :param relations: Input relation list.
        :return: Filtered and normalised (entities, relations).
        """
        if not self.enabled:
            return entities, relations

        if not entities or not relations:
            logger.info("SchemaVerificationModule: empty graph — skipping.")
            return entities, relations

        logger.info(
            f"SchemaVerificationModule: {len(entities)} entities, {len(relations)} relations."
        )

        # Step 1: candidate extraction via LLM
        triplets_raw = await self._extract_triplets(entities, relations)
        logger.info(f"Step 1 complete: {len(triplets_raw)} raw triplets.")

        if not triplets_raw:
            logger.warning("SchemaVerificationModule: Step 1 produced no triplets.")
            return entities, relations

        # Between steps: build FAISS indexes and retrieve top-K candidates
        entity_retriever, relation_retriever = await self._build_retrievers(entities, relations)
        triplets_with_candidates = await self._retrieve_candidates(
            triplets_raw, entity_retriever, relation_retriever
        )

        # Step 2: LLM refinement
        triplets_refined = await self._refine_triplets(triplets_with_candidates, entities)
        logger.info(f"Step 2 complete: {len(triplets_refined)} refined triplets.")

        # Step 3: type-based verification
        triplets_verified = self._verify_triplets(triplets_refined, entities)
        logger.info(f"Step 3 complete: {len(triplets_verified)} verified triplets.")

        result_entities, result_relations = self._rebuild_graph(
            triplets_verified, entities, relations
        )
        logger.info(
            f"SchemaVerificationModule: kept {len(result_entities)} entities, "
            f"{len(result_relations)} relations after verification."
        )

        # Step 4: merge entities connected by ALTERNATIVE_NAME relations
        if self._alt_name_merger is not None:
            result_entities, result_relations = await self._alt_name_merger.run(
                result_entities, result_relations
            )

        logger.info(
            f"SchemaVerificationModule done: {len(result_entities)} entities, "
            f"{len(result_relations)} relations."
        )
        return result_entities, result_relations

    # ------------------------------------------------------------------
    # Step 1: candidate extraction
    # ------------------------------------------------------------------

    def _graph_context(
        self, entities: List[Entity], relations: List[Relation]
    ) -> str:
        """Serialise entities and their outgoing relations as plain text."""
        entity_map = {e.id: e for e in entities}
        outgoing: Dict[str, List[Relation]] = {e.id: [] for e in entities}
        for rel in relations:
            if rel.subject_id in outgoing:
                outgoing[rel.subject_id].append(rel)

        lines: List[str] = []
        for entity in entities:
            etype = entity.entity_type or "UNKNOWN"
            desc = (entity.description or "").replace("\n", " ")[:200]
            lines.append(f'Entity: "{entity.entity_name}" ({etype}) — {desc}')
            for rel in outgoing.get(entity.id, []):
                obj = entity_map.get(rel.object_id)
                obj_name = rel.object_name or (obj.entity_name if obj else rel.object_id)
                obj_type = (obj.entity_type if obj else "UNKNOWN") or "UNKNOWN"
                rel_desc = (rel.description or "").replace("\n", " ")[:100]
                lines.append(f'  - {rel.relation_type} -> "{obj_name}" ({obj_type}): {rel_desc}')
        return "\n".join(lines)

    def _extraction_user_message(self, context: str) -> str:
        """Build few-shot extraction prompt."""
        parts: List[str] = []
        for i, ex in enumerate(_FEW_SHOT_EXAMPLES, 1):
            parts.append(
                f"Example {i}:\nContext:\n{ex['context']}\n"
                f"Output:\n{json.dumps(ex['output'], ensure_ascii=False)}"
            )
        parts.append(
            f"Now extract triplets from:\nContext:\n{context}\nOutput:"
        )
        return "\n\n".join(parts)

    @staticmethod
    def _parse_triplet_json(text: str) -> List[RawTriplet]:
        """Extract and parse a JSON array of triplets from an LLM response."""
        start = text.find("[")
        end = text.rfind("]") + 1
        if start == -1 or end == 0:
            return []
        try:
            items = json.loads(text[start:end])
        except json.JSONDecodeError:
            return []
        result: List[RawTriplet] = []
        for item in items:
            if all(k in item for k in ("subject", "relation", "object")):
                result.append(
                    RawTriplet(
                        subject=str(item["subject"]),
                        relation=str(item["relation"]),
                        object=str(item["object"]),
                    )
                )
        return result

    async def _extract_triplets(
        self, entities: List[Entity], relations: List[Relation]
    ) -> List[RawTriplet]:
        """Step 1: call LLM to extract candidate triplets in batches."""
        all_triplets: List[RawTriplet] = []

        for start in range(0, max(len(relations), 1), self.batch_size):
            batch_rels = relations[start: start + self.batch_size]
            batch_ids = {r.subject_id for r in batch_rels} | {r.object_id for r in batch_rels}
            batch_ents = [e for e in entities if e.id in batch_ids]

            context = self._graph_context(batch_ents, batch_rels)
            conversation = [
                {"role": "system", "content": _EXTRACTION_SYSTEM_PROMPT},
                {"role": "user", "content": self._extraction_user_message(context)},
            ]
            try:
                response: str = await self.llm.chat_completion(conversation)
                all_triplets.extend(self._parse_triplet_json(response))
            except Exception as exc:
                logger.warning(f"Step 1 batch {start // self.batch_size} failed: {exc}")

        return all_triplets

    # ------------------------------------------------------------------
    # Between steps: FAISS candidate retrieval
    # ------------------------------------------------------------------

    async def _build_retrievers(
        self, entities: List[Entity], relations: List[Relation]
    ) -> Tuple[_CandidateRetriever, _CandidateRetriever]:
        """Embed entity names and relation types; build FAISS indexes."""
        entity_names = sorted({e.entity_name for e in entities if e.entity_name})
        relation_types = sorted({r.relation_type for r in relations if r.relation_type})

        if entity_names:
            entity_vecs = np.array(
                await self.embedder.batch_embed_text(entity_names, desc="Embedding entity names"),
                dtype=np.float32,
            )
        else:
            entity_vecs = np.zeros((0, self.embedder.dim), dtype=np.float32)

        if relation_types:
            relation_vecs = np.array(
                await self.embedder.batch_embed_text(relation_types, desc="Embedding relation types"),
                dtype=np.float32,
            )
        else:
            relation_vecs = np.zeros((0, self.embedder.dim), dtype=np.float32)

        return (
            _CandidateRetriever(entity_names, entity_vecs),
            _CandidateRetriever(relation_types, relation_vecs),
        )

    async def _retrieve_candidates(
        self,
        triplets: List[RawTriplet],
        entity_retriever: _CandidateRetriever,
        relation_retriever: _CandidateRetriever,
    ) -> List[_TripletCandidates]:
        """Retrieve top-K candidates for each component of each triplet."""
        unique_ent_vals = sorted(
            {t["subject"] for t in triplets} | {t["object"] for t in triplets}
        )
        unique_rel_vals = sorted({t["relation"] for t in triplets})

        ent_vecs_raw = (
            await self.embedder.batch_embed_text(unique_ent_vals, desc="Embedding entity queries")
            if unique_ent_vals
            else []
        )
        rel_vecs_raw = (
            await self.embedder.batch_embed_text(unique_rel_vals, desc="Embedding relation queries")
            if unique_rel_vals
            else []
        )

        ent_map: Dict[str, np.ndarray] = {
            val: np.array(vec, dtype=np.float32)
            for val, vec in zip(unique_ent_vals, ent_vecs_raw)
        }
        rel_map: Dict[str, np.ndarray] = {
            val: np.array(vec, dtype=np.float32)
            for val, vec in zip(unique_rel_vals, rel_vecs_raw)
        }

        result: List[_TripletCandidates] = []
        for triplet in triplets:
            subj_vec = ent_map.get(triplet["subject"])
            rel_vec = rel_map.get(triplet["relation"])
            obj_vec = ent_map.get(triplet["object"])
            result.append(
                _TripletCandidates(
                    triplet=triplet,
                    subject_candidates=(
                        entity_retriever.top_k(subj_vec, self.top_k) if subj_vec is not None else []
                    ),
                    relation_candidates=(
                        relation_retriever.top_k(rel_vec, self.top_k) if rel_vec is not None else []
                    ),
                    object_candidates=(
                        entity_retriever.top_k(obj_vec, self.top_k) if obj_vec is not None else []
                    ),
                )
            )
        return result

    # ------------------------------------------------------------------
    # Step 2: LLM refinement
    # ------------------------------------------------------------------

    def _refinement_user_message(
        self,
        items: List[_TripletCandidates],
        entities: List[Entity],
    ) -> str:
        """Build the structured refinement prompt."""
        ctx_lines = [
            f'"{e.entity_name}" ({e.entity_type}): {(e.description or "")[:100]}'
            for e in entities[:5]
        ]
        text_ctx = "\n".join(ctx_lines) if ctx_lines else "(graph entities)"

        lines = [
            f"Text: {text_ctx}",
            "",
            "Triplets and corresponding entity and relation mappings:",
        ]
        for item in items:
            t = item["triplet"]
            lines.append(
                f'\nTriplet: ({t["subject"]}, {t["relation"]}, {t["object"]})'
            )
            lines.append(f'  {{{repr(t["subject"])}: {item["subject_candidates"]}}}')
            lines.append(f'  {{{repr(t["relation"])}: {item["relation_candidates"]}}}')
            lines.append(f'  {{{repr(t["object"])}: {item["object_candidates"]}}}')

        lines.append("\nSelect canonical forms from the candidate lists. Output JSON array:")
        return "\n".join(lines)

    async def _refine_triplets(
        self,
        items: List[_TripletCandidates],
        entities: List[Entity],
    ) -> List[RawTriplet]:
        """Step 2: LLM selects canonical names from candidates."""
        all_refined: List[RawTriplet] = []

        for start in range(0, len(items), self.batch_size):
            batch = items[start: start + self.batch_size]
            conversation = [
                {"role": "system", "content": _REFINEMENT_SYSTEM_PROMPT},
                {"role": "user", "content": self._refinement_user_message(batch, entities)},
            ]
            try:
                response: str = await self.llm.chat_completion(conversation)
                parsed = self._parse_triplet_json(response)
                if parsed:
                    all_refined.extend(parsed)
                else:
                    logger.warning(
                        f"Step 2 batch {start // self.batch_size}: no JSON returned; "
                        "keeping raw triplets."
                    )
                    all_refined.extend(item["triplet"] for item in batch)
            except Exception as exc:
                logger.warning(
                    f"Step 2 batch {start // self.batch_size} failed: {exc}; keeping raw."
                )
                all_refined.extend(item["triplet"] for item in batch)

        return all_refined

    # ------------------------------------------------------------------
    # Step 3: type-based verification
    # ------------------------------------------------------------------

    def _verify_triplets(
        self,
        triplets: List[RawTriplet],
        entities: List[Entity],
    ) -> List[RawTriplet]:
        """
        Step 3: filter triplets using local subject/object type constraints.

        A triplet passes if:
        - Its relation has no entry in ``_RELATION_TYPE_CONSTRAINTS`` (unknown → keep).
        - Subject entity type is in the allowed subject types (or constraint is empty).
        - Object entity type is in the allowed object types (or constraint is empty).
        """
        entity_type_by_name: Dict[str, str] = {
            e.entity_name.lower(): e.entity_type for e in entities
        }

        verified: List[RawTriplet] = []
        for triplet in triplets:
            constraints = _RELATION_TYPE_CONSTRAINTS.get(triplet["relation"])
            if constraints is None:
                verified.append(triplet)
                continue

            allowed_subj, allowed_obj = constraints
            subj_type = entity_type_by_name.get(triplet["subject"].lower(), "")
            obj_type = entity_type_by_name.get(triplet["object"].lower(), "")

            subj_ok = not allowed_subj or subj_type in allowed_subj
            obj_ok = not allowed_obj or obj_type in allowed_obj

            if subj_ok and obj_ok:
                verified.append(triplet)
            else:
                logger.debug(
                    f"Filtered ({triplet['subject']}, {triplet['relation']}, "
                    f"{triplet['object']}): "
                    f"subj_type={subj_type!r} not in {allowed_subj}, "
                    f"obj_type={obj_type!r} not in {allowed_obj}"
                )

        return verified

    # ------------------------------------------------------------------
    # Graph reconstruction
    # ------------------------------------------------------------------

    def _rebuild_graph(
        self,
        verified: List[RawTriplet],
        original_entities: List[Entity],
        original_relations: List[Relation],
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Map verified triplets back to original Entity/Relation objects.

        Matching is done by lower-cased (subject_name, relation_type, object_name).
        Triplets that cannot be matched to any original relation are skipped.
        """
        relation_lookup: Dict[Tuple[str, str, str], Relation] = {}
        for rel in original_relations:
            key = (
                rel.subject_name.lower(),
                rel.relation_type,
                rel.object_name.lower(),
            )
            relation_lookup.setdefault(key, rel)

        retained_relations: List[Relation] = []
        retained_entity_ids: set[str] = set()

        for triplet in verified:
            key = (
                triplet["subject"].lower(),
                triplet["relation"],
                triplet["object"].lower(),
            )
            rel = relation_lookup.get(key)
            if rel is None:
                logger.debug(f"No original relation matched: {triplet}")
                continue
            retained_relations.append(rel)
            retained_entity_ids.add(rel.subject_id)
            retained_entity_ids.add(rel.object_id)

        retained_entities = [e for e in original_entities if e.id in retained_entity_ids]
        return retained_entities, retained_relations
