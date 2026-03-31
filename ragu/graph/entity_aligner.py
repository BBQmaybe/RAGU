"""
Entity alignment module for deduplicating near-duplicate entities in the RAGU
knowledge graph pipeline.

Pipeline stages:
  1. Candidate detection — pairwise difflib.SequenceMatcher comparison (0 API calls).
                           Only same-type entity pairs above ``threshold`` are kept.
  2. LLM verification    — all candidate pairs verified in parallel via asyncio.gather.
                           Each call uses structured output (EntityAlignmentModel).
  3. Atomic merge        — edges collected, old entities removed, merged entity inserted,
                           edges redirected; self-loops discarded.

Usage as a pipeline module (disabled by default):
    aligner = EntityAligner(llm=llm, threshold=0.85, enabled=True)
    knowledge_graph = KnowledgeGraph(..., additional_modules=[aligner])

AlternativeNameMerger — zero-API module that merges entities connected by
ALTERNATIVE_NAME (or custom) relations:
    merger = AlternativeNameMerger()
    knowledge_graph = KnowledgeGraph(..., additional_modules=[merger])
"""

from __future__ import annotations

import asyncio
import difflib
from typing import Any, Dict, List, Set, Tuple

from pydantic import BaseModel

from ragu.common.logger import logger
from ragu.graph.graph_builder_pipeline import GraphBuilderModule
from ragu.graph.types import Entity, Relation
from ragu.models.llm import LLM


_SYSTEM_PROMPT = """\
Ты — эксперт по дедупликации сущностей в графах знаний.

Тебе будут предоставлены две записи об объектах из графа знаний: «Сущность А» и «Сущность Б».
Твоя задача — определить, описывают ли обе записи один и тот же реальный объект.

Критерии для принятия решения:

1. Имена. Внимательно сравни имена с учётом русскоязычных соглашений:
   - Разрешены: сокращения («А. Чехов» = «Антон Чехов»), инициалы, опечатки, \
разные падежи/склонения одного имени.
   - Частный случай для PERSON: имя + отчество без фамилии является стандартным \
сокращением полного имени (имя + отчество + фамилия). Например, «Яков Васильевич» — \
это сокращение «Яков Васильевич Булдеев». Если имя и отчество совпадают, а в одной \
из записей просто отсутствует фамилия — это СИЛЬНЫЙ сигнал к объединению.
   - Запрещено объединять, если имена явно разные (разные имена или отчества).

2. Тип. Типы сущностей должны совпадать или быть совместимыми (например, PERSON и PERSON). \
Несовместимые типы — запрет на объединение.

3. Описание. Проверь ключевые факты: роли, даты, места, действия. \
Описания могут частично пересекаться (одна запись — краткая, другая — развёрнутая): \
это нормально при объединении. Прямое противоречие фактов — запрет на объединение.

4. Приоритет формальных признаков. Если имя и отчество совпадают (для PERSON) \
и описания не противоречат — объединяй, даже если описания отличаются степенью детализации. \
Сомнение само по себе не является поводом для отказа, если структурные признаки указывают \
на одно и то же лицо.

Правила формирования результата при объединении:
- merged_entity_name: каноническое полное имя (предпочтительнее полная форма, включающая фамилию).
- merged_entity_type: тип из входных данных (если совпадают — оставить без изменений).
- merged_description: краткое объединённое описание, включающее ключевые факты обеих записей. \
Не более 300 символов.

Если объединение не требуется:
- should_merge=false
- заполни merged_* поля значениями Сущности А (как заглушку, они не будут применены).

Примеры правильных решений:
- «Яков Васильевич» (PERSON) + «Яков Васильевич Булдеев» (PERSON) → should_merge=true \
  (имя и отчество совпадают, вторая запись — полная форма первой)
- «Булдеева» (PERSON) + «Булдеев» (PERSON) → should_merge=false \
  (разные имена — жена и муж, разные люди)
- «А. П. Чехов» (PERSON) + «Антон Павлович Чехов» (PERSON) → should_merge=true \
  (инициалы совпадают с полным именем)"""


class EntityAlignmentModel(BaseModel):
    """Structured LLM output for a single entity-pair alignment decision."""

    should_merge: bool
    merged_entity_name: str
    merged_entity_type: str
    merged_description: str


class EntityAligner(GraphBuilderModule):
    """
    Graph-builder module that deduplicates near-duplicate entities.

    By default this module is **disabled** (``enabled=False``) to ensure
    existing pipelines are unaffected. Set ``enabled=True`` to activate.

    Pipeline executed when enabled:
      Step 1 — Candidate detection: pairwise SequenceMatcher on entity names.
               Only same-type pairs with ratio >= ``threshold`` are candidates.
      Step 2 — LLM verification: all candidate pairs sent in parallel via
               ``asyncio.gather``; each call returns ``EntityAlignmentModel``.
      Step 3 — Atomic merge per confirmed pair:
               edges of both entities collected → old entities removed →
               merged entity inserted → edges redirected → self-loops dropped.
               Pairs where one entity was already consumed are skipped.

    :param llm: LLM instance used for pair verification (structured output).
    :param threshold: Minimum SequenceMatcher ratio to consider as candidate
                      (default 0.85).
    :param enabled: Activate the module (default False).
    """

    def __init__(
        self,
        llm: LLM,
        threshold: float = 0.85,
        enabled: bool = False,
    ) -> None:
        super().__init__()
        self.llm = llm
        self.threshold = threshold
        self.enabled = enabled

    async def run(
        self,
        entities: List[Entity],
        relations: List[Relation],
        **_kwargs: Any,
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Execute the entity alignment pipeline.

        When disabled, returns input unchanged. When enabled, deduplicates
        near-duplicate same-type entities and returns the merged graph.

        :param entities: Input entity list.
        :param relations: Input relation list.
        :return: Deduplicated (entities, relations).
        """
        if not self.enabled:
            return entities, relations

        if len(entities) < 2:
            return entities, relations

        logger.info(
            f"EntityAligner: {len(entities)} entities, {len(relations)} relations."
        )

        candidate_pairs = self._find_candidates(entities)
        if not candidate_pairs:
            logger.info("EntityAligner: no candidate pairs found.")
            return entities, relations

        logger.info(f"EntityAligner: {len(candidate_pairs)} candidate pairs to verify.")

        results: List[EntityAlignmentModel] = list(
            await asyncio.gather(*[
                self._verify_pair(e1, e2) for e1, e2 in candidate_pairs
            ])
        )

        current_entities = list(entities)
        current_relations = list(relations)
        merged_ids: Set[str] = set()
        n_merged = 0

        for (e1, e2), result in zip(candidate_pairs, results):
            if not result.should_merge:
                continue
            if e1.id in merged_ids or e2.id in merged_ids:
                logger.debug(
                    f"Skipping merge of {e1.entity_name!r} + {e2.entity_name!r}: "
                    "one entity was already consumed by a prior merge."
                )
                continue
            current_entities, current_relations = self._apply_merge(
                e1, e2, result, current_entities, current_relations
            )
            merged_ids.add(e1.id)
            merged_ids.add(e2.id)
            n_merged += 1

        logger.info(
            f"EntityAligner done: {n_merged} pairs merged; "
            f"{len(current_entities)} entities, {len(current_relations)} relations remaining."
        )
        return current_entities, current_relations

    # ------------------------------------------------------------------
    # Step 1: candidate detection
    # ------------------------------------------------------------------

    def _find_candidates(self, entities: List[Entity]) -> List[Tuple[Entity, Entity]]:
        """
        Return entity pairs that are likely duplicates.

        Only pairs with identical ``entity_type`` and a SequenceMatcher ratio
        >= ``self.threshold`` on lower-cased names are returned.
        """
        candidates: List[Tuple[Entity, Entity]] = []
        for i in range(len(entities)):
            for j in range(i + 1, len(entities)):
                e1, e2 = entities[i], entities[j]
                if e1.entity_type != e2.entity_type:
                    continue
                ratio = difflib.SequenceMatcher(
                    None, e1.entity_name.lower(), e2.entity_name.lower()
                ).ratio()
                if ratio >= self.threshold:
                    candidates.append((e1, e2))
        return candidates

    # ------------------------------------------------------------------
    # Step 2: LLM verification
    # ------------------------------------------------------------------

    async def _verify_pair(self, e1: Entity, e2: Entity) -> EntityAlignmentModel:
        """
        Ask the LLM whether two entities refer to the same real-world entity.

        Uses structured output (``EntityAlignmentModel``). On any error returns
        a safe ``should_merge=False`` result so the pipeline never blocks.
        """
        user_message = (
            f"Entity A:\n"
            f"  name: {e1.entity_name}\n"
            f"  type: {e1.entity_type}\n"
            f"  description: {(e1.description or '').strip()[:300]}\n\n"
            f"Entity B:\n"
            f"  name: {e2.entity_name}\n"
            f"  type: {e2.entity_type}\n"
            f"  description: {(e2.description or '').strip()[:300]}\n\n"
            f"Should these two entries be merged into one entity?"
        )
        conversation = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ]
        try:
            return await self.llm.chat_completion(
                conversation, output_schema=EntityAlignmentModel
            )
        except Exception as exc:
            logger.warning(
                f"EntityAligner: LLM verification failed for "
                f"({e1.entity_name!r}, {e2.entity_name!r}): {exc}"
            )
            return EntityAlignmentModel(
                should_merge=False,
                merged_entity_name=e1.entity_name,
                merged_entity_type=e1.entity_type,
                merged_description=e1.description or "",
            )

    # ------------------------------------------------------------------
    # Step 3: atomic merge
    # ------------------------------------------------------------------

    def _apply_merge(
        self,
        e1: Entity,
        e2: Entity,
        result: EntityAlignmentModel,
        entities: List[Entity],
        relations: List[Relation],
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Merge e1 and e2 into a new entity and redirect all their edges.

        Steps:
          1. Remove e1 and e2 from the entity list.
          2. Create the merged entity (auto-generated id from name+type).
          3. Rebuild relation list: replace e1.id/e2.id with merged.id in
             subject_id/object_id; also update display names.
          4. Discard self-loops (subject_id == object_id after substitution).
        """
        merged = Entity(
            entity_name=result.merged_entity_name,
            entity_type=result.merged_entity_type,
            description=result.merged_description,
            source_chunk_id=sorted({
                chunk
                for e in (e1, e2)
                for chunk in e.source_chunk_id
            }),
            documents_id=sorted({
                doc
                for e in (e1, e2)
                for doc in e.documents_id
            }),
        )

        old_ids = {e1.id, e2.id}
        new_entities = [e for e in entities if e.id not in old_ids]
        new_entities.append(merged)

        new_relations: List[Relation] = []
        for rel in relations:
            subj_id = merged.id if rel.subject_id in old_ids else rel.subject_id
            obj_id = merged.id if rel.object_id in old_ids else rel.object_id

            if subj_id == obj_id:
                logger.debug(
                    f"Discarding self-loop after merge: "
                    f"({rel.subject_name}, {rel.relation_type}, {rel.object_name})"
                )
                continue

            subj_name = (
                result.merged_entity_name if rel.subject_id in old_ids else rel.subject_name
            )
            obj_name = (
                result.merged_entity_name if rel.object_id in old_ids else rel.object_name
            )

            new_relations.append(Relation(
                subject_id=subj_id,
                object_id=obj_id,
                subject_name=subj_name,
                object_name=obj_name,
                relation_type=rel.relation_type,
                description=rel.description,
                relation_strength=rel.relation_strength,
                source_chunk_id=list(rel.source_chunk_id),
            ))

        logger.debug(
            f"Merged {e1.entity_name!r} + {e2.entity_name!r} "
            f"→ {merged.entity_name!r} (id={merged.id!r})"
        )
        return new_entities, new_relations


# ---------------------------------------------------------------------------
# Union-Find helper (used by AlternativeNameMerger)
# ---------------------------------------------------------------------------

class _UnionFind:
    """Path-compressed union-find over string IDs."""

    def __init__(self, ids: Any) -> None:
        self.parent: Dict[str, str] = {i: i for i in ids}

    def find(self, x: str) -> str:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, x: str, y: str) -> None:
        px, py = self.find(x), self.find(y)
        if px != py:
            self.parent[py] = px


# ---------------------------------------------------------------------------
# AlternativeNameMerger
# ---------------------------------------------------------------------------

class AlternativeNameMerger(GraphBuilderModule):
    """
    Graph-builder module that merges entities connected by ALTERNATIVE_NAME
    (or a custom set of relation types) into a single canonical entity.

    When entity A has a relation ``ALTERNATIVE_NAME → B``, A and B refer to
    the same real-world object. This module:

      1. Finds all relations whose ``relation_type`` (case-insensitive) is in
         ``relation_types`` (default: ``{"ALTERNATIVE_NAME"}``).
      2. Groups transitively connected entities with union-find.
      3. For each group picks the canonical entity — the one with the longest
         description (most information); ties broken by name length.
      4. Creates a merged entity combining all source chunk / document IDs.
      5. Redirects every other relation from old IDs to the merged entity ID.
      6. Drops the processed ``ALTERNATIVE_NAME`` edges and any resulting
         self-loops.

    No LLM or embedder calls — pure in-memory graph operation.

    :param relation_types: Relation type strings treated as "same-as" signals
        (matched case-insensitively). Default: ``{"ALTERNATIVE_NAME"}``.
    """

    _DEFAULT_RELATION_TYPES: Set[str] = {"ALTERNATIVE_NAME"}

    def __init__(self, relation_types: Set[str] | None = None) -> None:
        super().__init__()
        self._relation_types: Set[str] = {
            r.upper() for r in (relation_types or self._DEFAULT_RELATION_TYPES)
        }

    async def run(
        self,
        entities: List[Entity],
        relations: List[Relation],
        **_kwargs: Any,
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Merge entities linked by ALTERNATIVE_NAME relations.

        :param entities: Input entity list.
        :param relations: Input relation list.
        :return: Deduplicated (entities, relations) with ALTERNATIVE_NAME edges removed.
        """
        alt_relations = [
            r for r in relations
            if r.relation_type.upper() in self._relation_types
        ]
        if not alt_relations:
            return entities, relations

        logger.info(
            f"AlternativeNameMerger: {len(alt_relations)} "
            f"{'/'.join(self._relation_types)} relation(s) found."
        )

        entity_map: Dict[str, Entity] = {e.id: e for e in entities}

        # Build union-find; skip edges that reference unknown entity IDs
        uf = _UnionFind(entity_map.keys())
        for rel in alt_relations:
            if rel.subject_id in entity_map and rel.object_id in entity_map:
                uf.union(rel.subject_id, rel.object_id)

        # Group entities by their union-find root
        groups: Dict[str, List[str]] = {}
        for eid in entity_map:
            groups.setdefault(uf.find(eid), []).append(eid)

        # Only groups with ≥2 members need merging
        merge_groups = {root: ids for root, ids in groups.items() if len(ids) > 1}
        if not merge_groups:
            return entities, relations

        # Build old_id → merged Entity mapping
        id_to_merged: Dict[str, Entity] = {}
        new_merged_entities: List[Entity] = []

        for ids in merge_groups.values():
            group_entities = [entity_map[eid] for eid in ids]
            canonical = self._pick_canonical(group_entities)
            merged = Entity(
                entity_name=canonical.entity_name,
                entity_type=canonical.entity_type,
                description=canonical.description,
                source_chunk_id=sorted({
                    c for e in group_entities for c in e.source_chunk_id
                }),
                documents_id=sorted({
                    d for e in group_entities for d in e.documents_id
                }),
            )
            for eid in ids:
                id_to_merged[eid] = merged
            new_merged_entities.append(merged)
            logger.info(
                f"AlternativeNameMerger: "
                f"{[e.entity_name for e in group_entities]} → {merged.entity_name!r}"
            )

        merged_old_ids = set(id_to_merged.keys())
        final_entities = (
            [e for e in entities if e.id not in merged_old_ids]
            + new_merged_entities
        )

        # Redirect relations; drop ALTERNATIVE_NAME edges and self-loops
        alt_relation_ids: Set[str] = {r.id for r in alt_relations}
        final_relations: List[Relation] = []

        for rel in relations:
            if rel.id in alt_relation_ids:
                continue

            new_subj_id = rel.subject_id
            new_subj_name = rel.subject_name
            if rel.subject_id in id_to_merged:
                m = id_to_merged[rel.subject_id]
                new_subj_id = m.id
                new_subj_name = m.entity_name

            new_obj_id = rel.object_id
            new_obj_name = rel.object_name
            if rel.object_id in id_to_merged:
                m = id_to_merged[rel.object_id]
                new_obj_id = m.id
                new_obj_name = m.entity_name

            if new_subj_id == new_obj_id:
                logger.debug(
                    f"AlternativeNameMerger: discarding self-loop "
                    f"({rel.subject_name}, {rel.relation_type}, {rel.object_name})"
                )
                continue

            final_relations.append(Relation(
                subject_id=new_subj_id,
                object_id=new_obj_id,
                subject_name=new_subj_name,
                object_name=new_obj_name,
                relation_type=rel.relation_type,
                description=rel.description,
                relation_strength=rel.relation_strength,
                source_chunk_id=list(rel.source_chunk_id),
            ))

        logger.info(
            f"AlternativeNameMerger done: "
            f"{len(entities)} → {len(final_entities)} entities, "
            f"{len(relations)} → {len(final_relations)} relations."
        )
        return final_entities, final_relations

    @staticmethod
    def _pick_canonical(group: List[Entity]) -> Entity:
        """
        Select the most informative entity from a merge group.

        Prefers the entity with the longest description; ties broken by name length.
        """
        return max(group, key=lambda e: (len(e.description or ""), len(e.entity_name)))
