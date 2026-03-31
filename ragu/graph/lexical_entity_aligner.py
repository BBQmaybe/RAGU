"""
Lexical entity alignment for the RAGU knowledge graph pipeline.

All merge decisions are made purely from string features — no LLM calls,
no embedder calls, zero API cost.

Four lexical signals are computed for every same-type entity pair:

  1. sequence_ratio   — difflib.SequenceMatcher character-level similarity.
  2. token_jaccard    — Jaccard index on word-token sets:
                        |tokens(A) ∩ tokens(B)| / |tokens(A) ∪ tokens(B)|.
  3. abbreviation     — 1.0 if one name is an abbreviation of the other
                        (its letters match the initials of the other's tokens).
  4. prefix_token     — fraction of shorter-name tokens that appear as
                        prefixes of some token in the longer name.

A weighted composite score is computed:

    score = w_seq   * sequence_ratio
          + w_jac   * token_jaccard
          + w_abbr  * abbreviation
          + w_pfx   * prefix_token

If ``score >= composite_threshold``, the pair is merged.
The canonical entity is the one with the longer description; ties are broken
by name length (longer name preferred as more informative).

Usage as a pipeline module (disabled by default)::

    from ragu.graph.lexical_entity_aligner import LexicalEntityAligner

    aligner = LexicalEntityAligner(enabled=True)
    knowledge_graph = KnowledgeGraph(..., additional_modules=[aligner])
"""

from __future__ import annotations

import difflib
import re
import unicodedata
from typing import Any, Dict, List, Set, Tuple

from ragu.common.logger import logger
from ragu.graph.graph_builder_pipeline import GraphBuilderModule
from ragu.graph.types import Entity, Relation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """
    Lowercase, strip punctuation and extra spaces, normalise Unicode.

    Used before every string comparison so that punctuation and casing
    differences do not affect similarity scores.
    """
    text = unicodedata.normalize("NFC", text)
    text = text.lower()
    text = re.sub(r"[«»„""\"\'\-–—]", " ", text)
    text = re.sub(r"[^\w\s]", "", text)
    return re.sub(r"\s+", " ", text).strip()


def _tokens(name: str) -> List[str]:
    """Return non-empty word tokens of a normalised name."""
    return [t for t in _normalize(name).split() if t]


# ---------------------------------------------------------------------------
# Lexical similarity metrics
# ---------------------------------------------------------------------------

def _sequence_ratio(a: str, b: str) -> float:
    """
    Character-level SequenceMatcher similarity on normalised names.

    :return: Float in [0, 1].
    """
    na, nb = _normalize(a), _normalize(b)
    return difflib.SequenceMatcher(None, na, nb).ratio()


def _token_jaccard(a: str, b: str) -> float:
    """
    Jaccard index on word-token sets.

    Example: ``"Иван Петров"`` and ``"Петров Иван"`` → 1.0.

    :return: Float in [0, 1]; 0.0 for empty token sets.
    """
    ta, tb = set(_tokens(a)), set(_tokens(b))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _is_abbreviation(a: str, b: str) -> float:
    """
    Return 1.0 if one name is an abbreviation / acronym of the other.

    Checks both directions:
      - ``a`` == concatenation of first letters of ``b`` tokens
        (e.g. ``"США"`` vs ``"Соединённые Штаты Америки"``).
      - ``b`` == concatenation of first letters of ``a`` tokens.

    :return: 1.0 if an abbreviation match is detected, 0.0 otherwise.
    """
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0

    initials_of_b = "".join(t[0] for t in tb)
    initials_of_a = "".join(t[0] for t in ta)

    na, nb = _normalize(a).replace(" ", ""), _normalize(b).replace(" ", "")
    if na == initials_of_b or nb == initials_of_a:
        return 1.0
    return 0.0


def _prefix_token_ratio(a: str, b: str) -> float:
    """
    Fraction of shorter-name tokens that are a prefix of some token in the
    longer name.

    Catches cases like:
      - ``"Ант."`` vs ``"Антон"``
      - ``"Яков Васильевич"`` vs ``"Яков Васильевич Булдеев"``

    :return: Float in [0, 1].
    """
    ta, tb = _tokens(a), _tokens(b)
    if not ta or not tb:
        return 0.0

    shorter, longer = (ta, tb) if len(ta) <= len(tb) else (tb, ta)

    matched = 0
    for s_tok in shorter:
        if any(l_tok.startswith(s_tok) or s_tok.startswith(l_tok) for l_tok in longer):
            matched += 1
    return matched / len(shorter)


# ---------------------------------------------------------------------------
# Composite score
# ---------------------------------------------------------------------------

_DEFAULT_WEIGHTS = {
    "sequence": 0.35,
    "jaccard":  0.25,
    "abbrev":   0.25,
    "prefix":   0.15,
}


def compute_lexical_score(
    a: str,
    b: str,
    weights: Dict[str, float] | None = None,
) -> float:
    """
    Compute a weighted composite lexical similarity score for two entity names.

    :param a: First entity name.
    :param b: Second entity name.
    :param weights: Optional dict with keys ``sequence``, ``jaccard``,
        ``abbrev``, ``prefix``.  Defaults to ``_DEFAULT_WEIGHTS``.
    :return: Composite score in [0, 1].
    """
    w = weights or _DEFAULT_WEIGHTS
    score = (
        w.get("sequence", 0.35) * _sequence_ratio(a, b)
        + w.get("jaccard",  0.25) * _token_jaccard(a, b)
        + w.get("abbrev",   0.25) * _is_abbreviation(a, b)
        + w.get("prefix",   0.15) * _prefix_token_ratio(a, b)
    )
    return min(score, 1.0)


# ---------------------------------------------------------------------------
# Main module
# ---------------------------------------------------------------------------

class LexicalEntityAligner(GraphBuilderModule):
    """
    Graph-builder module that deduplicates entities using only lexical string
    similarity — no LLM, no embedder, zero API cost.

    By default this module is **disabled** (``enabled=False``) to ensure
    existing pipelines are unaffected. Set ``enabled=True`` to activate.

    For every pair of same-type entities a composite lexical score is computed
    from four signals (see module docstring).  Pairs that exceed
    ``composite_threshold`` are merged without any external call.

    The canonical entity after merging is chosen by:
      1. Longest description (most information).
      2. Longest name (more complete form) as a tiebreaker.

    :param composite_threshold: Minimum composite score for a merge decision
        (default 0.75). Raise to reduce false positives; lower to catch more
        abbreviation / short-form matches.
    :param weights: Custom weights for the four lexical signals.  Keys:
        ``sequence``, ``jaccard``, ``abbrev``, ``prefix``.
        Must sum to 1.0 (not enforced, but recommended).
    :param enabled: Activate the module (default False).
    """

    def __init__(
        self,
        composite_threshold: float = 0.75,
        weights: Dict[str, float] | None = None,
        enabled: bool = False,
    ) -> None:
        super().__init__()
        self.composite_threshold = composite_threshold
        self.weights = weights or dict(_DEFAULT_WEIGHTS)
        self.enabled = enabled

    async def run(
        self,
        entities: List[Entity],
        relations: List[Relation],
        **kwargs: Any,
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Execute lexical alignment.

        When disabled, returns input unchanged.  When enabled, merges
        entity pairs whose composite lexical score exceeds the threshold.

        :param entities: Input entity list.
        :param relations: Input relation list.
        :return: Deduplicated (entities, relations).
        """
        if not self.enabled:
            return entities, relations

        if len(entities) < 2:
            return entities, relations

        logger.info(
            f"LexicalEntityAligner: {len(entities)} entities, "
            f"{len(relations)} relations. threshold={self.composite_threshold}"
        )

        candidate_pairs = self._find_candidates(entities)
        if not candidate_pairs:
            logger.info("LexicalEntityAligner: no candidate pairs found.")
            return entities, relations

        logger.info(f"LexicalEntityAligner: {len(candidate_pairs)} candidate pairs to merge.")

        current_entities = list(entities)
        current_relations = list(relations)
        merged_ids: Set[str] = set()
        n_merged = 0

        for e1, e2, score in candidate_pairs:
            if e1.id in merged_ids or e2.id in merged_ids:
                logger.debug(
                    f"Skipping {e1.entity_name!r} + {e2.entity_name!r}: "
                    "already consumed."
                )
                continue
            current_entities, current_relations = self._apply_merge(
                e1, e2, current_entities, current_relations
            )
            merged_ids.add(e1.id)
            merged_ids.add(e2.id)
            n_merged += 1
            logger.debug(
                f"Merged {e1.entity_name!r} + {e2.entity_name!r}  score={score:.3f}"
            )

        logger.info(
            f"LexicalEntityAligner done: {n_merged} pairs merged; "
            f"{len(current_entities)} entities, {len(current_relations)} relations."
        )
        return current_entities, current_relations

    # ------------------------------------------------------------------
    # Candidate detection
    # ------------------------------------------------------------------

    def _find_candidates(
        self, entities: List[Entity]
    ) -> List[Tuple[Entity, Entity, float]]:
        """
        Return entity pairs whose composite lexical score exceeds the threshold.

        Only pairs with the same ``entity_type`` are considered.

        :return: List of ``(entity_1, entity_2, score)`` triples, sorted by
            score descending (highest-confidence merges applied first).
        """
        candidates: List[Tuple[Entity, Entity, float]] = []

        for i in range(len(entities)):
            for j in range(i + 1, len(entities)):
                e1, e2 = entities[i], entities[j]
                if e1.entity_type != e2.entity_type:
                    continue

                score = compute_lexical_score(e1.entity_name, e2.entity_name, self.weights)
                if score >= self.composite_threshold:
                    candidates.append((e1, e2, score))

        # Process highest-confidence merges first to avoid chain-conflict issues
        candidates.sort(key=lambda x: x[2], reverse=True)
        return candidates

    # ------------------------------------------------------------------
    # Canonical entity selection
    # ------------------------------------------------------------------

    @staticmethod
    def _pick_canonical(e1: Entity, e2: Entity) -> Entity:
        """
        Select the more informative entity as the canonical representation.

        Prefers the entity with the longest description; ties broken by name length.
        """
        return max(
            (e1, e2),
            key=lambda e: (len(e.description or ""), len(e.entity_name)),
        )

    # ------------------------------------------------------------------
    # Atomic merge (no LLM — canonical chosen deterministically)
    # ------------------------------------------------------------------

    def _apply_merge(
        self,
        e1: Entity,
        e2: Entity,
        entities: List[Entity],
        relations: List[Relation],
    ) -> Tuple[List[Entity], List[Relation]]:
        """
        Merge e1 and e2 into a single canonical entity and redirect all edges.

        Steps:
          1. Choose the canonical entity (longer description / name wins).
          2. Remove e1 and e2 from the entity list; insert merged entity.
          3. Redirect all relations: replace e1.id / e2.id with merged.id in
             both subject_id and object_id; update display names accordingly.
          4. Discard self-loops (subject_id == object_id after substitution).
        """
        canonical = self._pick_canonical(e1, e2)
        other = e2 if canonical is e1 else e1

        merged = Entity(
            entity_name=canonical.entity_name,
            entity_type=canonical.entity_type,
            description=canonical.description,
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
            subj_id   = merged.id if rel.subject_id in old_ids else rel.subject_id
            obj_id    = merged.id if rel.object_id  in old_ids else rel.object_id

            if subj_id == obj_id:
                logger.debug(
                    f"LexicalEntityAligner: discarding self-loop "
                    f"({rel.subject_name}, {rel.relation_type}, {rel.object_name})"
                )
                continue

            subj_name = (
                merged.entity_name if rel.subject_id in old_ids else rel.subject_name
            )
            obj_name = (
                merged.entity_name if rel.object_id in old_ids else rel.object_name
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
            f"LexicalEntityAligner: {other.entity_name!r} → {merged.entity_name!r}"
        )
        return new_entities, new_relations
