"""
Internal schema-based triplet verification.

Validates triplets against the NEREL domain/range constraints defined
in :mod:`schema_verification.schema`.

For every triplet ``(subject, relation, object)`` the verifier checks:

1. The *relation* type is a recognised NEREL relation type.
2. The *subject* entity type falls within the allowed domain.
3. The *object* entity type falls within the allowed range.

A triplet is **valid** when all three conditions hold, *or* when the
constraints for the relation are open (empty sets ⇒ always valid).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List

from ragu.common.logger import logger
from ragu.graph.schema_verification.prompts import Triplet
from ragu.graph.schema_verification.schema import (
    ALLOWED_RELATION_TYPES,
    get_allowed_subject_types,
    get_allowed_object_types,
)


@dataclass
class VerificationResult:
    """Outcome of schema verification for a single triplet."""
    triplet: Triplet
    is_valid: bool
    subject_type: str = ""
    relation_type: str = ""
    object_type: str = ""
    reason: str = ""


class GraphSchemaVerifier:
    """
    Verifies triplets against the internal NEREL schema.

    :param entity_type_lookup: Mapping ``entity_name (lower) → entity_type``
        built from entities already in the pipeline.  Used to resolve the
        types of subject/object in each triplet.
    :param strict_relation: When *True* (default), reject triplets whose
        relation type is not in the NEREL vocabulary.  When *False*, unknown
        relations are accepted.
    """

    def __init__(
        self,
        entity_type_lookup: dict[str, str] | None = None,
        strict_relation: bool = True,
    ) -> None:
        self._type_lookup: dict[str, str] = entity_type_lookup or {}
        self.strict_relation = strict_relation

    def update_type_lookup(self, lookup: dict[str, str]) -> None:
        """Replace the entity-type lookup table."""
        self._type_lookup = lookup

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def verify_batch(
        self,
        triplets: List[Triplet],
    ) -> list[VerificationResult]:
        """
        Verify a list of triplets.

        :param triplets: Triplets with entity types resolved.
        :return: One :class:`VerificationResult` per input triplet.
        """
        return [self._verify_one(t) for t in triplets]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _resolve_entity_type(self, name: str) -> str:
        """Look up the entity type for *name* (case-insensitive)."""
        return self._type_lookup.get(name.lower(), "")

    def _verify_one(self, triplet: Triplet) -> VerificationResult:
        """Verify a single triplet against the NEREL schema."""

        subject_type = self._resolve_entity_type(triplet.subject)
        object_type = self._resolve_entity_type(triplet.object)
        relation_type = triplet.relation.upper()

        base = dict(
            triplet=triplet,
            subject_type=subject_type,
            relation_type=relation_type,
            object_type=object_type,
        )

        # 1. Check relation type is known
        if relation_type not in ALLOWED_RELATION_TYPES:
            if self.strict_relation:
                reason = (
                    f"unknown relation type '{relation_type}'; "
                    f"not in NEREL vocabulary"
                )
                logger.debug(f"Triplet REJECTED: {triplet} — {reason}")
                return VerificationResult(**base, is_valid=False, reason=reason)
            else:
                return VerificationResult(
                    **base, is_valid=True,
                    reason=f"unknown relation '{relation_type}'; accepted (non-strict)",
                )

        # 2. Get domain/range constraints
        allowed_subjects = get_allowed_subject_types(relation_type)
        allowed_objects = get_allowed_object_types(relation_type)

        # Open constraints → always valid
        if not allowed_subjects and not allowed_objects:
            return VerificationResult(
                **base, is_valid=True, reason="no constraints on relation"
            )

        # 3. If entity type not resolved, accept by default
        #    (we can't reject what we can't classify)
        if not subject_type and not object_type:
            return VerificationResult(
                **base, is_valid=True,
                reason="entity types not resolved; accepted by default",
            )

        # 4. Check domain constraint (subject type)
        subject_ok = True
        if allowed_subjects and subject_type:
            subject_ok = subject_type.upper() in allowed_subjects

        # 5. Check range constraint (object type)
        object_ok = True
        if allowed_objects and object_type:
            object_ok = object_type.upper() in allowed_objects

        is_valid = subject_ok and object_ok

        if not is_valid:
            reason_parts: list[str] = []
            if not subject_ok:
                reason_parts.append(
                    f"subject type '{subject_type}' not in allowed "
                    f"domain {set(allowed_subjects)}"
                )
            if not object_ok:
                reason_parts.append(
                    f"object type '{object_type}' not in allowed "
                    f"range {set(allowed_objects)}"
                )
            reason = "; ".join(reason_parts)
            logger.debug(f"Triplet REJECTED: {triplet} — {reason}")
        else:
            reason = "passed"

        return VerificationResult(**base, is_valid=is_valid, reason=reason)
