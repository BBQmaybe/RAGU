"""
Ontology-based triplet verification against Wikidata property constraints.

For every refined triplet ``(subject, relation, object)`` the verifier:

1. Resolves the *relation* label to a Wikidata property PID.
2. Fetches the **subject-type constraint** (Q21503250) and the
   **value-type constraint** (Q21510865) attached to that property.
3. Resolves the *subject* and *object* labels to QIDs and retrieves
   their full class hierarchy (``P31`` / ``P279``).
4. Checks that the subject's type hierarchy intersects with the allowed
   subject types, and likewise for the object.

A triplet is **valid** when *both* intersections are non-empty, *or* when
the property has no constraints defined (open constraint = always valid).

All Wikidata lookups are cached inside :class:`WikidataClient` so repeated
calls for the same PID / QID are free.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import List

from ragu.common.logger import logger
from ragu.graph.wikidata_verification.prompts import Triplet
from ragu.graph.wikidata_verification.wikidata_client import WikidataClient


@dataclass
class VerificationResult:
    """Outcome of ontology verification for a single triplet."""
    triplet: Triplet
    is_valid: bool
    subject_qid: str | None = None
    relation_pid: str | None = None
    object_qid: str | None = None
    reason: str = ""


class OntologyVerifier:
    """
    Verifies triplets against Wikidata property constraints.

    :param wikidata_client: Shared :class:`WikidataClient` instance (caches
        are shared across the whole pipeline).
    :param language: Language code for label resolution.
    """

    def __init__(
        self,
        wikidata_client: WikidataClient,
        language: str = "en",
    ) -> None:
        self.wikidata_client = wikidata_client
        self.language = language
        # Local label→QID cache to avoid redundant API calls
        self._label_qid_cache: dict[str, str | None] = {}
        self._label_pid_cache: dict[str, str | None] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def verify_batch(
        self,
        triplets: List[Triplet],
    ) -> list[VerificationResult]:
        """
        Verify a list of triplets concurrently.

        :param triplets: Refined triplets with canonical labels.
        :return: One :class:`VerificationResult` per input triplet (same
            order).
        """
        tasks = [self._verify_one(t) for t in triplets]
        return list(await asyncio.gather(*tasks))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _resolve_qid(self, label: str) -> str | None:
        if label in self._label_qid_cache:
            return self._label_qid_cache[label]
        qid = await self.wikidata_client.resolve_label_to_qid(
            label, language=self.language, entity_type="item"
        )
        self._label_qid_cache[label] = qid
        return qid

    async def _resolve_pid(self, label: str) -> str | None:
        if label in self._label_pid_cache:
            return self._label_pid_cache[label]
        pid = await self.wikidata_client.resolve_label_to_qid(
            label, language=self.language, entity_type="property"
        )
        self._label_pid_cache[label] = pid
        return pid

    async def _verify_one(self, triplet: Triplet) -> VerificationResult:
        """Verify a single triplet."""

        # 1. Resolve all three labels to Wikidata IDs
        subject_qid, relation_pid, object_qid = await asyncio.gather(
            self._resolve_qid(triplet.subject),
            self._resolve_pid(triplet.relation),
            self._resolve_qid(triplet.object),
        )

        base = dict(
            triplet=triplet,
            subject_qid=subject_qid,
            relation_pid=relation_pid,
            object_qid=object_qid,
        )

        if relation_pid is None:
            logger.debug(
                f"Cannot resolve relation '{triplet.relation}' to PID — "
                f"accepting triplet by default"
            )
            return VerificationResult(**base, is_valid=True,
                                      reason="relation PID not resolved; accepted")

        # 2. Get property constraints
        constraints = await self.wikidata_client.get_property_constraints(
            relation_pid
        )
        allowed_subject = constraints["subject_types"]
        allowed_value = constraints["value_types"]

        # No constraints defined → accept
        if not allowed_subject and not allowed_value:
            return VerificationResult(**base, is_valid=True,
                                      reason="no constraints on property")

        # 3. Get entity hierarchies
        subject_types: set[str] = set()
        object_types: set[str] = set()

        if subject_qid:
            subject_types = await self.wikidata_client.get_entity_types(
                subject_qid
            )
        if object_qid:
            object_types = await self.wikidata_client.get_entity_types(
                object_qid
            )

        # 4. Check intersection
        subject_ok = True
        if allowed_subject:
            if not subject_qid:
                subject_ok = False
            else:
                subject_ok = bool(subject_types & allowed_subject)

        object_ok = True
        if allowed_value:
            if not object_qid:
                object_ok = False
            else:
                object_ok = bool(object_types & allowed_value)

        is_valid = subject_ok and object_ok

        if not is_valid:
            reason_parts: list[str] = []
            if not subject_ok:
                reason_parts.append(
                    f"subject types {subject_types or '{}'} "
                    f"do not intersect allowed {allowed_subject}"
                )
            if not object_ok:
                reason_parts.append(
                    f"object types {object_types or '{}'} "
                    f"do not intersect allowed {allowed_value}"
                )
            reason = "; ".join(reason_parts)
            logger.debug(f"Triplet REJECTED: {triplet} — {reason}")
        else:
            reason = "passed"

        return VerificationResult(**base, is_valid=is_valid, reason=reason)
