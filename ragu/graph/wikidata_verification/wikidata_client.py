"""
HTTP client for Wikidata REST API and SPARQL (WDQS).

Provides:
* ``search_entities`` / ``search_properties`` — Wikidata ``wbsearchentities``
  action for free-text lookup of items and properties.
* ``get_entity_types`` — SPARQL query that walks the P31/P279 hierarchy and
  returns the full set of ancestor classes for an entity.
* ``get_property_constraints`` — SPARQL query that fetches
  *subject-type constraint* (Q21503250) and *value-type constraint*
  (Q21510865) for a property.
* ``resolve_label_to_qid`` — convenience wrapper: label → QID.

All network calls are **synchronous** (``requests``) but executed via
``asyncio.to_thread`` so the module can be ``await``-ed from async code
without blocking the event loop.

Results are cached in-memory so repeated calls for the same QID / PID are
virtually free.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import requests

from ragu.common.logger import logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WDQS_ENDPOINT = "https://query.wikidata.org/sparql"
_USER_AGENT = "RAGU-WikidataVerification/1.0 (https://github.com/RAGU)"

# Constraints QIDs
_SUBJECT_TYPE_CONSTRAINT = "Q21503250"
_VALUE_TYPE_CONSTRAINT = "Q21510865"

# SPARQL templates --------------------------------------------------------

_ENTITY_TYPES_SPARQL = """
SELECT DISTINCT ?type WHERE {{
  {{ wd:{qid} wdt:P31/wdt:P279* ?type . }}
  UNION
  {{ wd:{qid} wdt:P279/wdt:P279* ?type . }}
}}
LIMIT 500
"""

_PROPERTY_CONSTRAINTS_SPARQL = """
SELECT ?constraintType ?class WHERE {{
  wd:{pid} p:P2302 ?statement .
  ?statement ps:P2302 ?constraintType .
  OPTIONAL {{ ?statement pq:P2308 ?class . }}
  FILTER(?constraintType IN (wd:{sc}, wd:{vc}))
}}
""".replace("{sc}", _SUBJECT_TYPE_CONSTRAINT).replace("{vc}", _VALUE_TYPE_CONSTRAINT)

# ---------------------------------------------------------------------------
# Retry helper
# ---------------------------------------------------------------------------

_MAX_RETRIES = 3
_BACKOFF_BASE = 1.5  # seconds


def _retry_request(method: str, url: str, **kwargs: Any) -> requests.Response:
    """Execute an HTTP request with up to *_MAX_RETRIES* attempts."""
    kwargs.setdefault("timeout", 15)
    kwargs.setdefault("headers", {})
    kwargs["headers"].setdefault("User-Agent", _USER_AGENT)
    last_exc: Exception | None = None
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.request(method, url, **kwargs)
            resp.raise_for_status()
            return resp
        except (requests.RequestException, requests.HTTPError) as exc:
            last_exc = exc
            if attempt < _MAX_RETRIES:
                wait = _BACKOFF_BASE ** attempt
                logger.warning(
                    f"Wikidata request failed (attempt {attempt}/{_MAX_RETRIES}), "
                    f"retrying in {wait:.1f}s: {exc}"
                )
                time.sleep(wait)
    raise RuntimeError(
        f"Wikidata request failed after {_MAX_RETRIES} attempts: {last_exc}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------

class WikidataClient:
    """
    Thin async wrapper around the Wikidata REST API and SPARQL endpoint.

    All heavy I/O is offloaded to a thread-pool via ``asyncio.to_thread``.
    Property constraints and entity-type hierarchies are cached in memory
    for the lifetime of the client instance.
    """

    def __init__(self) -> None:
        self._property_constraints_cache: dict[str, dict[str, set[str]]] = {}
        self._entity_types_cache: dict[str, set[str]] = {}

    # -- Wikidata Search API ------------------------------------------------

    @staticmethod
    def _search_entities_sync(
        query: str,
        language: str = "en",
        limit: int = 10,
        entity_type: str = "item",
    ) -> list[dict[str, str]]:
        """
        Call ``wbsearchentities`` and return a list of candidates.

        Each candidate is ``{"id": "Q…", "label": "…", "description": "…"}``.
        """
        params = {
            "action": "wbsearchentities",
            "search": query,
            "language": language,
            "limit": limit,
            "format": "json",
            "type": entity_type,
        }
        resp = _retry_request("GET", WIKIDATA_API_URL, params=params)
        data = resp.json()
        results: list[dict[str, str]] = []
        for item in data.get("search", []):
            results.append({
                "id": item.get("id", ""),
                "label": item.get("label", ""),
                "description": item.get("description", ""),
            })
        return results

    async def search_entities(
        self,
        query: str,
        language: str = "en",
        limit: int = 10,
    ) -> list[dict[str, str]]:
        """Search Wikidata items by free-text *query*."""
        try:
            return await asyncio.to_thread(
                self._search_entities_sync, query, language, limit, "item"
            )
        except Exception as exc:
            logger.warning(f"Wikidata search_entities failed for '{query}': {exc}")
            return []

    async def search_properties(
        self,
        query: str,
        language: str = "en",
        limit: int = 10,
    ) -> list[dict[str, str]]:
        """Search Wikidata properties by free-text *query*."""
        try:
            return await asyncio.to_thread(
                self._search_entities_sync, query, language, limit, "property"
            )
        except Exception as exc:
            logger.warning(f"Wikidata search_properties failed for '{query}': {exc}")
            return []

    # -- SPARQL helpers -----------------------------------------------------

    @staticmethod
    def _sparql_sync(query: str) -> list[dict[str, Any]]:
        """Execute a SPARQL query against WDQS and return bindings."""
        resp = _retry_request(
            "GET",
            WDQS_ENDPOINT,
            params={"query": query, "format": "json"},
            headers={
                "User-Agent": _USER_AGENT,
                "Accept": "application/sparql-results+json",
            },
        )
        return resp.json().get("results", {}).get("bindings", [])

    @staticmethod
    def _qid_from_uri(uri: str) -> str:
        """``http://www.wikidata.org/entity/Q42`` → ``Q42``."""
        return uri.rsplit("/", 1)[-1] if "/" in uri else uri

    # -- Entity types -------------------------------------------------------

    def _get_entity_types_sync(self, qid: str) -> set[str]:
        if qid in self._entity_types_cache:
            return self._entity_types_cache[qid]
        sparql = _ENTITY_TYPES_SPARQL.format(qid=qid)
        try:
            bindings = self._sparql_sync(sparql)
        except Exception as exc:
            logger.warning(f"SPARQL entity-types failed for {qid}: {exc}")
            return set()
        types = {
            self._qid_from_uri(b["type"]["value"])
            for b in bindings
            if "type" in b
        }
        self._entity_types_cache[qid] = types
        return types

    async def get_entity_types(self, qid: str) -> set[str]:
        """
        Return the full set of ancestor classes for *qid* via P31/P279*.

        Results are cached.
        """
        if qid in self._entity_types_cache:
            return self._entity_types_cache[qid]
        return await asyncio.to_thread(self._get_entity_types_sync, qid)

    # -- Property constraints -----------------------------------------------

    def _get_property_constraints_sync(self, pid: str) -> dict[str, set[str]]:
        if pid in self._property_constraints_cache:
            return self._property_constraints_cache[pid]
        sparql = _PROPERTY_CONSTRAINTS_SPARQL.format(pid=pid)
        subject_types: set[str] = set()
        value_types: set[str] = set()
        try:
            bindings = self._sparql_sync(sparql)
        except Exception as exc:
            logger.warning(f"SPARQL property-constraints failed for {pid}: {exc}")
            result: dict[str, set[str]] = {
                "subject_types": set(),
                "value_types": set(),
            }
            self._property_constraints_cache[pid] = result
            return result

        for b in bindings:
            ct = self._qid_from_uri(b.get("constraintType", {}).get("value", ""))
            cls = b.get("class", {}).get("value", "")
            if not cls:
                continue
            cls_qid = self._qid_from_uri(cls)
            if ct == _SUBJECT_TYPE_CONSTRAINT:
                subject_types.add(cls_qid)
            elif ct == _VALUE_TYPE_CONSTRAINT:
                value_types.add(cls_qid)

        result = {"subject_types": subject_types, "value_types": value_types}
        self._property_constraints_cache[pid] = result
        return result

    async def get_property_constraints(self, pid: str) -> dict[str, set[str]]:
        """
        Return subject-type and value-type constraints for property *pid*.

        Results are cached.  Returns
        ``{"subject_types": {QIDs…}, "value_types": {QIDs…}}``.
        """
        if pid in self._property_constraints_cache:
            return self._property_constraints_cache[pid]
        return await asyncio.to_thread(
            self._get_property_constraints_sync, pid
        )

    # -- Label → QID --------------------------------------------------------

    async def resolve_label_to_qid(
        self,
        label: str,
        language: str = "en",
        entity_type: str = "item",
    ) -> str | None:
        """
        Resolve a human-readable *label* to its Wikidata QID/PID.

        Returns the first search hit or ``None``.
        """
        try:
            results = await asyncio.to_thread(
                self._search_entities_sync, label, language, 1, entity_type
            )
        except Exception as exc:
            logger.warning(f"Wikidata resolve_label_to_qid failed for '{label}': {exc}")
            return None
        if results:
            return results[0]["id"]
        return None
