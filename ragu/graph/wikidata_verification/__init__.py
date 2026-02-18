"""
Wikidata-based knowledge graph verification module for RAGU.

Implements the three-stage pipeline:
    1. Candidate extraction (LLM, few-shot)
    2. Refinement via FAISS top-k canonical linking + LLM selection
    3. Ontology-based verification against Wikidata property constraints

Usage::

    from ragu.graph.wikidata_verification import WikidataVerificationModule

    module = WikidataVerificationModule(
        client=llm_client,
        embedder=embedder,
        top_k=5,
        verify_ontology=True,
    )

Plug the module into any RAGU :class:`KnowledgeGraph` via
``additional_modules=[module]``.
"""

from ragu.graph.wikidata_verification.module import WikidataVerificationModule
from ragu.graph.wikidata_verification.prompts import Triplet, TripletList
from ragu.graph.wikidata_verification.wikidata_client import WikidataClient
from ragu.graph.wikidata_verification.candidate_linker import WikidataCandidateLinker
from ragu.graph.wikidata_verification.ontology_verifier import (
    OntologyVerifier,
    VerificationResult,
)

__all__ = [
    "WikidataVerificationModule",
    "Triplet",
    "TripletList",
    "WikidataClient",
    "WikidataCandidateLinker",
    "OntologyVerifier",
    "VerificationResult",
]
