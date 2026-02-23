"""
Schema-based knowledge graph verification module for RAGU.

Implements a three-stage pipeline:
    1. Candidate extraction (LLM, few-shot)
    2. Refinement via FAISS top-k canonical linking + LLM selection
    3. Schema-based verification against NEREL domain/range constraints

All verification is performed internally using the NEREL ontology
(29 entity types, 49 relation types) with no external API dependencies.

The module is **disabled by default** (safe mode).  Pass ``enabled=True``
to activate the verification pipeline at runtime.

Usage::

    from ragu.graph.schema_verification import SchemaVerificationModule

    module = SchemaVerificationModule(
        client=llm_client,
        embedder=embedder,
        enabled=True,     # OFF by default; set True to activate
        top_k=5,
        verify_schema=True,
    )

Plug the module into any RAGU :class:`KnowledgeGraph` via
``additional_modules=[module]``.  When ``enabled=False`` the module
is a no-op and returns the input unchanged.
"""

from ragu.graph.schema_verification.module import SchemaVerificationModule
from ragu.graph.schema_verification.prompts import Triplet, TripletList
from ragu.graph.schema_verification.candidate_linker import SchemaAwareCandidateLinker
from ragu.graph.schema_verification.schema_verifier import (
    GraphSchemaVerifier,
    VerificationResult,
)
from ragu.graph.schema_verification.schema import (
    RELATION_CONSTRAINTS,
    ALLOWED_ENTITY_TYPES,
    ALLOWED_RELATION_TYPES,
    validate_entity_type,
    validate_relation_type,
    get_allowed_subject_types,
    get_allowed_object_types,
)
from ragu.graph.schema_verification.gml_processor import (
    GmlGraphProcessor,
    ProcessingReport,
)

__all__ = [
    "SchemaVerificationModule",
    "Triplet",
    "TripletList",
    "SchemaAwareCandidateLinker",
    "GraphSchemaVerifier",
    "VerificationResult",
    "GmlGraphProcessor",
    "ProcessingReport",
    "RELATION_CONSTRAINTS",
    "ALLOWED_ENTITY_TYPES",
    "ALLOWED_RELATION_TYPES",
    "validate_entity_type",
    "validate_relation_type",
    "get_allowed_subject_types",
    "get_allowed_object_types",
]
