"""
Prompts and Pydantic response models for the two-step knowledge
extraction and refinement pipeline with NEREL schema awareness.

Step 1 — **Candidate extraction**: few-shot prompt that asks the LLM to
          output a JSON array of ``{subject, relation, object}`` triplets.
Step 2 — **Refinement**: prompt that provides candidate canonical mappings
          from the NEREL vocabulary and asks the LLM to select the best
          match from each top-k list.
"""

from __future__ import annotations

from typing import List

from pydantic import BaseModel, Field

from ragu.common.prompts.messages import (
    ChatMessages,
    SystemMessage,
    UserMessage,
    AIMessage,
)

# ---------------------------------------------------------------------------
# Pydantic response models
# ---------------------------------------------------------------------------

class Triplet(BaseModel):
    """A single (subject, relation, object) fact."""
    subject: str = Field(..., description="Subject entity name")
    relation: str = Field(..., description="Relation / predicate name")
    object: str = Field(..., description="Object entity name")


class TripletList(BaseModel):
    """Structured LLM response — list of triplets."""
    triplets: List[Triplet] = Field(
        default_factory=list,
        description="List of extracted (subject, relation, object) triplets",
    )


# ---------------------------------------------------------------------------
# Step 1 — Candidate extraction (few-shot)
# ---------------------------------------------------------------------------

EXTRACTION_SYSTEM_PROMPT = (
    "You are a knowledge-graph construction algorithm.  "
    "Your task is to extract factual triplets from the provided text.  "
    "Output ONLY a JSON object that matches the provided schema.  "
    "Each triplet must have exactly three string fields: "
    '"subject", "relation", "object".  '
    "Use informative relation names that describe the relationship "
    "(e.g. LOCATED_IN, WORKS_AS, DATE_OF_BIRTH).  "
    "Do not include any explanation or commentary."
)

# Three in-context examples using NEREL-style relations.
EXTRACTION_FEW_SHOT_EXAMPLES: list[tuple[str, str]] = [
    # Example 1
    (
        "Text: Tahiti Honey is a 1943 American comedy film directed by "
        "John H. Auer and written by Lillie Hayward.",
        '{"triplets": ['
        '{"subject": "Tahiti Honey", "relation": "FOUNDED_BY", "object": "John H. Auer"}, '
        '{"subject": "Tahiti Honey", "relation": "FOUNDED_BY", "object": "Lillie Hayward"}, '
        '{"subject": "Tahiti Honey", "relation": "DATE_OF_CREATION", "object": "1943"}'
        "]}"
    ),
    # Example 2
    (
        "Text: Marie Curie was a Polish-French physicist and chemist who "
        "conducted pioneering research on radioactivity. She was the first "
        "woman to win a Nobel Prize.",
        '{"triplets": ['
        '{"subject": "Marie Curie", "relation": "ORIGINS_FROM", "object": "Poland"}, '
        '{"subject": "Marie Curie", "relation": "ORIGINS_FROM", "object": "France"}, '
        '{"subject": "Marie Curie", "relation": "WORKS_AS", "object": "physicist"}, '
        '{"subject": "Marie Curie", "relation": "WORKS_AS", "object": "chemist"}, '
        '{"subject": "Marie Curie", "relation": "AWARDED_WITH", "object": "Nobel Prize"}'
        "]}"
    ),
    # Example 3
    (
        "Text: The Eiffel Tower is a wrought-iron lattice tower on the "
        "Champ de Mars in Paris, France. It was designed by Gustave Eiffel "
        "and completed in 1889.",
        '{"triplets": ['
        '{"subject": "Eiffel Tower", "relation": "LOCATED_IN", "object": "Paris"}, '
        '{"subject": "Paris", "relation": "PART_OF", "object": "France"}, '
        '{"subject": "Eiffel Tower", "relation": "FOUNDED_BY", "object": "Gustave Eiffel"}, '
        '{"subject": "Eiffel Tower", "relation": "DATE_OF_CREATION", "object": "1889"}'
        "]}"
    ),
]


def build_extraction_messages(text: str) -> ChatMessages:
    """
    Assemble the full few-shot extraction conversation for a single text.

    :param text: Raw input text from which triplets should be extracted.
    :return: Ready-to-send :class:`ChatMessages`.
    """
    msgs = [SystemMessage(content=EXTRACTION_SYSTEM_PROMPT)]

    for example_user, example_assistant in EXTRACTION_FEW_SHOT_EXAMPLES:
        msgs.append(UserMessage(content=example_user))
        msgs.append(AIMessage(content=example_assistant))

    msgs.append(UserMessage(content=f"Text: {text}"))
    return ChatMessages.from_messages(msgs)


# ---------------------------------------------------------------------------
# Step 2 — Refinement
# ---------------------------------------------------------------------------

REFINEMENT_SYSTEM_PROMPT = (
    "You are a knowledge-graph entity and relation normalisation algorithm.  "
    "You are given:\n"
    "  1. An original text.\n"
    "  2. A list of extracted triplets.\n"
    "  3. For every subject, relation and object of each triplet — a list "
    "of candidate canonical names from the graph schema vocabulary.\n\n"
    "Your task: for every element of every triplet choose the BEST "
    "canonical name from the corresponding candidate list.  "
    "You may keep the original name ONLY if it already appears in the "
    "candidate list and fits the context better than the alternatives.  "
    "Output ONLY a JSON object matching the provided schema — "
    "an array of triplets with fields \"subject\", \"relation\", \"object\" "
    "containing the selected canonical names.  "
    "Do not add commentary."
)


def build_refinement_messages(
    text: str,
    triplets: List[Triplet],
    subject_mappings: dict[str, list[str]],
    relation_mappings: dict[str, list[str]],
    object_mappings: dict[str, list[str]],
) -> ChatMessages:
    """
    Build the refinement prompt for a single text + its triplets.

    :param text: The original input text (or reconstructed context).
    :param triplets: Candidate triplets from Step 1.
    :param subject_mappings: ``{extracted_subject: [cand1 ... cand5]}``.
    :param relation_mappings: ``{extracted_relation: [cand1 ... cand5]}``.
    :param object_mappings: ``{extracted_object: [cand1 ... cand5]}``.
    :return: Ready-to-send :class:`ChatMessages`.
    """
    lines: list[str] = [f"Text: {text}", ""]
    lines.append("Triplets and corresponding canonical name candidates:")

    for i, t in enumerate(triplets, 1):
        lines.append(f"\n--- Triplet {i} ---")
        lines.append(
            f'{{"subject": "{t.subject}", '
            f'"relation": "{t.relation}", '
            f'"object": "{t.object}"}}'
        )

        s_cands = subject_mappings.get(t.subject, [t.subject])
        r_cands = relation_mappings.get(t.relation, [t.relation])
        o_cands = object_mappings.get(t.object, [t.object])

        lines.append(f"Subject candidates: {{\"{t.subject}\": {s_cands}}}")
        lines.append(f"Relation candidates: {{\"{t.relation}\": {r_cands}}}")
        lines.append(f"Object candidates: {{\"{t.object}\": {o_cands}}}")

    user_content = "\n".join(lines)
    return ChatMessages.from_messages([
        SystemMessage(content=REFINEMENT_SYSTEM_PROMPT),
        UserMessage(content=user_content),
    ])
