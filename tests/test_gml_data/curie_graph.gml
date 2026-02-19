graph [
  multigraph 1
  node [
    id 0
    label "ent-curie"
    entity_name "Marie Curie"
    entity_type "PERSON"
    description "Polish-French physicist and chemist"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 1
    label "ent-poland"
    entity_name "Poland"
    entity_type "COUNTRY"
    description "A country in Central Europe"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 2
    label "ent-physicist"
    entity_name "physicist"
    entity_type "PROFESSION"
    description "A scientist studying physics"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 3
    label "ent-nobel"
    entity_name "Nobel Prize"
    entity_type "AWARD"
    description "International award for outstanding achievements"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 4
    label "ent-pierre"
    entity_name "Pierre Curie"
    entity_type "PERSON"
    description "French physicist"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 5
    label "ent-paris"
    entity_name "Paris"
    entity_type "CITY"
    description "Capital of France"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 6
    label "ent-sorbonne"
    entity_name "Sorbonne"
    entity_type "ORGANIZATION"
    description "University in Paris"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 7
    label "ent-irene"
    entity_name "Irene Joliot-Curie"
    entity_type "PERSON"
    description "Daughter of Marie and Pierre Curie"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 8
    label "ent-1867"
    entity_name "1867"
    entity_type "DATE"
    description "Year of Marie Curie birth"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 9
    label "ent-institute"
    entity_name "Institut Curie"
    entity_type "ORGANIZATION"
    description "Research institute founded by Curie"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 10
    label "ent-chemist"
    entity_name "chemist"
    entity_type "PROFESSION"
    description "A scientist studying chemistry"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  node [
    id 11
    label "ent-orphan"
    entity_name "Radioactivity"
    entity_type "CONCEPT"
    description "The phenomenon of radioactive decay"
    source_chunk_id "c1"
    documents_id "d1"
    clusters "[]"
  ]
  edge [
    source 0
    target 1
    key "rel-001"
    subject_name "Marie Curie"
    object_name "Poland"
    relation_type "ORIGINS_FROM"
    description "Marie Curie originated from Poland"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-001"
  ]
  edge [
    source 0
    target 2
    key "rel-002"
    subject_name "Marie Curie"
    object_name "physicist"
    relation_type "WORKS_AS"
    description "Marie Curie worked as a physicist"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-002"
  ]
  edge [
    source 0
    target 10
    key "rel-003"
    subject_name "Marie Curie"
    object_name "chemist"
    relation_type "WORKS_AS"
    description "Marie Curie worked as a chemist"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-003"
  ]
  edge [
    source 0
    target 3
    key "rel-004"
    subject_name "Marie Curie"
    object_name "Nobel Prize"
    relation_type "AWARDED_WITH"
    description "Marie Curie received the Nobel Prize"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-004"
  ]
  edge [
    source 0
    target 4
    key "rel-005"
    subject_name "Marie Curie"
    object_name "Pierre Curie"
    relation_type "SPOUSE"
    description "Marie Curie was married to Pierre Curie"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-005"
  ]
  edge [
    source 0
    target 7
    key "rel-006"
    subject_name "Marie Curie"
    object_name "Irene Joliot-Curie"
    relation_type "PARENT_OF"
    description "Marie Curie was mother of Irene"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-006"
  ]
  edge [
    source 0
    target 8
    key "rel-007"
    subject_name "Marie Curie"
    object_name "1867"
    relation_type "DATE_OF_BIRTH"
    description "Marie Curie was born in 1867"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-007"
  ]
  edge [
    source 0
    target 5
    key "rel-008"
    subject_name "Marie Curie"
    object_name "Paris"
    relation_type "PLACE_RESIDES_IN"
    description "Marie Curie lived in Paris"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-008"
  ]
  edge [
    source 4
    target 6
    key "rel-009"
    subject_name "Pierre Curie"
    object_name "Sorbonne"
    relation_type "WORKPLACE"
    description "Pierre Curie worked at the Sorbonne"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-009"
  ]
  edge [
    source 0
    target 9
    key "rel-010"
    subject_name "Marie Curie"
    object_name "Institut Curie"
    relation_type "FOUNDED_BY"
    description "Institut Curie was founded by Marie Curie"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-010"
  ]
  edge [
    source 6
    target 5
    key "rel-011"
    subject_name "Sorbonne"
    object_name "Paris"
    relation_type "LOCATED_IN"
    description "The Sorbonne is located in Paris"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-011"
  ]
  edge [
    source 0
    target 5
    key "rel-bad1"
    subject_name "Marie Curie"
    object_name "Paris"
    relation_type "TELEPORTED_TO"
    description "Invalid relation type for testing"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-bad1"
  ]
  edge [
    source 3
    target 1
    key "rel-bad2"
    subject_name "Nobel Prize"
    object_name "Poland"
    relation_type "SPOUSE"
    description "Invalid: AWARD cannot be SPOUSE of COUNTRY"
    relation_strength 1.0
    source_chunk_id "c1"
    id "rel-bad2"
  ]
]
