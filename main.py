from dotenv import load_dotenv
import os
import sys
import asyncio

load_dotenv()

from ragu.common.logger import logger
logger.remove()
logger.add(sys.stdout, level="DEBUG")

from ragu import (
    SimpleChunker,
    KnowledgeGraph,
    BuilderArguments,
    Settings,
    ArtifactsExtractorLLM,
)
from ragu.graph.lexical_entity_aligner import LexicalEntityAligner
from ragu.graph.community_entity_aligner import CommunityEntityAligner
from ragu.graph.schema_verification import SchemaVerificationModule
from ragu.models.embedder import EmbedderOpenAI
from ragu.models.llm import LLMOpenAI
from ragu.models.openai import CachedAsyncOpenAI
from ragu.utils.ragu_utils import read_text_from_files

client = CachedAsyncOpenAI(
    base_url=os.environ['OPENAI_BASE_URL'],
    api_key=os.environ['OPENAI_API_KEY'],
    rate_min_delay=2,
    rate_max_simultaneous=10,
    retry_times_sec=(2, 2, 2, 2, 2),
    cache='./llm_cache',
    debug_errors_storage='./llm_debug',
)

llm = LLMOpenAI(client, "gpt-4o-mini")
embedder = EmbedderOpenAI(client, "text-embedding-3-large", dim=3072)

Settings.storage_folder = "bm1"
Settings.language = "russian"

# shutil.rmtree(Settings.storage_folder, ignore_errors=True)

docs = read_text_from_files("benchmark_ru")

chunker = SimpleChunker(max_chunk_size=1000)

artifact_extractor = ArtifactsExtractorLLM(
    llm=llm,
    do_validation=False,
)

builder_settings = BuilderArguments(
    use_llm_summarization=True,
    vectorize_chunks=True,
)

schema_module = SchemaVerificationModule(
    llm=llm,
    embedder=embedder,
    top_k=5,
    batch_size=40,
    enabled=True,
)
community_aligner = CommunityEntityAligner(
    llm=llm,
    embedder=embedder,
    cosine_threshold=0.85,
    min_community_size=2,
    enabled=True,
)
lexical_aligner = LexicalEntityAligner(
    composite_threshold=0.75,
    enabled=True,
)

knowledge_graph = KnowledgeGraph(
    llm=llm,
    embedder=embedder,
    chunker=chunker,
    artifact_extractor=artifact_extractor,
    builder_settings=builder_settings,
    additional_modules=[schema_module],
)

asyncio.run(knowledge_graph.build_from_docs(docs))