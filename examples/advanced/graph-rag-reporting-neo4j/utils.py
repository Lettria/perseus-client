import logging
from typing import Callable, List, Optional, cast

from google import genai
from google.genai import types as genai_types
from neo4j import GraphDatabase
from perseus_client.config import settings
from perseus_client.exceptions import PerseusException

logging.basicConfig(level=logging.INFO)


def make_gemini_embed_fn(
    genai_client: Optional[genai.Client] = None,
    model: str = "gemini-embedding-001",
    batch_size: int = 100,
) -> Callable[[List[str]], List[List[float]]]:
    """
    simple_graph_retriever no longer talks to any embedder itself; this
    wires its required embed_fn callback to Gemini's embedding API instead
    of running a local TEI container. Swap this out for Azure OpenAI,
    OpenAI, etc. in your own code.

    batch_size caps how many texts go into a single embed_content call,
    staying under the Gemini API's per-request batch limit.
    """
    client = genai_client or genai.Client()

    def embed_fn(texts: List[str]) -> List[List[float]]:
        embeddings: List[List[float]] = []
        for i in range(0, len(texts), batch_size):
            chunk = texts[i : i + batch_size]
            response = client.models.embed_content(
                model=model, contents=cast(genai_types.ContentListUnion, chunk)
            )
            for embedding in response.embeddings or []:
                if embedding.values is None:
                    raise PerseusException(
                        f"Gemini returned no embedding values for model {model!r}."
                    )
                embeddings.append(embedding.values)
        return embeddings

    return embed_fn
