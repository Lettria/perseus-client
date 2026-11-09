import logging
import os
import time
from typing import Callable, List, Optional

import httpx
from neo4j import GraphDatabase
from perseus_client.config import settings
from perseus_client.exceptions import PerseusException

logging.basicConfig(level=logging.INFO)


def make_tei_embed_fn(
    embedder_url: Optional[str] = None, batch_size: int = 32, max_chars: int = 500
) -> Callable[[List[str]], List[List[float]]]:
    """
    simple_graph_retriever no longer talks to any embedder itself; this
    wires its required embed_fn callback to the local TEI service that
    this example's docker-compose.yaml already runs. Swap this out for
    Azure OpenAI, OpenAI, etc. in your own code.

    TEI rejects a request that's too large (HTTP 413) either because it
    has too many inputs at once (batch_size caps this at TEI's default
    max-client-batch-size, 32) or because the inputs themselves are too
    long (max_chars truncates each one, matching the SDK's old built-in
    behavior before embedding became caller-supplied).
    """
    url = embedder_url or os.getenv("EMBEDDER_URL", "http://localhost:8080")

    def embed_fn(texts: List[str]) -> List[List[float]]:
        truncated = [t[:max_chars] for t in texts]
        embeddings = []
        with httpx.Client() as client:
            for i in range(0, len(truncated), batch_size):
                chunk = truncated[i : i + batch_size]
                resp = client.post(url, json={"inputs": chunk}, timeout=30)
                resp.raise_for_status()
                embeddings.extend(resp.json())
        return embeddings

    return embed_fn


def wait_for_embedder(timeout: int = 600):
    start_time = time.time()
    embedder_url = os.getenv("EMBEDDER_URL", "http://localhost:8080")
    health_url = f"{embedder_url}/health"
    while time.time() - start_time < timeout:
        try:
            with httpx.Client() as client:
                response = client.get(health_url)
                if response.status_code == 200:
                    logging.info("Embedder is ready.")
                    return
        except httpx.RequestError:
            pass
        logging.info("Waiting for embedder to be ready...")
        time.sleep(5)
    raise PerseusException("Timed out waiting for embedder to be ready.")
