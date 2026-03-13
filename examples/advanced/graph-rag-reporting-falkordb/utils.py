import logging
import os
import time
import httpx

from perseus_client.exceptions import PerseusException


logging.basicConfig(level=logging.INFO)


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
