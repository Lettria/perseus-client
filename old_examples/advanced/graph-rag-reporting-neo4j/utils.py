import logging
import os
import time
import httpx
from neo4j import GraphDatabase
from perseus_client.config import settings
from perseus_client.exceptions import PerseusException

logging.basicConfig(level=logging.INFO)


def wait_for_neo4j(timeout: int = 600):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            driver = GraphDatabase.driver(
                settings.neo4j_uri,
                auth=(settings.neo4j_user, settings.neo4j_password),
            )
            driver.verify_connectivity()
            driver.close()
            logging.info("Neo4j is ready.")
            return
        except Exception:
            pass
        logging.info("Waiting for Neo4j to be ready...")
        time.sleep(5)
    raise PerseusException("Timed out waiting for Neo4j to be ready.")


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
