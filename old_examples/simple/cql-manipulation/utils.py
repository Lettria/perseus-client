import logging
import time
import os
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


def read_cql_file(file_path: str) -> str:
    """Reads the content of a CQL file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()
