import logging
import os
import time
import httpx
from falkordb import FalkorDB
from perseus_client.config import settings
from perseus_client.exceptions import PerseusException

logging.basicConfig(level=logging.INFO)


def wait_for_falkordb(timeout: int = 600):
    print("settings.falkordb_host:", getattr(settings, "falkordb_host", None))
    print("settings.falkordb_port:", getattr(settings, "falkordb_port", None))
    print("settings.falkordb_username:", getattr(settings, "falkordb_username", None))
    print("settings.falkordb_password:", getattr(settings, "falkordb_password", None))
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            driver = FalkorDB(
                host=settings.falkordb_host,
                port=settings.falkordb_port,
                password=getattr(settings, "falkordb_password", None),
                # decode_responses=True 
            )
            driver.connection.ping()
            logging.info("FalkorDB is ready.")
            return
        except Exception as e:
            logging.warning(f"Waiting for FalkorDB... Error: {e}")
            pass
        logging.info("Waiting for FalkorDB to be ready...")
        time.sleep(5)
    raise PerseusException("Timed out waiting for FalkorDB to be ready.")


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
