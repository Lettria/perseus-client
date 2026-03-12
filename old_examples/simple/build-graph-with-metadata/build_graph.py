import logging
import sys
import os
from typing import Dict, Any, Optional
from perseus_client.client import PerseusClient
from perseus_client.models import Job
from utils import wait_for_neo4j

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(file_path: str):
    try:
        wait_for_neo4j()

        # Define the custom metadata as a dictionary
        custom_metadata: Dict[str, Any] = {
            "source_file": os.path.basename(file_path),
            "processing_date": "2026-03-10",  # Example fixed date
            "version": "1.0",
            "custom_tag": "example-metadata",
        }

        with PerseusClient() as client:
            logger.info("Building graph with metadata and saving to Neo4j...")
            job: Job = client.build_graph(
                file_path=file_path,
                output_path="./output/graph",  # Output will still be downloaded locally
                save_to_neo4j=True,  # Now using the SDK's feature to save to Neo4j
                save_to_falkordb=False,  # Set to True if you also want to save to FalkorDB
                metadata=custom_metadata,  # Pass the metadata dictionary directly
            )
            logger.info(
                f"Graph building job {job.id} completed with status: {job.status}"
            )
            logger.info(
                "Metadata should now be visible on nodes and relationships in Neo4j."
            )

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    script_input = sys.argv[1] if len(sys.argv) > 1 else None
    if not script_input:
        print("Please provide a file path as an argument.")
        sys.exit(1)
    main(script_input)
