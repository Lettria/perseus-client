import logging
import sys
import os
from perseus_client.client import PerseusClient
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()


def main(file_path: str):
    """
    Builds a knowledge graph from a given document using the Perseus client.
    """
    ontology_path = "assets/ontology_csrd.ttl"
    file_stem = os.path.splitext(os.path.basename(file_path))[0]
    output_path = os.path.join("output", file_stem)

    with PerseusClient() as client:
        try:
            logging.info(f"Processing: {file_path}")
            job = client.build_graph(
                file_path=file_path,
                ontology_path=ontology_path,
                output_path=output_path,
                save_to_neo4j=True,
            )
            logging.info(f"Job ID: {job.id} - Status: {job.status.value}")
            logging.info(
                f"View in Perseus: https://app.perseus.lettria.net/app/jobs/{job.id}"
            )

        except Exception as e:
            logging.error(e)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.info("No file path provided. Using default: assets/annual_report.md")
        file_to_process = "assets/annual_report.md"
    else:
        file_to_process = sys.argv[1]

    if not os.path.exists(file_to_process):
        logging.error(f"File not found: {file_to_process}")
        sys.exit(1)

    main(file_to_process)
