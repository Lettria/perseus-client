import logging
import sys
from perseus_client.client import PerseusClient
from perseus_client.models import Job
from simple_graph_retriever.client import GraphRetrievalClient
from utils import wait_for_embedder, wait_for_neo4j

logging.basicConfig(level=logging.INFO)


def main(file_path: str):
    with PerseusClient() as client:
        try:
            wait_for_neo4j()
            job: Job = client.build_graph(
                file_path=file_path,
                output_path="./output/graph",
                save_to_neo4j=True,
            )
            wait_for_embedder()
            GraphRetrievalClient().index()

        except Exception as e:
            logging.error(e)


if __name__ == "__main__":
    script_input = sys.argv[1] if len(sys.argv) > 1 else None
    if not script_input:
        print("Please provide a file path as an argument.")
        sys.exit(1)
    main(script_input)
