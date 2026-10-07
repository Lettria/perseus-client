"""
Synchronous usage with an explicit client: `with PerseusClient() as client:`.

Inside the `with` block, every call reuses the same connection. The connection is
closed when the block ends, even if an exception is raised.

Calling `PerseusClient().build_graph(...)` without `with` also works: the client
opens a connection for that call and closes it when the call returns.
"""

import logging
import os
import sys

from perseus_client import PerseusClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(file_path: str):
    with PerseusClient() as client:
        logger.info(f"Building graph for {file_path}...")
        knowledge_graphs = client.build_graph(
            file_paths=[file_path],
            ontology_path="./assets/ontology.ttl",
            metadata={"source_file": os.path.basename(file_path)},
        )
        kg = knowledge_graphs[0]
        logger.info(
            f"Graph built with {len(kg.entities)} entities and {len(kg.relations)} relations."
        )

        # A second call in the same block reuses the open connection.
        # The graph was already built, so the completed job is reused.
        logger.info("Building the same graph again (reuses the completed job)...")
        client.build_graph(
            file_paths=[file_path],
            ontology_path="./assets/ontology.ttl",
        )

        kg.save_ttl("./output/with_client.ttl")
        kg.save_cql("./output/with_client.cql")


if __name__ == "__main__":
    script_input = sys.argv[1] if len(sys.argv) > 1 else "assets/sample.txt"

    if not os.path.exists(script_input):
        print(f"Error: The file '{script_input}' does not exist.")
        sys.exit(1)

    main(script_input)
