"""
Simplest usage: the module-level `perseus_client.build_graph` function.

No client to create or close: the module uses one shared client, keeps its
connection open across calls and closes it automatically when the program exits.
"""

import logging
import os
import sys

import perseus_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(file_path: str):
    logger.info(f"Building graph for {file_path}...")
    knowledge_graphs = perseus_client.build_graph(
        file_paths=[file_path],
        ontology_path="./assets/ontology.ttl",
    )

    for kg in knowledge_graphs:
        logger.info(
            f"Graph built with {len(kg.entities)} entities and {len(kg.relations)} relations."
        )
        kg.save_ttl("./output/module.ttl")


if __name__ == "__main__":
    script_input = sys.argv[1] if len(sys.argv) > 1 else "assets/sample.txt"

    if not os.path.exists(script_input):
        print(f"Error: The file '{script_input}' does not exist.")
        sys.exit(1)

    main(script_input)
