import logging
import sys
import os
from typing import Dict, Any
from perseus_client.client import PerseusClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(file_path: str):
    """
    Main function to build a graph with metadata and save it to Neo4j.
    """
    try:
        # Define the custom metadata as a dictionary
        custom_metadata: Dict[str, Any] = {
            "source_file": os.path.basename(file_path),
            "processing_date": "2026-03-10",
            "version": "1.0",
            "custom_tag": "example-metadata",
        }

        with PerseusClient() as client:
            logger.info("Building graph with metadata...")
            # The build_graph method now returns a list of KnowledgeGraph objects
            knowledge_graphs = client.build_graph(
                file_path=[file_path],
                metadata=custom_metadata,
            )

            if knowledge_graphs:
                kg = knowledge_graphs[0]
                logger.info("Graph built successfully. Saving to Neo4j...")
                # The KnowledgeGraph object has a method to save to Neo4j
                kg.save_to_neo4j()
                logger.info(
                    "Graph with metadata saved to Neo4j. "
                    "You can now inspect the nodes and relationships in your Neo4j browser."
                )

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        script_input = sys.argv[1]
    else:
        # Default to the sample file if no argument is provided
        script_input = "assets/sample.txt"

    if not os.path.exists(script_input):
        print(f"Error: The file '{script_input}' does not exist.")
        sys.exit(1)

    main(script_input)
