import logging
import sys
import os
from perseus_client.client import PerseusClient
from perseus_client.models import KnowledgeGraph
from typing import List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(
    file_paths: List[str],
    ontology_path: str,
):
    """
    Main function to build multiple graphs from files with an ontology and save them to Neo4j.
    """
    try:
        with PerseusClient() as client:
            logger.info(
                f"Building graphs from files: {file_paths} with ontology: {ontology_path}"
            )
            knowledge_graphs = client.build_graph(
                file_path=file_paths,
                ontology_path=ontology_path,
                metadata={"source": "multi_file_graph_example"},
            )

            if not knowledge_graphs:
                logger.error("Failed to build any graphs from the provided files.")
                return

            logger.info(f"Successfully built {len(knowledge_graphs)} knowledge graphs.")

            for i, kg in enumerate(knowledge_graphs):
                prefix = f"graph_{i + 1}"
                logger.info(f"--- Processing {prefix} (Graph {i+1}) ---")
                logger.info(
                    f"{prefix} built with {len(kg.entities)} entities and {len(kg.relations)} relations."
                )

                # Save the modified graph to Neo4j
                logger.info(f"Saving {prefix} (modified graph) to Neo4j...")
                kg.save_to_neo4j()
                logger.info(f"{prefix} saved to Neo4j.")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    person1_path = os.path.join(base_dir, "assets", "person1.txt")
    person2_path = os.path.join(base_dir, "assets", "person2.txt")
    ontology_path = os.path.join(base_dir, "assets", "ontology.ttl")
    # output_dir is no longer needed for file saving

    # Ensure asset files exist
    if not os.path.exists(person1_path):
        print(f"Error: Asset file not found: {person1_path}")
        sys.exit(1)
    if not os.path.exists(person2_path):
        print(f"Error: Asset file not found: {person2_path}")
        sys.exit(1)
    if not os.path.exists(ontology_path):
        print(f"Error: Ontology file not found: {ontology_path}")
        sys.exit(1)

    main(
        file_paths=[person1_path, person2_path],
        ontology_path=ontology_path,
    )
