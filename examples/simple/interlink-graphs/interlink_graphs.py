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
    Main function to build multiple graphs, interlink them into a single unified
    graph, and save the result to Neo4j.
    """
    try:
        with PerseusClient() as client:
            logger.info(
                f"Building graphs from files: {file_paths} with ontology: {ontology_path}"
            )
            knowledge_graphs = client.build_graph(
                file_path=file_paths,
                ontology_path=ontology_path,
                metadata={"source": "interlink_graphs_example"},
            )

            if not knowledge_graphs or len(knowledge_graphs) < 2:
                logger.error("Failed to build at least two graphs for interlinking.")
                return

            logger.info(
                f"Successfully built {len(knowledge_graphs)} individual knowledge graphs."
            )
            for i, kg in enumerate(knowledge_graphs):
                logger.info(
                    f"  - Graph {i+1}: {len(kg.entities)} entities, {len(kg.relations)} relations"
                )

            # 2. Interlink the knowledge graphs
            logger.info("--- Interlinking Knowledge Graphs ---")
            # The default interlinking key is rdfs:label, which is suitable for this example.
            merged_kg = KnowledgeGraph.interlink(kbs=knowledge_graphs)
            logger.info("Interlinking complete.")
            logger.info(
                f"  - Merged Graph: {len(merged_kg.entities)} entities, {len(merged_kg.relations)} relations"
            )

            # 3. Save the single, merged graph to Neo4j
            logger.info("--- Saving Merged Graph to Neo4j ---")
            merged_kg.save_to_neo4j(strip_prefixes=True)
            logger.info("Merged graph saved to Neo4j.")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    person1_path = os.path.join(base_dir, "assets", "person1.txt")
    person2_path = os.path.join(base_dir, "assets", "person2.txt")
    ontology_path = os.path.join(base_dir, "assets", "ontology.ttl")

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
