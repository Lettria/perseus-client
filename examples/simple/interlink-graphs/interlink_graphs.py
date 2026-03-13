import sys
import os
from perseus_client.client import PerseusClient
from perseus_client.models import KnowledgeGraph
from typing import List
import logging

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
            knowledge_graphs = client.build_graph(
                file_path=file_paths,
                ontology_path=ontology_path,
                metadata={"source": "interlink_graphs_example"},
                # refresh_graph=True,
            )

            if not knowledge_graphs or len(knowledge_graphs) < 2:
                logger.error("Failed to build at least two graphs for interlinking.")
                return

            # 2. Interlink the knowledge graphs
            merged_kg = KnowledgeGraph.interlink(
                kbs=knowledge_graphs,
                # immutable_properties=["hasJobTitle"]
            )

            # 3. Save the single, merged graph to Neo4j
            merged_kg.save_to_neo4j(strip_prefixes=True)

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
