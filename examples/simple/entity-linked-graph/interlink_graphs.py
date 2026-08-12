import sys
import os
import perseus_client
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
        knowledge_graphs = perseus_client.build_graph(
            file_paths=file_paths,
            ontology_path=ontology_path,
            metadata={"source": "interlink_graphs_example"},
        )
        # 2. Interlink the knowledge graphs
        merged_kg = perseus_client.interlink(
            kbs=knowledge_graphs,
            merge_properties_on_conflict=True,
            # immutable_properties=["hasJobTitle"]
        )
        merged_kg.save_ttl("./output/merged_graph.ttl")
        merged_kg.save_cql("./output/merged_graph.cql")

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
