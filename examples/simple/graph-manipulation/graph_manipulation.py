
import logging
import sys
import os
from perseus_client.client import PerseusClient
from perseus_client.models import Entity, Relation

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(file_path: str):
    """
    Main function to build a graph, manipulate it, and serialize the results.
    """
    # Define output directory
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    try:
        with PerseusClient() as client:
            # 1. Build the initial KnowledgeGraph from a file using the SDK
            logger.info(f"Building initial graph from file: {file_path}")
            knowledge_graphs = client.build_graph(file_path=[file_path])
            if not knowledge_graphs:
                logger.error("Failed to build graph from the file.")
                return

            kg = knowledge_graphs[0]
            logger.info(f"Initial graph built with {len(kg.entities)} entities and {len(kg.relations)} relations.")

            # 2. Save the original, unmodified graph content from the API
            logger.info("--- Original Graph Serialization ---")
            if kg.ttl_content:
                original_ttl_path = os.path.join(output_dir, "output_graph_original.ttl")
                with open(original_ttl_path, "w", encoding="utf-8") as f:
                    f.write(kg.ttl_content)
                logger.info(f"Original TTL content from API saved to {original_ttl_path}")
            else:
                logger.warning("No original TTL content was returned from the API.")

            if kg.cql_content:
                original_cql_path = os.path.join(output_dir, "output_graph_original.cql")
                with open(original_cql_path, "w", encoding="utf-8") as f:
                    f.write(kg.cql_content)
                logger.info(f"Original CQL content from API saved to {original_cql_path}")
            else:
                logger.warning("No original CQL content was returned from the API.")

            # 3. Manipulate the KnowledgeGraph object
            logger.info("--- Manipulating the Graph ---")
            
            # Add a new entity
            new_entity = Entity(id="http://example.com/award/1", label="Award", properties={"name": "Nobel Prize in Physics"})
            kg.entities.append(new_entity)
            logger.info(f"Added new entity: {new_entity.properties['name']}")

            # Find Marie Curie's entity to connect the new relation
            marie_curie_entity = next((e for e in kg.entities if "Marie Curie" in e.properties.get("name", "")), None)

            if marie_curie_entity:
                # Add a new relation
                new_relation = Relation(source=marie_curie_entity.id, target=new_entity.id, type="WON_AWARD", properties={"year": 1903})
                kg.relations.append(new_relation)
                logger.info("Added new relation: Marie Curie WON_AWARD Nobel Prize in Physics")
            else:
                logger.warning("Could not find Marie Curie's entity to add the new relation.")
            
            logger.info(f"Graph now has {len(kg.entities)} entities and {len(kg.relations)} relations.")

            # 4. Serialize the modified graph to TTL and CQL using the new methods
            logger.info("--- Modified Graph Serialization ---")
            kg.save_ttl(os.path.join(output_dir, "output_graph_modified.ttl"))
            logger.info(f"Modified TTL content saved to {os.path.join(output_dir, 'output_graph_modified.ttl')}")

            kg.save_cql(os.path.join(output_dir, "output_graph_modified.cql"))
            logger.info(f"Modified CQL content saved to {os.path.join(output_dir, 'output_graph_modified.cql')}")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Ensure the script is run from the correct directory for asset path to be correct
    if not os.path.exists("assets/sample.txt"):
        print("Error: Please run this script from the 'examples/simple/graph-manipulation' directory.")
        sys.exit(1)
        
    main("assets/sample.txt")
