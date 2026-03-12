import sys
import os
import logging
from perseus_client.client import PerseusClient
from utils import wait_for_neo4j, read_cql_file

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def rename_label_in_cql(cql_content: str, old_label: str, new_label: str) -> str:
    """
    Performs a simple string replacement to change a label in a CQL string.

    NOTE: This is a basic demonstration. For complex CQL, a more robust
    parsing method would be required.
    """
    logger.info(
        f"Replacing all occurrences of label ':{old_label}' with ':{new_label}'"
    )
    return cql_content.replace(f":{old_label}", f":{new_label}")


def main(text_file_path: str):
    """
    Runs the full workflow:
    1. Generates a graph from a text file.
    2. Modifies the generated CQL to rename a label.
    3. Executes the modified CQL against a Neo4j database.
    """
    os.makedirs("output", exist_ok=True)
    
    try:
        # Step 0: Wait for Neo4j to be ready
        wait_for_neo4j()

        with PerseusClient() as client:
            # Step 1: Generate the graph from the text file
            logger.info(f"Generating graph from '{text_file_path}'...")
            kgl = client.build_graph(
                file_path=[text_file_path],
                ontology_path="./assets/ontology.ttl",
            )
            
            if not kgl or not kgl[0].cql_content:
                logger.error("Failed to generate graph or get CQL content.")
                sys.exit(1)
            
            kg = kgl[0]
            original_cql = kg.cql_content
            
            # Save the original CQL for inspection
            with open("output/graph.cql", "w", encoding="utf-8") as f:
                f.write(original_cql)
            logger.info("Original CQL saved to 'output/graph.cql'")

            # Step 2: Perform custom manipulation on the CQL
            modified_cql = rename_label_in_cql(original_cql, "Person", "Individual")

            # Save the modified cql for inspection
            with open("output/graph_modified.cql", "w", encoding="utf-8") as f:
                f.write(modified_cql)
            logger.info("Modified CQL saved to 'output/graph_modified.cql'")

            # Step 3: Execute the modified CQL against Neo4j
            logger.info("Executing modified CQL against Neo4j...")
            # Update the knowledge graph's CQL content
            kg.cql_content = modified_cql
            # Use the built-in method to save to Neo4j
            kg.save_to_neo4j()
            logger.info("Successfully executed modified CQL.")
            logger.info("You can now query Neo4j for the 'Individual' label.")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        input_path = sys.argv[1]
    else:
        input_path = "assets/sample.txt"
    
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
        
    main(input_path)
