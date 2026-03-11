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
    1. Generates a graph from a text file and saves the CQL locally.
    2. Reads and manipulates the generated CQL to rename a label.
    3. Executes the modified CQL against a Neo4j database.
    """
    cql_output_path = "./output/graph.cql"

    try:
        # Step 0: Wait for Neo4j to be ready
        wait_for_neo4j()

        with PerseusClient() as client:
            # Step 1: Generate the graph and save the CQL file locally
            logger.info(f"Generating graph from '{text_file_path}'...")
            client.build_graph(
                file_path=text_file_path,
                ontology_path="./assets/ontology.ttl",  # Use the custom ontology
                output_path=os.path.splitext(cql_output_path)[
                    0
                ],  # Pass path without extension
                save_to_neo4j=False,  # We want to intercept and modify the CQL first
            )
            logger.info(f"CQL file saved to '{cql_output_path}'")

            # Step 2: Read and perform custom manipulation on the CQL
            original_cql = read_cql_file(cql_output_path)
            modified_cql = rename_label_in_cql(original_cql, "Person", "Individual")

            # For demonstration, you could save the modified cql too
            modified_cql_path = "./output/graph_modified.cql"
            with open(modified_cql_path, "w", encoding="utf-8") as f:
                f.write(modified_cql)
            logger.info(f"Modified CQL saved to '{modified_cql_path}'")

            # Step 3: Execute the modified CQL against Neo4j
            logger.info("Executing modified CQL against Neo4j...")
            client.neo4j.execute_cql_string(modified_cql)
            logger.info("Successfully executed modified CQL.")
            logger.info("You can now query Neo4j for the 'Individual' label.")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python manipulate_cql.py <path_to_text_file>")
        sys.exit(1)
    main(sys.argv[1])
