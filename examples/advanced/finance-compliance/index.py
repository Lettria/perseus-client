import logging
import sys
import os
from perseus_client.client import PerseusClient
from dotenv import load_dotenv

# Configure logging and load environment variables
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

def main(file_path: str):
    """
    Builds a knowledge graph from a document, saves the TTL output, and loads it into Neo4j.
    """
    ontology_path = "assets/ontology_csrd.ttl"
    
    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)
    
    file_stem = os.path.splitext(os.path.basename(file_path))[0]
    output_ttl_path = os.path.join("output", f"{file_stem}.ttl")

    try:
        with PerseusClient() as client:
            logging.info(f"Processing file: {file_path} with ontology: {ontology_path}")
            
            # The build_graph method now returns a list of KnowledgeGraph objects
            knowledge_graphs = client.build_graph(
                file_path=[file_path],
                ontology_path=ontology_path
            )

            if not knowledge_graphs:
                logging.error("Graph building process did not return any knowledge graphs.")
                return

            kg = knowledge_graphs[0]
            logging.info("Graph building complete.")

            # Save the TTL content locally
            if kg.ttl_content:
                with open(output_ttl_path, "w", encoding="utf-8") as f:
                    f.write(kg.ttl_content)
                logging.info(f"TTL output saved to {output_ttl_path}")

            # Load the graph into Neo4j
            logging.info("Loading graph into Neo4j...")
            kg.save_to_neo4j()
            logging.info("Graph successfully loaded into Neo4j.")

    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.warning("No file path provided. Please specify a file to process.")
        logging.info("Example usage: python index.py assets/ecosteel_annual_report.md")
        sys.exit(1)
        
    file_to_process = sys.argv[1]

    if not os.path.exists(file_to_process):
        logging.error(f"File not found: {file_to_process}")
        sys.exit(1)

    main(file_to_process)
