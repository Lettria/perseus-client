import logging
import sys
import os
from perseus_client.client import PerseusClient
from simple_graph_retriever.client import GraphRetrievalClient
from utils import wait_for_embedder, wait_for_falkordb

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main(file_path: str):
    """
    Builds a knowledge graph, saves it to FalkorDB, and indexes it for retrieval.
    """
    os.makedirs("output", exist_ok=True)
    file_stem = os.path.splitext(os.path.basename(file_path))[0]
    output_ttl_path = os.path.join("output", f"{file_stem}.ttl")

    try:
        logging.info("Waiting for services to become available...")
        wait_for_falkordb()
        wait_for_embedder()
        logging.info("Services are ready.")

        with PerseusClient() as client:
            logging.info(f"Building knowledge graph from: {file_path}")
            
            # The build_graph method returns a list of KnowledgeGraph objects
            knowledge_graphs = client.build_graph(file_path=[file_path])

            if not knowledge_graphs:
                logging.error("Graph building process did not return any knowledge graphs.")
                sys.exit(1)

            kg = knowledge_graphs[0]
            logging.info("Graph building complete.")

            # Save the TTL content locally for inspection
            if kg.ttl_content:
                with open(output_ttl_path, "w", encoding="utf-8") as f:
                    f.write(kg.ttl_content)
                logging.info(f"TTL output saved to {output_ttl_path}")

            # Load the graph into FalkorDB
            logging.info("Loading graph into FalkorDB...")
            kg.save_to_falkordb()
            logging.info("Graph successfully loaded into FalkorDB.")

            # Index the graph for retrieval
            logging.info("Indexing graph for retrieval...")
            GraphRetrievalClient().index()
            logging.info("Graph indexing complete.")

    except Exception as e:
        logging.error(f"An unexpected error occurred during the indexing process: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error("Please provide a file path as an argument.")
        logging.info("Example usage: python index.py assets/LOREAL_Rapport_Annuel_2024.md")
        sys.exit(1)
        
    script_input = sys.argv[1]
    
    if not os.path.exists(script_input):
        logging.error(f"File not found: {script_input}")
        sys.exit(1)

    main(script_input)
