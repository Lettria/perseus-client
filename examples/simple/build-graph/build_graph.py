import asyncio
import logging
import sys
import os
from typing import Dict, Any, List

from perseus_client import PerseusClient
import perseus_client
from perseus_client.models import KnowledgeGraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main(file_path: str):
    """
    Main function to build a graph with metadata and save it to Neo4j.
    """
    async with PerseusClient() as client:
        try:
            # Define the custom metadata as a dictionary
            custom_metadata: Dict[str, Any] = {
                "source_file": os.path.basename(file_path),
                "processing_date": "2026-03-10",
                "version": "1.0",
                "custom_tag": "example-metadata",
            }

            logger.info("Building graph with metadata...")
            knowledge_graphs = await client.build_graph_async(
                file_paths=[file_path],
                metadata=custom_metadata,
                refresh_graph=True,
                ontology_path="./assets/ontology.ttl",
            )

            if knowledge_graphs:
                kg = knowledge_graphs[0]
                logger.info("Graph built successfully. Saving to Neo4j...")
                await kg.save_to_neo4j_async()
                kg.save_ttl("./output/graph.ttl")
                kg.save_cql("./output/graph.cql")
                logger.info(
                    "Graph with metadata saved to Neo4j. "
                    f"You can now inspect the nodes and relationships in your Neo4j browser at http://localhost:7474/browser/ (credentials available in your .env file)."
                )
        except Exception as e:
            logger.error(f"An error occurred while building the graph: {e}")
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

    asyncio.run(main(script_input))
