import asyncio
import logging
import sys
import os
from typing import Dict, Any

from perseus_client import PerseusClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main(file_path: str):
    """
    Main function to build a graph with metadata and save it to Spanner Graph.
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
                refresh_graph=False,
                ontology_path="./assets/ontology.ttl",
            )

            if knowledge_graphs:
                kg = knowledge_graphs[0]
                logger.info("Graph built successfully. Saving to Spanner Graph...")
                await kg.save_to_spanner_graph_async()
                kg.save_spgl("./output/graph.spgl")
                logger.info("Graph with metadata saved to Spanner Graph.")
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
