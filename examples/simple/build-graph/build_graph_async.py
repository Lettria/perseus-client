"""
Asynchronous usage: `async with PerseusClient() as client:` and `build_graph_async`.

Use this from async code (web servers, async pipelines). The client opens its
connection in the running event loop and closes it when the block ends.
"""

import asyncio
import logging
import os
import sys
from typing import List

from perseus_client import PerseusClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main(file_paths: List[str]):
    async with PerseusClient() as client:
        logger.info(f"Building graphs for {len(file_paths)} file(s)...")
        knowledge_graphs = await client.build_graph_async(
            file_paths=file_paths,
            ontology_path="./assets/ontology.ttl",
        )

        for path, kg in zip(file_paths, knowledge_graphs):
            name = os.path.splitext(os.path.basename(path))[0]
            logger.info(
                f"{name}: {len(kg.entities)} entities, {len(kg.relations)} relations."
            )
            kg.save_ttl(f"./output/async_{name}.ttl")


if __name__ == "__main__":
    script_inputs = sys.argv[1:] or ["assets/sample.txt", "assets/sample2.txt"]

    missing = [p for p in script_inputs if not os.path.exists(p)]
    if missing:
        print(f"Error: These files do not exist: {', '.join(missing)}")
        sys.exit(1)

    asyncio.run(main(script_inputs))
