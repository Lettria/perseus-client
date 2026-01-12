import os
import asyncio
import logging
from perseus_client.client import PerseusClient
from perseus_client.exceptions import PerseusException
from perseus_client.models import Job


async def main():
    async with PerseusClient() as client:
        try:
            job: Job = await client.build_graph(
                file_path="assets/pizza.txt",
                ontology_path="assets/pizza.ttl",
                output_path="./output/graph",
                save_to_neo4j=False,
                refresh_graph=False,
            )
            print("Job completed successfully:", job)
        except Exception as e:
            logging.error(e)


if __name__ == "__main__":
    asyncio.run(main())
