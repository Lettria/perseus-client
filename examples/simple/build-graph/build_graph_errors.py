"""
Handling failures when building graphs for several files.

The batch below deliberately includes a file that doesn't exist, to show the two
behaviours of `build_graph`:

1. Default (`return_exceptions=False`): the first failure cancels the files still
   being processed and is raised. Catch it with `try` / `except`.
2. `return_exceptions=True`: every file is processed. The result list holds a
   `KnowledgeGraph` or the exception for each file, in input order, so the graphs
   that succeeded are kept.

Retrying failed files is cheap: files that already succeeded reuse their completed
jobs instead of running new ones.
"""

import logging
import os
from typing import List

from perseus_client import PerseusClient, PerseusException
from perseus_client.models import KnowledgeGraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fail_fast(client: PerseusClient, file_paths: List[str]):
    logger.info("--- 1. Default behaviour: stop at the first failure ---")
    try:
        client.build_graph(file_paths=file_paths)
    except PerseusException as e:
        logger.error(f"The batch failed, no graphs were returned: {e}")


def keep_partial_results(client: PerseusClient, file_paths: List[str]):
    logger.info("--- 2. return_exceptions=True: keep the graphs that succeeded ---")
    results = client.build_graph(file_paths=file_paths, return_exceptions=True)

    failed: List[str] = []
    for path, result in zip(file_paths, results):
        if isinstance(result, KnowledgeGraph):
            name = os.path.splitext(os.path.basename(path))[0]
            logger.info(f"OK     {path}: {len(result.entities)} entities")
            result.save_ttl(f"./output/errors_{name}.ttl")
        else:
            logger.error(f"FAILED {path}: {result}")
            failed.append(path)

    if failed:
        logger.info(f"{len(failed)} file(s) to fix and retry: {failed}")


if __name__ == "__main__":
    batch = [
        "assets/sample.txt",
        "assets/does_not_exist.txt",
        "assets/sample2.txt",
    ]

    with PerseusClient() as client:
        fail_fast(client, batch)
        keep_partial_results(client, batch)
