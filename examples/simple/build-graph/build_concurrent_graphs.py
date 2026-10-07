"""
Building graphs for many files with a concurrency limit.

`max_concurrency` sets how many files are processed at the same time (upload,
job and download). The default is 10. Lower it to reduce memory use and load on
the API, or raise it for many small files. Results are returned in the same
order as `file_paths`.

Usage:
    python build_concurrent_graphs.py [--max-concurrency N] [file_or_directory ...]
"""

import argparse
import glob
import logging
import os
import sys
import time
from typing import List

from perseus_client import PerseusClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def collect_files(inputs: List[str]) -> List[str]:
    """Expands directories into the .txt files they contain."""
    paths: List[str] = []
    for item in inputs:
        if os.path.isdir(item):
            paths.extend(sorted(glob.glob(os.path.join(item, "*.txt"))))
        else:
            paths.append(item)
    return paths


def main(file_paths: List[str], max_concurrency: int):
    logger.info(
        f"Building graphs for {len(file_paths)} file(s), {max_concurrency} at a time..."
    )
    start = time.monotonic()

    with PerseusClient() as client:
        knowledge_graphs = client.build_graph(
            file_paths=file_paths,
            ontology_path="./assets/ontology.ttl",
            max_concurrency=max_concurrency,
        )

    logger.info(f"Done in {time.monotonic() - start:.1f}s.")
    for path, kg in zip(file_paths, knowledge_graphs):
        name = os.path.splitext(os.path.basename(path))[0]
        logger.info(
            f"{name}: {len(kg.entities)} entities, {len(kg.relations)} relations."
        )
        kg.save_ttl(f"./output/concurrent_{name}.ttl")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "inputs",
        nargs="*",
        default=["assets"],
        help="Files or directories of .txt files (default: assets/).",
    )
    parser.add_argument("--max-concurrency", type=int, default=2)
    args = parser.parse_args()

    script_inputs = collect_files(args.inputs)
    missing = [p for p in script_inputs if not os.path.exists(p)]
    if not script_inputs or missing:
        print(f"Error: No input files, or these files do not exist: {', '.join(missing)}")
        sys.exit(1)

    main(script_inputs, args.max_concurrency)
