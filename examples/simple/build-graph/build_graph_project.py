import asyncio
import logging
import sys
import os
from typing import Optional

from perseus_client import PerseusClient
from perseus_client.models import KnowledgeGraph

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main(file_path: str, project_id: Optional[str] = None):
    """
    Main function to build a graph and assign it to a project.

    This example demonstrates how to organize jobs by assigning them to projects.
    Projects help you group related jobs together for better organization.

    Args:
        file_path: Path to the input file
        project_id: The project ID to assign the job to. Pass None to unassign.
                   If not provided, project assignment remains unchanged.
    """
    async with PerseusClient() as client:
        try:
            logger.info(f"Building graph from: {file_path}")

            if project_id is None:
                logger.info("Unassigning job from any project (project_id=None)")
                knowledge_graphs = await client.build_graph_async(
                    file_paths=[file_path],
                    project_id=None,  # Explicitly unassign from any project
                )
            elif project_id:
                logger.info(f"Assigning job to project: {project_id}")
                knowledge_graphs = await client.build_graph_async(
                    file_paths=[file_path],
                    project_id=project_id,  # Assign to specific project
                )
            else:
                logger.info("Building graph without changing project assignment")
                knowledge_graphs = await client.build_graph_async(
                    file_paths=[file_path],
                    # project_id not provided - keeps existing assignment
                )

            if knowledge_graphs:
                kg = knowledge_graphs[0]
                logger.info(
                    f"✅ Graph built successfully with {len(kg.entities)} entities "
                    f"and {len(kg.relations)} relations."
                )

                # Save outputs
                kg.save_ttl("./output/graph.ttl")
                kg.save_cql("./output/graph.cql")
                logger.info("Graph saved to ./output/")

                if project_id:
                    logger.info(
                        f"💡 Job has been assigned to project: {project_id}\n"
                        f"   You can view it in the Perseus app project view."
                    )
                elif project_id is None:
                    logger.info("💡 Job has been unassigned from any project.")
                else:
                    logger.info("💡 Job's project assignment remains unchanged.")

        except Exception as e:
            logger.error(f"An error occurred while building the graph: {e}")
            sys.exit(1)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Build a knowledge graph and optionally assign it to a project."
    )
    parser.add_argument(
        "file",
        nargs="?",
        default="assets/sample.txt",
        help="Path to the input file (default: assets/sample.txt)",
    )
    parser.add_argument(
        "--project-id",
        type=str,
        help=(
            "Project ID to assign the job to. "
            "Find your project ID in the Perseus app URL: "
            "https://app.perseus.lettria.net/app/w/.../p/<PROJECT_ID>"
        ),
    )
    parser.add_argument(
        "--unassign",
        action="store_true",
        help="Explicitly unassign the job from any project (sets project_id=None)",
    )

    args = parser.parse_args()

    if not os.path.exists(args.file):
        print(f"Error: The file '{args.file}' does not exist.")
        sys.exit(1)

    # Determine project_id based on arguments
    if args.unassign:
        project_id = None  # Explicit None to unassign
    elif args.project_id:
        project_id = args.project_id  # Assign to specific project
    else:
        project_id = ""  # Empty string means don't provide the parameter

    asyncio.run(main(args.file, project_id if project_id != "" else None))
