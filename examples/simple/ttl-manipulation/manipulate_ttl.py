import sys
import os
import logging
from perseus_client.client import PerseusClient
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import Namespace, RDF

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def add_status_to_persons(ttl_content: str, status_value: str) -> str:
    """
    Parses TTL content and adds a status property to all entities of type Person.

    Args:
        ttl_content: The Turtle file content as a string.
        status_value: The value for the new status property.

    Returns:
        The modified Turtle content as a string.
    """
    g = Graph()
    g.parse(data=ttl_content, format="turtle")

    ex = Namespace("http://example.org/ontology#")
    person_subjects = list(g.subjects(predicate=RDF.type, object=ex.Person))
    status_predicate = ex.status

    logger.info(f"Found {len(person_subjects)} 'Person' entities. Adding status='{status_value}'...")
    for person in person_subjects:
        g.add((person, status_predicate, Literal(status_value)))

    return g.serialize(format="turtle")


def main(text_file_path: str):
    """
    Runs the full workflow:
    1. Generates a graph from a text file.
    2. Manipulates the TTL to add a new property to all Persons.
    3. Saves the modified TTL to a new file.
    """
    os.makedirs("output", exist_ok=True)

    try:
        with PerseusClient() as client:
            # Step 1: Generate the graph
            logger.info(f"Generating graph from '{text_file_path}'...")
            kgl = client.build_graph(
                file_path=[text_file_path],
                ontology_path="./assets/ontology.ttl",
            )

            if not kgl or not kgl[0].ttl_content:
                logger.error("Failed to generate graph or get TTL content.")
                sys.exit(1)
            
            kg = kgl[0]
            original_ttl = kg.ttl_content
            
            with open("output/graph.ttl", "w", encoding="utf-8") as f:
                f.write(original_ttl)
            logger.info("Original TTL content saved to 'output/graph.ttl'")

            # Step 2: Read and manipulate the TTL
            modified_ttl = add_status_to_persons(original_ttl, "verified")

            # Step 3: Save the modified TTL to a new file
            with open("output/graph_modified.ttl", "w", encoding="utf-8") as f:
                f.write(modified_ttl)
            logger.info("Modified TTL with 'status' property saved to 'output/graph_modified.ttl'")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    input_path = "assets/sample.txt" if len(sys.argv) < 2 else sys.argv[1]
    if not os.path.exists(input_path):
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
    main(input_path)
