import sys
import os
import logging
from perseus_client.client import PerseusClient
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import Namespace, RDF
from utils import read_ttl_file

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

    # Define the namespaces and properties we'll use
    # This should match the class defined in your ontology
    ex = Namespace("http://example.org/ontology#")
    
    # Find all subjects that are of type ex:Person
    person_subjects = g.subjects(predicate=RDF.type, object=ex.Person)
    
    # Define the new property to add
    status_predicate = ex.status

    logger.info(f"Adding status='{status_value}' to all Person entities...")
    for person in person_subjects:
        g.add((person, status_predicate, Literal(status_value)))
    
    return g.serialize(format="turtle")


def main(text_file_path: str):
    """
    Runs the full workflow:
    1. Generates a graph from a text file, creating a TTL file.
    2. Reads and manipulates the TTL to add a new property to all Persons.
    3. Saves the modified TTL to a new file.
    """
    ttl_output_path = "./output/graph.ttl"
    modified_ttl_output_path = "./output/graph_modified.ttl"
    
    try:
        with PerseusClient() as client:
            # Step 1: Generate the graph using an ontology
            logger.info(f"Generating graph from '{text_file_path}'...")
            client.build_graph(
                file_path=text_file_path,
                ontology_path="./assets/ontology.ttl",
                output_path=os.path.splitext(ttl_output_path)[0] # Pass path without extension
            )
            logger.info(f"TTL file saved to '{ttl_output_path}'")

            # Step 2: Read and manipulate the TTL file
            original_ttl = read_ttl_file(ttl_output_path)
            modified_ttl = add_status_to_persons(original_ttl, "verified")
            
            # Step 3: Save the modified TTL to a new file
            with open(modified_ttl_output_path, "w", encoding="utf-8") as f:
                f.write(modified_ttl)
            logger.info(f"Modified TTL with 'status' property saved to '{modified_ttl_output_path}'")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        sys.exit(1)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python manipulate_ttl.py <path_to_text_file>")
        sys.exit(1)
    main(sys.argv[1])
