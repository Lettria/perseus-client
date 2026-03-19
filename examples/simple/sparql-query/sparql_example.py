from typing import cast, Iterable, Tuple
from rdflib.term import Node
import perseus_client


def main():
    """
    This example demonstrates how to build a graph, convert it to an rdflib graph,
    and then run a SPARQL query on it.
    """
    # Build a graph from a sample text file using the provided ontology
    graphs = perseus_client.build_graph(
        file_paths=["./assets/sample.txt"],
        ontology_path="./assets/ontology.ttl",
    )
    if not graphs:
        print("Could not build graph.")
        return

    # Get the first graph
    knowledge_graph = graphs[0]
    print(
        f"Graph built with {len(knowledge_graph.entities)} entities and "
        f"{len(knowledge_graph.relations)} relations."
    )

    # write ttl file
    # Create output directory if it doesn't exist
    import os

    os.makedirs("./output", exist_ok=True)

    ttl_file_path = "./output/sample.graph.ttl"
    with open(ttl_file_path, "w") as ttl_file:
        ttl_file.write(knowledge_graph.to_ttl())
    print(f"Graph written to {ttl_file_path}")

    # Convert to an rdflib graph
    rdflib_graph = knowledge_graph.to_rdflib()

    # Define a SPARQL query to find all persons and their names
    query = """
    PREFIX ont: <http://example.org/ontology#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT ?person ?name
    WHERE {
        ?person a ont:Person .
        ?person rdfs:label ?name .
    }
    """

    # Execute the query
    results = rdflib_graph.query(query)

    # Provide a specific type hint to the static analyzer to resolve warnings
    typed_results = cast(Iterable[Tuple[Node, Node]], results)

    # Print the results
    print("\n--- SPARQL Query Results ---")
    for person, name in typed_results:
        print(f"Person: {person}, Name: {name}")
    print("--------------------------")


if __name__ == "__main__":
    main()
