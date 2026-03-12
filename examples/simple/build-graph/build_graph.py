import os
from perseus_client.client import PerseusClient
from perseus_client.models import KnowledgeGraph

# Create the output directory if it does not exist
os.makedirs("output", exist_ok=True)

with PerseusClient() as client:
    # The build_graph method returns a list of KnowledgeGraph objects
    kgl: list[KnowledgeGraph] = client.build_graph(
        file_path=["assets/pizza.txt"],
        ontology_path="assets/pizza.ttl",
    )

    # Save the output of the first knowledge graph to the output directory
    if kgl:
        kg = kgl[0]
        if kg.ttl_content:
            with open("output/graph.ttl", "w", encoding="utf-8") as f:
                f.write(kg.ttl_content)
        if kg.cql_content:
            with open("output/graph.cql", "w", encoding="utf-8") as f:
                f.write(kg.cql_content)
