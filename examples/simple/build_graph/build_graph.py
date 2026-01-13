from perseus_client.client import PerseusClient
from perseus_client.models import Job


with PerseusClient() as client:
    job: Job = client.build_graph(
        file_path="assets/pizza.txt",
        ontology_path="assets/pizza.ttl",
        output_path="./output/graph",
    )
