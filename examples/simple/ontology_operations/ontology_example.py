import os
from perseus_client.client import PerseusClient


def main():
    ontology_path = "assets/example_ontology.ttl"

    with PerseusClient() as client:
        print(f"Uploading ontology: {ontology_path}")
        uploaded_ontology = client.ontology.upload_ontology(ontology_path)
        print(
            f"Uploaded Ontology ID: {uploaded_ontology.id}, Name: {uploaded_ontology.name}, Status: {uploaded_ontology.status}"
        )

        client.ontology.wait_for_ontology_upload(uploaded_ontology.id)

        print(f"Finding ontology with ID: {uploaded_ontology.id}")
        found_ontologies = client.ontology.find_ontologies(
            ids=[uploaded_ontology.id]
        )
        if found_ontologies:
            print(
                f"Found Ontology ID: {found_ontologies[0].id}, Name: {found_ontologies[0].name}, Status: {found_ontologies[0].status}"
            )
        else:
            print("Ontology not found.")


if __name__ == "__main__":
    main()
