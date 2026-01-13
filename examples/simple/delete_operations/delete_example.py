import os
from perseus_client.client import PerseusClient
from perseus_client.exceptions import APIException


def main():
    # Ensure LETTRIA_API_KEY environment variable is set
    # export LETTRIA_API_KEY="your_api_key_here"

    file_path_to_delete = "assets/file_to_delete.txt"
    ontology_path_to_delete = "assets/ontology_to_delete.ttl"

    with PerseusClient() as client:
        # Upload a file and then delete it
        try:
            print(f"Uploading file for deletion: {file_path_to_delete}")
            file_to_delete = client.file.upload_file(file_path_to_delete)
            print(f"Uploaded File ID: {file_to_delete.id}")

            client.file.delete_file(file_to_delete.id)

            # Verify deletion
            print(f"Verifying deletion of file ID: {file_to_delete.id}")
            found_files = client.file.find_files(ids=[file_to_delete.id])
            if not found_files:
                print(f"File {file_to_delete.id} successfully deleted and not found.")
            else:
                print(f"Verification failed: File {file_to_delete.id} still exists.")

        except APIException as e:
            print(f"An API error occurred during file deletion process: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

        print("-" * 20)

        # Upload an ontology and then delete it
        try:
            print(f"Uploading ontology for deletion: {ontology_path_to_delete}")
            ontology_to_delete = client.ontology.upload_ontology(
                ontology_path_to_delete
            )
            print(f"Uploaded Ontology ID: {ontology_to_delete.id}")

            client.ontology.delete_ontology(ontology_to_delete.id)

            # Verify deletion
            print(f"Verifying deletion of ontology ID: {ontology_to_delete.id}")
            found_ontologies = client.ontology.find_ontologies(
                ids=[ontology_to_delete.id]
            )
            if not found_ontologies:
                print(
                    f"Ontology {ontology_to_delete.id} successfully deleted and not found."
                )
            else:
                print(
                    f"Verification failed: Ontology {ontology_to_delete.id} still exists."
                )

        except APIException as e:
            print(f"An API error occurred during ontology deletion process: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main()
