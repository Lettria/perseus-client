import logging
from perseus_client.client import PerseusClient
from perseus_client.exceptions import APIException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def demonstrate_file_deletion():
    """
    Demonstrates uploading a file and then deleting it.
    """
    file_path = "assets/file_to_delete.txt"
    logging.info(f"--- Starting File Deletion Demonstration ---")
    
    with PerseusClient() as client:
        try:
            # Step 1: Upload a file
            logging.info(f"Uploading file: {file_path}")
            uploaded_file = client.file.upload_file(file_path)
            logging.info(f"File uploaded successfully. File ID: {uploaded_file.id}")

            # Step 2: Delete the file
            logging.info(f"Deleting file with ID: {uploaded_file.id}")
            client.file.delete_file(uploaded_file.id)
            logging.info("Delete command issued successfully.")

            # Step 3: Verify deletion
            logging.info(f"Verifying that file {uploaded_file.id} has been deleted...")
            found_files = client.file.find_files(ids=[uploaded_file.id])
            if not found_files:
                logging.info(f"Verification successful: File {uploaded_file.id} was not found.")
            else:
                logging.warning(f"Verification failed: File {uploaded_file.id} still exists.")

        except APIException as e:
            logging.error(f"An API error occurred during the file deletion process: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
    logging.info(f"--- Finished File Deletion Demonstration ---
")


def demonstrate_ontology_deletion():
    """
    Demonstrates uploading an ontology and then deleting it.
    """
    ontology_path = "assets/ontology_to_delete.ttl"
    logging.info(f"--- Starting Ontology Deletion Demonstration ---")

    with PerseusClient() as client:
        try:
            # Step 1: Upload an ontology
            logging.info(f"Uploading ontology: {ontology_path}")
            uploaded_ontology = client.ontology.upload_ontology(ontology_path)
            logging.info(f"Ontology uploaded successfully. Ontology ID: {uploaded_ontology.id}")

            # Step 2: Delete the ontology
            logging.info(f"Deleting ontology with ID: {uploaded_ontology.id}")
            client.ontology.delete_ontology(uploaded_ontology.id)
            logging.info("Delete command issued successfully.")

            # Step 3: Verify deletion
            logging.info(f"Verifying that ontology {uploaded_ontology.id} has been deleted...")
            found_ontologies = client.ontology.find_ontologies(ids=[uploaded_ontology.id])
            if not found_ontologies:
                logging.info(f"Verification successful: Ontology {uploaded_ontology.id} was not found.")
            else:
                logging.warning(f"Verification failed: Ontology {uploaded_ontology.id} still exists.")

        except APIException as e:
            logging.error(f"An API error occurred during the ontology deletion process: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
    logging.info(f"--- Finished Ontology Deletion Demonstration ---")


if __name__ == "__main__":
    demonstrate_file_deletion()
    demonstrate_ontology_deletion()
