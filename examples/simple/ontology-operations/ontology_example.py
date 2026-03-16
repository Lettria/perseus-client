import logging
from perseus_client.client import PerseusClient
from perseus_client.models import OntologyStatus

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def demonstrate_ontology_operations():
    """
    Demonstrates uploading an ontology, waiting for it to be processed, and finding it.
    """
    ontology_path = "assets/example_ontology.ttl"
    logging.info("--- Starting Ontology Operations Demonstration ---")

    with PerseusClient() as client:
        try:
            # Step 1: Upload an ontology
            logging.info(f"Uploading ontology: {ontology_path}")
            # The upload_ontology method returns an Ontology object
            uploaded_ontology = client.ontology.upload_ontology(ontology_path)
            logging.info(f"Ontology upload initiated. Ontology ID: {uploaded_ontology.id}, Status: {uploaded_ontology.status}")

            # Step 2: Wait for the ontology to be fully processed
            if uploaded_ontology.status == OntologyStatus.PENDING:
                logging.info(f"Waiting for ontology {uploaded_ontology.id} to be processed...")
                # The wait_for_ontology_upload method polls until the status is UPLOADED or FAILED
                processed_ontology = client.ontology.wait_for_ontology_upload(uploaded_ontology.id)
                logging.info(f"Ontology processing complete. Final Status: {processed_ontology.status}")

            # Step 3: Find the ontology by its ID
            logging.info(f"Attempting to find the ontology with ID: {uploaded_ontology.id}")
            # The find_ontologies method returns a list of Ontology objects
            found_ontologies = client.ontology.find_ontologies(ids=[uploaded_ontology.id])

            if found_ontologies:
                logging.info("Successfully found the ontology:")
                for o in found_ontologies:
                    logging.info(f"  - ID: {o.id}, Name: {o.name}, Status: {o.status}, Created At: {o.created_at}")
            else:
                logging.warning(f"Could not find ontology with ID: {uploaded_ontology.id}")

        except Exception as e:
            logging.error(f"An unexpected error occurred during ontology operations: {e}")

    logging.info("--- Finished Ontology Operations Demonstration ---")


if __name__ == "__main__":
    demonstrate_ontology_operations()
