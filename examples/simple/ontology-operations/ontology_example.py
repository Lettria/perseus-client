import logging
import perseus_client
from perseus_client.models import OntologyStatus

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def demonstrate_ontology_operations():
    """
    Demonstrates uploading an ontology, waiting for it to be processed, and finding it.
    """
    ontology_path = "assets/example_ontology.ttl"
    logging.info("--- Starting Ontology Operations Demonstration ---")

    try:
        logging.info(f"Uploading ontology: {ontology_path}")
        uploaded_ontology = perseus_client.upload_ontology(ontology_path)
        logging.info(f"Ontology upload initiated. Ontology ID: {uploaded_ontology.id}, Status: {uploaded_ontology.status}")

        if uploaded_ontology.status == OntologyStatus.PENDING:
            logging.info(f"Waiting for ontology {uploaded_ontology.id} to be processed...")
            processed_ontology = perseus_client.wait_for_ontology_upload(uploaded_ontology.id)
            logging.info(f"Ontology processing complete. Final Status: {processed_ontology.status}")

        logging.info(f"Attempting to find the ontology with ID: {uploaded_ontology.id}")
        found_ontologies = perseus_client.find_ontologies(ids=[uploaded_ontology.id])

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
