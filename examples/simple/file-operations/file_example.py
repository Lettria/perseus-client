import logging
from perseus_client.client import PerseusClient
from perseus_client.models import FileStatus

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def demonstrate_file_operations():
    """
    Demonstrates uploading a file, waiting for it to be processed, and finding it.
    """
    file_path = "assets/example.txt"
    logging.info("--- Starting File Operations Demonstration ---")

    with PerseusClient() as client:
        try:
            # Step 1: Upload a file
            logging.info(f"Uploading file: {file_path}")
            # The upload_file method returns a File object upon successful API call
            uploaded_file = client.file.upload_file(file_path)
            logging.info(f"File upload initiated. File ID: {uploaded_file.id}, Status: {uploaded_file.status}")

            # Step 2: Wait for the file to be fully processed by the backend
            # This is important for ensuring the file is ready before proceeding
            if uploaded_file.status == FileStatus.PENDING:
                logging.info(f"Waiting for file {uploaded_file.id} to be processed...")
                # The wait_for_file_upload method polls until the file status is UPLOADED or FAILED
                processed_file = client.file.wait_for_file_upload(uploaded_file.id)
                logging.info(f"File processing complete. Final Status: {processed_file.status}")
            
            # Step 3: Find the file by its ID
            logging.info(f"Attempting to find the file with ID: {uploaded_file.id}")
            # The find_files method returns a list of File objects
            found_files = client.file.find_files(ids=[uploaded_file.id])

            if found_files:
                logging.info(f"Successfully found the file:")
                for f in found_files:
                    logging.info(f"  - ID: {f.id}, Name: {f.name}, Status: {f.status}, Created At: {f.created_at}")
            else:
                logging.warning(f"Could not find file with ID: {uploaded_file.id}")

        except Exception as e:
            logging.error(f"An unexpected error occurred during file operations: {e}")
            
    logging.info("--- Finished File Operations Demonstration ---")


if __name__ == "__main__":
    demonstrate_file_operations()
