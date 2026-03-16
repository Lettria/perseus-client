# Delete Operations

This example demonstrates how to use the Perseus Client to delete files and ontologies that have been uploaded to the platform.

The script performs two main demonstrations:

1.  **File Deletion:** It uploads a sample text file, deletes it, and then verifies that the file can no longer be found.
2.  **Ontology Deletion:** It uploads a sample ontology file, deletes it, and then verifies its deletion.

## How to run

### Setup

1.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Set up your environment:**
    Create a `.env` file in this directory and add your `PERSEUS_API_KEY`.
    ```env
    PERSEUS_API_KEY="YOUR_API_KEY"
    ```

### Usage

Run the `delete_example.py` script from your terminal:

```bash
python delete_example.py
```

## Expected output

The script will log its progress to the console, showing the steps for uploading, deleting, and verifying for both a file and an ontology. You will see success messages if the deletions are performed and verified correctly.
