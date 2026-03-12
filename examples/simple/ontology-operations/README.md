# Ontology Operations Example

This example demonstrates basic ontology operations using the Perseus Client:
1.  **Uploading an ontology:** It shows how to upload a local `.ttl` ontology file to the Perseus platform.
2.  **Waiting for processing:** It demonstrates how to poll the status of the uploaded ontology until it is fully processed.
3.  **Finding an ontology:** It shows how to retrieve the details of an uploaded ontology using its ID.

## Setup

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set up your environment:**
    Create a `.env` file in this directory with your `LETTRIA_API_KEY`.
    ```env
    LETTRIA_API_KEY="YOUR_API_KEY"
    ```

## Usage

Run the `ontology_example.py` script from your terminal:

```bash
python ontology_example.py
```

The script will log its actions to the console. You will see the process of the ontology being uploaded, the script waiting for its status to change to `UPLOADED`, and finally, the script successfully finding and displaying the ontology's details.
