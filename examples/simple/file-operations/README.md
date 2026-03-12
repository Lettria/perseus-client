# File Operations Example

This example demonstrates the basic file operations using the Perseus Client:

1.  **Uploading a file:** It shows how to upload a local file to the Perseus platform.
2.  **Waiting for processing:** It demonstrates how to poll the status of the uploaded file until it is fully processed and ready.
3.  **Finding a file:** It shows how to retrieve the details of an uploaded file using its ID.

## Setup

1.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Set up your environment:**
    Create a `.env` file in this directory with your `PERSEUS_API_KEY`.
    ```env
    PERSEUS_API_KEY="YOUR_API_KEY"
    ```

## Usage

Run the `file_example.py` script from your terminal:

```bash
python file_example.py
```

The script will log its actions to the console. You will see the process of the file being uploaded, the script waiting for its status to change to `UPLOADED`, and finally, the script successfully finding and displaying the file's details.
