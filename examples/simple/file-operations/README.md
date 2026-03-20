# File Operations

This example demonstrates the basic file operations using the Perseus Client:

1.  **Uploading a file:** It shows how to upload a local file to the Perseus platform.
2.  **Waiting for processing:** It demonstrates how to poll the status of the uploaded file until it is fully processed and ready.
3.  **Finding a file:** It shows how to retrieve the details of an uploaded file using its ID.

## How to run

### Setup

1.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Set up your environment:**
    Copy the `template.env` file to `.env` and fill in the required environment variables.

    ```bash
    cp template.env .env
    ```

    You will need to fill in the following variables in your new `.env` file:
    - `PERSEUS_API_KEY`

### Usage

Run the `file_example.py` script from your terminal:

```bash
python file_example.py
```

## Expected output

The script will log its actions to the console. You will see the process of the file being uploaded, the script waiting for its status to change to `UPLOADED`, and finally, the script successfully finding and displaying the file's details.
