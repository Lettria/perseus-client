# Build Graph Example

This example demonstrates how to build a knowledge graph from a text file and an ontology using the Perseus Client.

## Setup

1.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Set up your environment:**
    Create a `.env` file in this directory and add your `PERSEUS_API_KEY`:
    ```env
    PERSEUS_API_KEY="YOUR_API_KEY"
    ```

## Usage

Run the `build_graph.py` script:

```bash
python build_graph.py
```

The script will:

1.  Upload the `assets/pizza.txt` file and `assets/pizza.ttl` ontology.
2.  Run a job to process the file and generate a knowledge graph.
3.  Download the output graph in TTL and CQL formats to the `output/` directory.
