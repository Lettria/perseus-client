# Build Graph

This example demonstrates how to build a knowledge graph from a text file, add custom metadata to all nodes and relationships, and save the resulting graph to a Neo4j database.

## Setup

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

3.  **Start Neo4j:**
    A `docker compose.yaml` file is provided to easily start a Neo4j instance.
    ```bash
    docker compose up -d
    ```

## Usage

Run the `build_graph.py` script, optionally providing a path to a text file. If no path is provided, it will use the default `assets/sample.txt`.

```bash
python build_graph.py [path/to/your/file.txt]
```

The script will:

1.  Wait for the Neo4j container to be ready.
2.  Upload the specified text file.
3.  Run a job to process the file and generate a knowledge graph.
4.  Add the custom metadata defined in the script to every node and relationship in the graph.
5.  Connect to the Neo4j database and execute the CQL queries to create the graph.

After the script finishes, you can connect to your Neo4j instance (e.g., via the Neo4j Browser at `http://localhost:7474`) to see the graph with the added metadata.

## Variants: ways to call `build_graph`

Each script below shows one way to use `build_graph`. They only need `PERSEUS_API_KEY` (no Neo4j) and save their graphs to `output/`.

| Script                       | Shows                                                                                       |
| ---------------------------- | ------------------------------------------------------------------------------------------- |
| `build_graph_module.py`      | The module-level `perseus_client.build_graph(...)`. No client to create or close.           |
| `build_graph_with_client.py` | `with PerseusClient() as client:`, reusing one connection for several calls.                |
| `build_graph_async.py`       | `async with PerseusClient() as client:` and `await client.build_graph_async(...)`.          |
| `build_concurrent_graphs.py` | Many files with a concurrency limit: `build_graph(..., max_concurrency=N)`.                 |
| `build_graph_errors.py`      | Failure handling: `try` / `except` (default), and `return_exceptions=True` to keep partial results. |

```bash
python build_graph_module.py [path/to/file.txt]
python build_graph_with_client.py [path/to/file.txt]
python build_graph_async.py [path/to/file1.txt path/to/file2.txt ...]
python build_concurrent_graphs.py [--max-concurrency N] [file_or_directory ...]   # default: assets/, 2 at a time
python build_graph_errors.py
```

`build_graph_errors.py` deliberately includes a file that doesn't exist, so one file in its batch fails.

The `max_concurrency` and `return_exceptions` arguments require the version of `perseus-client` that includes the fix for issue #30.
