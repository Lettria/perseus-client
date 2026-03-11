# Custom CQL Manipulation Example

This example demonstrates a complete workflow where you first generate a graph using the Perseus SDK, then programmatically modify the resulting Cypher Query Language (CQL) file before finally loading it into a Neo4j database.

The custom manipulation in this example is to rename a node label from `:Person` to `:Individual`.

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- An active Lettria API key

## Setup

1.  **Navigate to the example directory:**
    ```bash
    cd examples/simple/cql-manipulation
    ```

2.  **Set up the environment:**
    - Create a `.env` file from the template:
      ```bash
      cp template.env .env
      ```
    - Edit the `.env` file and add your `LETTRIA_API_KEY`.

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Start the Neo4j database:**
    ```bash
    docker-compose up -d
    ```

## Usage

Run the script with the path to the sample text file. The script will orchestrate the full workflow.

```bash
source .env
python manipulate_cql.py assets/sample.txt
```

### Workflow Steps

1.  The script calls the Perseus `build_graph` method to generate a graph from `sample.txt` and saves the output to `./output/graph.cql`.
2.  It then reads `graph.cql` and programmatically replaces all occurrences of the label `:Person` with `:Individual`.
3.  Finally, it uses the SDK's Neo4j service to execute the modified CQL, loading the transformed graph into the database.

### Verify the Result

1.  Open the Neo4j Browser at `http://localhost:7474`.
2.  Connect to the database using the credentials from your `.env` file (e.g., `neo4j`/`password`).
3.  Run the following Cypher query to inspect the nodes. Note that you should query for `Individual`, not `Person`.
    ```cypher
    MATCH (n:Individual) RETURN n
    ```
    You will see the nodes that were originally labeled as `Person` are now labeled as `Individual`.

## Cleanup

To stop and remove the Neo4j container, run:
```bash
docker-compose down
```
