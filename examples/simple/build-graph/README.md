# Build Graph

This example demonstrates how to build a knowledge graph from a text file, add custom metadata to all nodes and relationships, and save the resulting graph to a Neo4j database.

## Setup

1.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

2.  **Set up your environment:**
    Create a `.env` file in this directory and add your `PERSEUS_API_KEY`. You can also configure your Neo4j connection details here if they are different from the defaults in the `docker-compose.yaml`.

    ```env
    PERSEUS_API_KEY="YOUR_API_KEY"
    NEO4J_URI="bolt://localhost:7687"
    NEO4J_USER="neo4j"
    NEO4J_PASSWORD="j4oenj4oen"
    ```

3.  **Start Neo4j:**
    A `docker-compose.yaml` file is provided to easily start a Neo4j instance.
    ```bash
    docker-compose up -d
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
