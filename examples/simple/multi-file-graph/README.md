# Multi-File Graph Building with Ontology Example

This example demonstrates how to use the `PerseusClient` to build multiple knowledge graphs from several input files while applying a shared ontology.

It showcases:

1.  Building two separate `KnowledgeGraph` objects from `person1.txt` and `person2.txt`.
2.  Applying a single `ontology.ttl` to both graphs during the build process.
3.  **Saving the modified graphs directly to Neo4j**, using native Neo4j naming conventions for easier database exploitation.

## Prerequisites

1.  A running instance of the Perseus API.
2.  Your Perseus API key.
3.  [Docker](https://docs.docker.com/get-docker/) installed and running.
4.  Optionally, a local Neo4j instance can be started using Docker Compose provided in this example.

## How to Run

1.  Navigate to the example directory:
    ```bash
    cd examples/simple/multi-file-graph
    ```

2.  **Start Neo4j using Docker Compose:**
    ```bash
    docker-compose up -d neo4j
    ```
    Wait a few moments for Neo4j to fully start. You can check its health with `docker-compose ps`.

3.  Create a `.env` file from the template and fill in your API details:
    ```bash
    cp template.env .env
    ```

4.  Edit the `.env` file and add your `PERSEUS_API_KEY`.

5.  Install the `perseus-client` (with `rdf` and `neo4j` extras) in editable mode from the project root if you haven't already:
    ```bash
    # If you are not in the project root, navigate there first:
    # cd ../../../ # Go to the project root: perseus-client/
    pip install -e '.[rdf,neo4j]'
    ```

6.  Run the example script:
    ```bash
    python build_multi_file_graph.py
    ```

This will:
- Connect to the Perseus API to build two graphs from `assets/person1.txt` and `assets/person2.txt`, using `assets/ontology.ttl`.
- **Save the modified graphs directly to your configured Neo4j instance.**

## Cleaning Up

To stop and remove the Neo4j container and its associated volume, run:
```bash
docker-compose down -v
```
