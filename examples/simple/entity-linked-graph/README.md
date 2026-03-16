# Graph Interlinking Example

This example demonstrates how to build multiple knowledge graphs and then merge them into a single, unified graph using the `KnowledgeGraph.interlink()` method.

It showcases:

1.  Building two separate `KnowledgeGraph` objects from `person1.txt` and `person2.txt`.
2.  Applying a shared `ontology.ttl` to both graphs.
3.  **Calling `KnowledgeGraph.interlink()`** to deduplicate entities based on their `rdfs:label` and merge the two graphs.
4.  **Saving the final, unified graph directly to Neo4j**.

## Prerequisites

1.  A running instance of the Perseus API.
2.  Your Perseus API key.
3.  [Docker](https://docs.docker.com/get-docker/) installed and running.

## How to Run

1.  Navigate to the example directory:
    ```bash
    cd examples/simple/interlink-graphs
    ```

2.  **Start Neo4j using Docker Compose:**
    ```bash
    docker-compose up -d neo4j
    ```
    Wait a few moments for Neo4j to fully start. You can check its health with `docker-compose ps`.

3.  Create a `.env` file from the template and fill in your API and Neo4j details:
    ```bash
    cp template.env .env
    ```

4.  Edit the `.env` file and add your `PERSEUS_API_KEY`. The Neo4j details can be left as default if you are using the provided `docker-compose.yaml`.

5.  Install the `perseus-client` (with `rdf` and `neo4j` extras) in editable mode from the project root if you haven't already:
    ```bash
    # If you are not in the project root, navigate there first:
    # cd ../../../ # Go to the project root: perseus-client/
    pip install -e '.[rdf,neo4j]'
    ```

6.  Run the example script:
    ```bash
    python interlink_graphs.py
    ```

This will:
- Build two separate graphs from `assets/person1.txt` and `assets/person2.txt`.
- Merge them into a single graph, deduplicating entities like "Alice" and "Bob".
- **Save the final, unified graph to your configured Neo4j instance.**

## Cleaning Up

To stop and remove the Neo4j container and its associated volume, run:
```bash
docker-compose down -v
```