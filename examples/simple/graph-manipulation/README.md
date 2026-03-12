# Graph Manipulation Example

This example demonstrates the end-to-end process of:

1.  **Building a knowledge graph from a file** using a live `PerseusClient`.
2.  **Serializing the initial graph** to [Turtle (TTL)](https://www.w3.org/TR/turtle/) and [Cypher Query Language (CQL)](https://neo4j.com/docs/cypher-manual/current/) formats.
3.  **Manipulating the in-memory graph** by adding new entities and relations.
4.  **Re-serializing the modified graph** to TTL and CQL to reflect the changes.

## Prerequisites

1.  A running instance of the Perseus API.
2.  Your Perseus API key.

## How to Run

1.  Navigate to the example directory:
    ```bash
    cd examples/simple/graph-manipulation
    ```

2.  Create a `.env` file from the template:
    ```bash
    cp template.env .env
    ```

3.  Edit the `.env` file and add your `PERSEUS_API_KEY`.

4.  Install the `perseus-client` in editable mode from the project root if you haven't already:
    ```bash
    # If you are not in the project root, navigate there first:
    # cd ../../../ # Go to the project root: perseus-client/
    pip install -e .
    ```

5.  Run the example script:
    ```bash
    python graph_manipulation.py
    ```

This will:
- Connect to the Perseus API to build a graph from `assets/sample.txt`.
- Create an `output` directory within the example folder.
- Save the initial graph to `output/output_graph_original.ttl` and `output/output_graph_original.cql`.
- Add a new "Award" entity and a "WON_AWARD" relation in-memory.
- Save the modified graph to `output/output_graph_modified.ttl` and `output/output_graph_modified.cql`.