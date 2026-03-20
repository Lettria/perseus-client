# Graph Manipulation

This example demonstrates the end-to-end process of:

1.  **Building a knowledge graph from a file** using a live `PerseusClient`.
2.  **Injects metadata** into the graph, such as `source: graph_manipulation_example`.
3.  **Manipulating the in-memory graph** by adding new entities and relations.
4.  **Saving the modified graph to Neo4j**.
5.  **Serializing the modified graph** to TTL to reflect the changes.

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

3.  **Start Neo4j:**
    A `docker compose.yaml` file is provided to easily start a Neo4j instance.
    ```bash
    docker compose up -d
    ```

### Usage

Run the `graph_manipulation.py` script from your terminal:

```bash
python graph_manipulation.py
```

## Expected output

The script will:

- Connect to the Perseus API to build a graph from `assets/sample.txt`.
- Create an `output` directory within the example folder.
- Save the initial graph to `output/output_graph_original.ttl`.
- Add a new "Award" entity and a "WON_AWARD" relation in-memory.
- **Save the modified graph to your configured Neo4j instance.**
- Save the modified graph to `output/output_graph_modified.ttl`.
- Log the progress to the console.

After the script finishes, you can connect to your Neo4j instance (e.g., via the Neo4j Browser at `http://localhost:7474`) to see the modified graph.
