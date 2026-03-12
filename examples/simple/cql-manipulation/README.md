# CQL Manipulation Example

This example demonstrates a workflow where you generate a knowledge graph, programmatically modify the resulting Cypher Query Language (CQL) statements, and then load the modified graph into a Neo4j database.

This is useful when you need to perform custom transformations on the graph structure or properties before it's stored. In this specific example, we rename a node label from `Person` to `Individual`.

## Setup

1.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set up your environment:**
    Create a `.env` file in this directory with your `LETTRIA_API_KEY` and Neo4j connection details.
    ```env
    LETTRIA_API_KEY="YOUR_API_KEY"
    NEO4J_URI="bolt://localhost:7687"
    NEO4J_USER="neo4j"
    NEO4J_PASSWORD="password"
    ```

3.  **Start Neo4j:**
    A `docker-compose.yaml` file is provided to run a local Neo4j instance.
    ```bash
    docker-compose up -d
    ```

## Usage

Run the `manipulate_cql.py` script, providing a path to a text file. If no path is provided, it will use `assets/sample.txt`.

```bash
python manipulate_cql.py [path/to/your/file.txt]
```

The script performs the following steps:
1.  **Generate Graph:** It first calls the Perseus API to generate a knowledge graph from the input text file and a sample ontology (`assets/ontology.ttl`).
2.  **Intercept CQL:** Instead of directly saving the graph to Neo4j, it gets the generated CQL statements as a string. The original and modified CQL are saved to the `output/` directory for inspection.
3.  **Manipulate CQL:** A function (`rename_label_in_cql`) performs a simple string replacement on the CQL content to change all occurrences of the label `:Person` to `:Individual`.
4.  **Execute Modified CQL:** Finally, it connects to the Neo4j database and executes the *modified* CQL statements.

After the script completes, you can query your Neo4j database and verify that the nodes have the label `Individual` instead of `Person`.
