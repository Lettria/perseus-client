# Entity-Linked Graph

This example demonstrates how to build multiple knowledge graphs and then merge them into a single, unified graph using the `perseus_client.interlink()` method.

It showcases:

1.  Building two separate `KnowledgeGraph` objects from `person1.txt` and `person2.txt`.
2.  Applying a shared `ontology.ttl` to both graphs.
3.  Injects shared metadata into all graphs, such as `source: interlink_graphs_example`.
4.  **Calling `perseus_client.interlink()`** to deduplicate entities based on their `rdfs:label` and merge the two graphs.
5.  **Saving the final, unified graph directly to Neo4j**.

## How to run

### Setup

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

### Usage

Run the `interlink_graphs.py` script from your terminal:

```bash
python interlink_graphs.py
```

## Expected output

The script will:
- Build two separate graphs from `assets/person1.txt` and `assets/person2.txt`.
- Merge them into a single graph, deduplicating entities like "Alice" and "Bob".
- **Save the final, unified graph to your configured Neo4j instance.**
- Log the progress to the console.

After the script finishes, you can connect to your Neo4j instance (e.g., via the Neo4j Browser at `http://localhost:7474`) to see the unified graph.

## Advanced Usage: Handling Conflicting Properties

This example intentionally includes a conflict between `person1.txt` and `person2.txt` to demonstrate how to handle disagreeing properties during a merge.

-   In `assets/person1.txt`, Alice is a "software engineer".
-   In `assets/person2.txt`, Alice is a "dentist".

By default, the `interlink()` method will merge the two "Alice" entities into one and, since `merge_properties_on_conflict` is `True`, it will retain both job titles.

However, you can prevent merging based on specific properties by using the `immutable_properties` argument.

### Trying it out

1.  **Open `interlink_graphs.py`**.
2.  **Uncomment the `immutable_properties` line**:

    ```python
    # Before
    merged_kg = perseus_client.interlink(
        kbs=knowledge_graphs,
        merge_properties_on_conflict=True,
        # immutable_properties=["hasJobTitle"]
    )

    # After
    merged_kg = perseus_client.interlink(
        kbs=knowledge_graphs,
        merge_properties_on_conflict=True,
        immutable_properties=["hasJobTitle"]
    )
    ```

3.  **Run the script again.**

### Expected Outcome

-   **Default (commented out):** The two "Alice" entities are merged into a single node in Neo4j.
-   **With `immutable_properties=["hasJobTitle"]`:** The two "Alice" entities will **not** be merged because their `hasJobTitle` values are different. You will see two separate "Alice" nodes in your Neo4j graph.
