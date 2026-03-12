# TTL Manipulation Example

This example demonstrates a workflow where you generate a knowledge graph, programmatically modify the resulting RDF Turtle (TTL) data, and save the result. This is useful for enriching or transforming the graph data before further use.

The script uses the `rdflib` library to parse the TTL and add a new property (`status: "verified"`) to all entities that are of the type `Person` according to the provided ontology.

## Setup

1.  **Install dependencies:**
    This example requires `rdflib`.
    ```bash
    pip install -r requirements.txt
    ```

2.  **Set up your environment:**
    Create a `.env` file in this directory with your `LETTRIA_API_KEY`.
    ```env
    LETTRIA_API_KEY="YOUR_API_KEY"
    ```

## Usage

Run the `manipulate_ttl.py` script, optionally providing a path to a text file. If no path is given, it will use `assets/sample.txt`.

```bash
python manipulate_ttl.py [path/to/your/file.txt]
```

The script performs the following steps:
1.  **Generate Graph:** It calls the Perseus API to generate a knowledge graph from the input text and `assets/ontology.ttl`.
2.  **Intercept TTL:** It retrieves the generated graph as a TTL-formatted string.
3.  **Manipulate TTL:** Using `rdflib`, it parses the string, finds all triples that declare an entity as a `Person`, and adds a new `status` property to each of them.
4.  **Save Results:** The original and the modified TTL content are saved to the `output/` directory for you to inspect.

After running, you can compare `output/graph.ttl` and `output/graph_modified.ttl` to see the added triples.
