# Custom TTL Manipulation Example

This example demonstrates how to generate a knowledge graph with the Perseus SDK and then programmatically modify the resulting Turtle (TTL) file using the `rdflib` library.

The custom manipulation involves parsing the TTL file, finding all entities of the class `:Person`, and adding a new property (`:status "verified"`) to each of them. This workflow does not require a database connection.

## Prerequisites

- Python 3.8+
- An active Lettria API key

## Setup

1.  **Navigate to the example directory:**
    ```bash
    cd examples/simple/ttl-manipulation
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

## Usage

Run the script with the path to the sample text file.

```bash
source .env
python manipulate_ttl.py assets/sample.txt
```

### Workflow Steps

1.  The script calls the Perseus `build_graph` method with a custom ontology to generate a graph from `sample.txt`, saving the output to `./output/graph.ttl`.
2.  It then reads the `graph.ttl` file and uses `rdflib` to parse its content.
3.  It finds all subjects of type `:Person` and adds a new triple, `:status "verified"`, to each.
4.  Finally, it saves the modified graph to a new file, `./output/graph_modified.ttl`.

### Verify the Result

After running the script, you can inspect the contents of `./output/graph_modified.ttl`. You will see that each `Person` entity now has an additional `meta:status` property with the value `"verified"`.
