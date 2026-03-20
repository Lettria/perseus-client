# SPARQL Query with RDFLib

This example demonstrates how to use the Perseus Client to build a knowledge graph, convert it into an RDFLib graph, and then execute a SPARQL query to retrieve specific information from it.

## Setup

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

## Usage

Run the `sparql_example.py` script.

```bash
python sparql_example.py
```

The script will:

1.  Upload the `assets/sample.txt` file along with the local `assets/ontology.ttl`.
2.  Run a job to process the file and generate a knowledge graph based on the provided ontology.
3.  Convert the `KnowledgeGraph` object to an `rdflib.Graph`.
4.  Save the resulting RDFLib graph to `./output/sample.graph.ttl`.
5.  Execute a SPARQL query to find all `ont:Person` entities and their `rdfs:label`.
6.  Print the results to the console.

After running, you should see output showing the graph being built and the results of the SPARQL query, listing the persons found in the text and their names, as defined by the ontology. A `sample.graph.ttl` file will also be generated in the `output/` directory, containing the RDFLib graph in Turtle format.
