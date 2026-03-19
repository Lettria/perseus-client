<div align="center">

# Perseus Text-to-Graph

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-available-blue.svg)](https://docs.perseus.lettria.net/)

[Documentation](https://docs.perseus.lettria.net/)

</div>

In today's world, a vast amount of valuable information is locked away in unstructured text—documents, articles, emails, and more. While AI and analytics tools are incredibly powerful, they struggle to make sense of this chaotic data. They need structured, connected information to reason effectively.

This is where the gap lies:

| **What Organizations Have** | **What AI Systems Need**        |
| :-------------------------- | :------------------------------ |
| 📄 **Unstructured Text**    | 🔗 **Connected Knowledge**      |
| Chaotic, disconnected data  | Structured, queryable graphs    |
| Implicit relationships      | Explicit entities and relations |
| Hard to query and analyze   | Ready for deep analysis         |

Without a way to bridge this gap, AI systems can't unlock the full potential of your data. They might miss critical insights, provide incomplete answers, or fail to see the bigger picture.

Lettria's Perseus service is designed to solve this problem. It transforms your raw text into a structured knowledge graph, making it instantly usable for AI applications, from advanced search to complex reasoning. Furthermore, the SDK empowers users to leverage their own ontologies, providing a flexible way to define the desired data schema. This greatly reduces data complexity and ensures the generated knowledge graph is precisely tailored to specific use cases.

## 🌟 Features

- **Asynchronous Client**: High-performance, non-blocking API calls using `asyncio` and `aiohttp`.
- **Simple Interface**: Easy-to-use methods for file operations, ontology management, and graph building.
- **Data Validation**: Robust data modeling with `pydantic`.
- **Neo4j Integration**: Directly save your graph data to a Neo4j instance.
- **FalkorDB Integration**: Directly save your graph data to a FalkorDB instance.
- **Flexible Configuration**: Configure via environment variables or directly in code.

## 📦 Installation

```bash
# For both Neo4j and FalkorDB support
pip install "perseus-client[all]==1.0.0-rc.13"

# For Neo4j support
pip install "perseus-client[neo4j]==1.0.0-rc.13"

# For FalkorDB support
pip install "perseus-client[falkordb]==1.0.0-rc.13"
```

## 🚀 Quick Start

To start using the SDK, you will need an API key from Lettria.

To create an API key, please visit our app [here](https://app.perseus.lettria.net/).

### Configuration

The SDK can be configured via environment variables. The `PerseusClient` will automatically load them. You can place them in a `.env` file in your project root.

| Variable              | Description                                | Required |
| --------------------- | ------------------------------------------ | -------- |
| `PERSEUS_API_KEY`     | Your unique API key for the Lettria API.   | Yes      |
| `LOGLEVEL`            | The log level for the client.              | No       |
| `NEO4J_URI`           | The URI for your Neo4j database instance.  | No       |
| `NEO4J_USER`          | The username for your Neo4j database.      | No       |
| `NEO4J_PASSWORD`      | The password for your Neo4j database.      | No       |
| `FALKORDB_HOST`       | The host for your FalkorDB instance.       | No       |
| `FALKORDB_PORT`       | The port for your FalkorDB (default 6379). | No       |
| `FALKORDB_GRAPH_NAME` | The name of the graph key to use.          | No       |
| `FALKORDB_PASSWORD`   | The password for your FalkorDB instance.   | No       |

By default, the log level is set to `INFO`. You can change it by setting the `LOGLEVEL` environment variable to `DEBUG`, `WARNING`, `ERROR`, or `CRITICAL`.

### Example: Build a Graph

This example shows how to build a graph from a text file.

```python
import perseus_client

# This will automatically use the configuration from your environment variables.
try:
    # Build a graph from a document
    knowledge_graphs = perseus_client.build_graph(
        file_paths=["path/to/your/document.txt"],
    )

    # Print the result
    for graph in knowledge_graphs:
        print(f"🎉 Graph built successfully with {len(graph.entities)} entities and {len(graph.relations)} relations!")

except Exception as e:
    print(f"An error occurred: {e}")
```

### The `KnowledgeGraph` Object

The `build_graph_async` method returns a `KnowledgeGraph` object, which holds the structured data of your graph.

#### Properties

| Property      | Type             | Description                                              |
| ------------- | ---------------- | -------------------------------------------------------- |
| `entities`    | `List[Entity]`   | A list of nodes (entities) in the graph.                 |
| `relations`   | `List[Relation]` | A list of relationships (facts) connecting the entities. |
| `documents`   | `List[Document]` | A list of source documents used to generate the graph.   |
| `ttl_content` | `Optional[str]`  | The raw TTL content of the graph.                        |
| `cql_content` | `Optional[str]`  | The raw Cypher Query Language content of the graph.      |

#### Methods

The `KnowledgeGraph` object also has several built-in methods to save or convert the data to different formats and databases.

| Method                                                  | Return Type      | Description                                                     |
| ------------------------------------------------------- | ---------------- | --------------------------------------------------------------- |
| `save_ttl(file_path: str)`                              | `None`           | Saves the graph to a TTL file.                                  |
| `to_ttl()`                                              | `str`            | Returns the graph as a TTL string.                              |
| `save_cql(file_path: str, strip_prefixes: bool = True)` | `None`           | Saves the graph to a CQL file.                                  |
| `to_cql(strip_prefixes: bool = True)`                   | `str`            | Returns the graph as a CQL string.                              |
| `save_to_neo4j(strip_prefixes: bool = True)`            | `None`           | Saves the graph to a Neo4j instance synchronously.              |
| `save_to_neo4j_async(strip_prefixes: bool = True)`      | `None`           | Saves the graph to a Neo4j instance asynchronously.             |
| `save_to_falkordb(strip_prefixes: bool = True)`         | `None`           | Saves the graph to a FalkorDB instance synchronously.           |
| `save_to_falkordb_async(strip_prefixes: bool = True)`   | `None`           | Saves the graph to a FalkorDB instance asynchronously.          |
| `to_json()`                                             | `dict`           | Converts the knowledge graph to a JSON serializable dictionary. |
| `interlink(kbs: List[KnowledgeGraph], ...)`             | `KnowledgeGraph` | Merges multiple `KnowledgeGraph` objects into a single one.     |

### Merging `KnowledgeGraph`s

You can merge multiple `KnowledgeGraph` objects using the `perseus_client.interlink` function.

```python
import perseus_client

try:
    # Build two graphs in a single call
    knowledge_graphs = perseus_client.build_graph(
        file_paths=["path/to/document1.txt", "path/to/document2.txt"]
    )

    # Interlink them
    if len(knowledge_graphs) >= 2:
        merged_graph = perseus_client.interlink(kbs=knowledge_graphs)
        print(f"🎉 Graphs merged successfully with {len(merged_graph.entities)} entities and {len(merged_graph.relations)} relations!")

except Exception as e:
    print(f"An error occurred: {e}")
```

## ⚡ Advanced Usage: Asynchronous Client

For long-running asynchronous applications or when you need explicit control over the client's lifecycle, you can use `PerseusClient` as an asynchronous context manager. This ensures connections are managed optimally and closed precisely when you're done.

```python
import asyncio
from typing import List
from perseus_client import PerseusClient
from perseus_client.models import KnowledgeGraph

async def main():
    async with PerseusClient() as client:
        try:
            graphs: List[KnowledgeGraph] = await client.build.build_graph_async(
                file_paths=["path/to/your/async_document.txt"],
                ontology_path="path/to/your/ontology.ttl",
            )
            for graph in graphs:
                print(f"⚡ Async Graph built successfully with {len(graph.entities)} entities and {len(graph.relations)} relations!")
        except Exception as e:
            print(f"An async error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
```

## 📚 API Reference

### `client.build_graph`

```python
def build_graph(
    file_paths: List[str],
    ontology_path: Optional[str] = None,
    refresh_graph: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[KnowledgeGraph]:
```

Processes one or more files by uploading them, optionally with an ontology, running jobs, and returning `KnowledgeGraph` objects synchronously.

| Parameter       | Type                       | Description                                                     | Default |
| --------------- | -------------------------- | --------------------------------------------------------------- | ------- |
| `file_paths`     | `List[str]`                | A list of file paths to process.                                |         |
| `ontology_path` | `Optional[str]`            | The path to the ontology file to use.                           | `None`  |
| `refresh_graph` | `bool`                     | Whether to force a new job to be created (refresh the graph).   | `False` |
| `metadata`      | `Optional[Dict[str, Any]]` | A dictionary of metadata to add to all nodes and relationships. | `None`  |

### `KnowledgeGraph.interlink`

```python
@staticmethod
def interlink(
    kbs: List["KnowledgeGraph"],
    interlinking_key_uris: List[str] = ["http://www.w3.org/2000/01/rdf-schema#label"],
    immutable_properties: Optional[List[str]] = None,
    merge_properties_on_conflict: bool = False,
) -> "KnowledgeGraph":
```

Merges multiple `KnowledgeGraph` objects into a single one based on a linking key. Entities are deduplicated and their properties are combined. By default, entities of different types will not be merged, even if they share the same linking key. If `immutable_properties` are specified, entities with conflicting values for these properties will also not be merged, resulting in separate entities in the final graph.

| Parameter                      | Type                  | Description                                                                                                                                                              | Default                                            |
| ------------------------------ | --------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------- |
| `kbs`                          | `List[KnowledgeGraph]`| A list of `KnowledgeGraph` objects to merge.                                                                                                                             |                                                    |
| `interlinking_key_uris`        | `List[str]`           | The URI of the property to use for linking entities (e.g., `rdfs:label`).                                                                                                  | `["http://www.w3.org/2000/01/rdf-schema#label"]` |
| `immutable_properties`         | `Optional[List[str]]` | A list of property URIs (e.g., `"http://purl.org/dc/elements/1.1/title"` or `"hasJobTitle"`) that, if their values conflict between entities, will prevent those entities from being merged. Instead, separate entities will be retained. | `None`                                             |
| `merge_properties_on_conflict` | `bool`                | If `True`, merges properties when a conflict occurs. Otherwise, keeps the first one.                                                                                     | `False`                                            |

## 📂 Examples

For more detailed examples, check out the [`examples/`](./examples/) directory. Each example has its own README with instructions.

### Simple Examples

- **[Build Graph](./examples/simple/build-graph/)**: Build a knowledge graph from a text file.
- **[Delete Operations](./examples/simple/delete-operations/)**: Delete files and ontologies.
- **[Entity Linked Graph](./examples/simple/entity-linked-graph/)**: Interlink multiple graphs into a unified structure.
- **[File Operations](./examples/simple/file-operations/)**: Upload and manage files.
- **[Graph Manipulation](./examples/simple/graph-manipulation/)**: Perform custom modifications on the graph.
- **[Ontology Operations](./examples/simple/ontology-operations/)**: Upload and manage ontologies.
- **[SPARQL Query](./examples/simple/sparql-query/)**: Query the graph using SPARQL.

### Advanced Examples

- **[Finance Compliance](./examples/advanced/finance-compliance/)**: A complete pipeline to convert unstructured sustainability disclosures into a knowledge graph and produce CSRD-compliant reports.
- **[Graph RAG Reporting FalkorDB](./examples/advanced/graph-rag-reporting-falkordb/)**: A complete workflow to turn a PDF into a knowledge graph and generate a report. Graph is saved in FalkorDB.
- **[Graph RAG Reporting Neo4j](./examples/advanced/graph-rag-reporting-neo4j/)**: A complete workflow to turn a PDF into a knowledge graph and generate a report. Graph is saved in Neo4j.

## 🤝 Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

## 📧 Contact

For support or questions, please reach out at `hello@lettria.com`.

## 📄 License

This SDK is licensed under the MIT License.
