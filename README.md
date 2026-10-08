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
pip install "perseus-client[all]==1.0.0-rc.24"

# For Neo4j support
pip install "perseus-client[neo4j]==1.0.0-rc.24"

# For FalkorDB support
pip install "perseus-client[falkordb]==1.0.0-rc.24"
```

## 🚀 Quick Start

To start using the SDK, you’ll need an API key from Lettria, which you can create by visiting our app [here](https://app.perseus.lettria.net/).

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

knowledge_graphs = perseus_client.build_graph(
    file_paths=["path/to/your/document.txt"],
)
for graph in knowledge_graphs:
    print(f"🎉 Graph built successfully with {len(graph.entities)} entities and {len(graph.relations)} relations!")
```

#### Organizing Jobs with Projects

You can assign jobs to projects for better organization:

```python
import perseus_client

# Assign job to a project
knowledge_graphs = perseus_client.build_graph(
    file_paths=["path/to/your/document.txt"],
    project_id="your-project-id",
)

# Explicitly remove project assignment
knowledge_graphs = perseus_client.build_graph(
    file_paths=["path/to/your/document.txt"],
    project_id=None,
)

# Don't change project assignment (default)
knowledge_graphs = perseus_client.build_graph(
    file_paths=["path/to/your/document.txt"],
)
```

**Getting your Project ID:** Currently, you can find your project ID in the URL when viewing a project in the Perseus app. For example, in the URL `https://app.perseus.lettria.net/app/w/.../p/51b5764b-0f84-4aa6-8930-0acea7914617`, the project ID is the UUID after `/p/`. A "Copy Project ID" button will be added to the UI soon for easier access.

### The `KnowledgeGraph` Object

Both `build_graph` and `build_graph_async` methods return a `List[KnowledgeGraph]` (one graph per input file), which holds the structured data of your graphs.

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

`async with` is the recommended async pattern. Awaiting `client.build_graph_async(...)` or `client.interlink_async(...)` on a client that isn't active also works, but the session stays open until you call `client.close()`. The service properties (`client.file`, `client.job`, …) can't activate the client inside a running event loop, so use `async with` before accessing them.

### Client lifecycle with `PerseusClient`

When you create a `PerseusClient` yourself:

- `client.build_graph(...)` and `client.interlink(...)` open a session for the call and close it when they return, unless the client is already active.
- The service properties (`client.file`, `client.job`, `client.ontology`, …) open a session that stays open. Use `with PerseusClient() as client:` or call `client.close()` when you're done.
- Services keep the session they were created with. Use `client.file.<method>(...)` each time instead of storing `client.file` and reusing it after `close()`.

```python
from perseus_client import PerseusClient

with PerseusClient() as client:
    graphs = client.build_graph(file_paths=["doc1.txt", "doc2.txt"])
    files = client.file.find_files()
```

The module-level functions (`perseus_client.build_graph(...)`, …) share a single client whose session is closed automatically when the interpreter exits.

## 📚 API Reference

### `client.build_graph`

```python
def build_graph(
    file_paths: List[str],
    ontology_path: Optional[str] = None,
    refresh_graph: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
```python
def build_graph(
    file_paths: List[str],
    ontology_path: Optional[str] = None,
    refresh_graph: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
    project_id: Optional[str] = None,
    max_concurrency: int = 10,
    return_exceptions: bool = False,
) -> List[Union[KnowledgeGraph, BaseException]]:
```

Processes one or more files by uploading them, optionally with an ontology, running jobs, and returning `KnowledgeGraph` objects synchronously.

**Note:** Input file size limits for text-to-graph generation vary by workspace plan. If your files exceed the limit, you will receive an error message indicating your plan's specific limit. Upgrade to a paid plan to increase these limits.

| Parameter       | Type                       | Description                                                     | Default |
| --------------- | -------------------------- | --------------------------------------------------------------- | ------- |
| `file_paths`    | `List[str]`                | A list of file paths to process.                                |         |
| `ontology_path` | `Optional[str]`            | The path to the ontology file to use.                           | `None`  |
| `refresh_graph` | `bool`                     | Whether to force a new job to be created (refresh the graph).   | `False` |
| `metadata`      | `Optional[Dict[str, Any]]` | A dictionary of metadata to add to all nodes and relationships. | `None`  |
| `project_id`    | `Optional[str]`            | The project ID to assign jobs to. Pass explicit `None` to unassign from any project. If not provided, the job's project assignment remains unchanged. | Not set |
| `max_concurrency` | `int`                    | The maximum number of files processed at the same time (upload, job and download). Must be at least 1. | `10` |
| `return_exceptions` | `bool`                 | If `False`, the first failure cancels the remaining files and is raised. If `True`, every file is processed and the list holds a `KnowledgeGraph` or the exception for each file. | `False` |

Results are returned in the same order as `file_paths`. Each job's timeout (one hour) starts when the file gets a concurrency slot, and includes any time the job spends queued on the server.

To keep the graphs that succeeded when some files fail, use `return_exceptions=True`:

```python
results = client.build_graph(file_paths=paths, return_exceptions=True)
graphs = [r for r in results if isinstance(r, KnowledgeGraph)]
failed = [(p, r) for p, r in zip(paths, results) if isinstance(r, BaseException)]
```
```

Processes one or more files by uploading them, optionally with an ontology, running jobs, and returning `KnowledgeGraph` objects synchronously.

**Note:** Input file size limits for text-to-graph generation vary by workspace plan. If your files exceed the limit, you'll receive an error message indicating your plan's specific limit. Upgrade to a paid plan to increase these limits.

| Parameter       | Type                       | Description                                                     | Default |
| --------------- | -------------------------- | --------------------------------------------------------------- | ------- |
| `file_paths`    | `List[str]`                | A list of file paths to process.                                |         |
| `ontology_path` | `Optional[str]`            | The path to the ontology file to use.                           | `None`  |
| `refresh_graph` | `bool`                     | Whether to force a new job to be created (refresh the graph).   | `False` |
| `metadata`      | `Optional[Dict[str, Any]]` | A dictionary of metadata to add to all nodes and relationships. | `None`  |
```python
def build_graph(
    file_paths: List[str],
    ontology_path: Optional[str] = None,
    refresh_graph: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
    project_id: Optional[str] = None,
    max_concurrency: int = 10,
    return_exceptions: bool = False,
) -> List[Union[KnowledgeGraph, BaseException]]:
```

Processes one or more files by uploading them, optionally with an ontology, running jobs, and returning `KnowledgeGraph` objects synchronously.

**Note:** Input file size limits for text-to-graph generation vary by workspace plan. If your files exceed the limit, you will receive an error message indicating your plan's specific limit. Upgrade to a paid plan to increase these limits.

| Parameter       | Type                       | Description                                                     | Default |
| --------------- | -------------------------- | --------------------------------------------------------------- | ------- |
| `file_paths`    | `List[str]`                | A list of file paths to process.                                |         |
| `ontology_path` | `Optional[str]`            | The path to the ontology file to use.                           | `None`  |
| `refresh_graph` | `bool`                     | Whether to force a new job to be created (refresh the graph).   | `False` |
| `metadata`      | `Optional[Dict[str, Any]]` | A dictionary of metadata to add to all nodes and relationships. | `None`  |
| `project_id`    | `Optional[str]`            | The project ID to assign jobs to. Pass explicit `None` to unassign from any project. If not provided, the job's project assignment remains unchanged. | Not set |
| `max_concurrency` | `int`                    | The maximum number of files processed at the same time (upload, job and download). Must be at least 1. | `10` |
| `return_exceptions` | `bool`                 | If `False`, the first failure cancels the remaining files and is raised. If `True`, every file is processed and the list holds a `KnowledgeGraph` or the exception for each file. | `False` |

Results are returned in the same order as `file_paths`. Each job's timeout (one hour) starts when the file gets a concurrency slot, and includes any time the job spends queued on the server.

To keep the graphs that succeeded when some files fail, use `return_exceptions=True`:

```python
results = client.build_graph(file_paths=paths, return_exceptions=True)
graphs = [r for r in results if isinstance(r, KnowledgeGraph)]
failed = [(p, r) for p, r in zip(paths, results) if isinstance(r, BaseException)]
```

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

| Parameter                      | Type                   | Description                                                                                                                                                                                                                               | Default                                          |
| ------------------------------ | ---------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| `kbs`                          | `List[KnowledgeGraph]` | A list of `KnowledgeGraph` objects to merge.                                                                                                                                                                                              |                                                  |
| `interlinking_key_uris`        | `List[str]`            | The URI of the property to use for linking entities (e.g., `rdfs:label`).                                                                                                                                                                 | `["http://www.w3.org/2000/01/rdf-schema#label"]` |
| `immutable_properties`         | `Optional[List[str]]`  | A list of property URIs (e.g., `"http://purl.org/dc/elements/1.1/title"` or `"hasJobTitle"`) that, if their values conflict between entities, will prevent those entities from being merged. Instead, separate entities will be retained. | `None`                                           |
| `merge_properties_on_conflict` | `bool`                 | If `True`, merges properties when a conflict occurs. Otherwise, keeps the first one.                                                                                                                                                      | `False`                                          |

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
