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
# For Neo4j support
pip install "perseus-client[neo4j]==1.0.0-rc.7"

# For FalkorDB support
pip install "perseus-client[falkordb]==1.0.0-rc.7"

# For both Neo4j and FalkorDB support
pip install "perseus-client[all]==1.0.0-rc.7"
```

## 🚀 Quick Start

To start using the SDK, you will need an API key from Lettria.

To create an API key, please visit our app [here](https://app.perseus.lettria.net/).

### Configuration

The SDK can be configured via environment variables. The `PerseusClient` will automatically load them. You can place them in a `.env` file in your project root.

| Variable              | Description                                | Required |
| --------------------- | ------------------------------------------ | -------- |
| `LETTRIA_API_KEY`     | Your unique API key for the Lettria API.   | Yes      |
| `NEO4J_URI`           | The URI for your Neo4j database instance.  | No       |
| `NEO4J_USER`          | The username for your Neo4j database.      | No       |
| `NEO4J_PASSWORD`      | The password for your Neo4j database.      | No       |
| `FALKORDB_HOST`       | The host for your FalkorDB instance.       | No       |
| `FALKORDB_PORT`       | The port for your FalkorDB (default 6379). | No       |
| `FALKORDB_GRAPH_NAME` | The name of the graph key to use.          | No       |
| `FALKORDB_PASSWORD`   | The password for your FalkorDB instance.   | No       |

### Example: Build a Graph

This example shows how to build a graph from a text file.

```python
import asyncio
from perseus_client import PerseusClient

async def main():
    async with PerseusClient() as client:
        try:
            await client.build_graph(
                file_path="path/to/your/document.txt",
            )
            print("🎉 Graph built successfully!")
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    asyncio.run(main())
```

## 📚 API Reference

### `client.build_graph`

```python
async def build_graph(
    file_path: str,
    ontology_path: Optional[str] = None,
    output_path: Optional[str] = None,
    save_to_neo4j: bool = False,
    save_to_falkordb: bool = False,
    refresh_graph: bool = False,
) -> Job:
```

Processes a file by uploading it, optionally with an ontology, running a job, and downloading the output.

| Parameter          | Type            | Description                                                                   | Default |
| ------------------ | --------------- | ----------------------------------------------------------------------------- | ------- |
| `file_path`        | `str`           | The path to the file to process.                                              |         |
| `ontology_path`    | `Optional[str]` | The path to the ontology file to use.                                         | `None`  |
| `output_path`      | `Optional[str]` | The path to save the output to. If not provided, a default path will be used. | `None`  |
| `save_to_neo4j`    | `bool`          | Whether to save the output to Neo4j.                                          | `False` |
| `save_to_falkordb` | `bool`          | Whether to save the output to FalkorDB.                                       | `False` |
| `refresh_graph`    | `bool`          | Whether to force a new job to be created (refresh the graph).                 | `False` |

## 📂 Examples

For more detailed examples, check out the [`examples/`](./examples/) directory. Each example has its own README with instructions.

### Simple Examples

- **[Build Graph](./examples/simple/build-graph/)**: Build a knowledge graph from a text file.
- **[Build Graph With Metadata](./examples/simple/build-graph-with-metadata/)**: Build a knowledge graph and add custom metadata to nodes and relationships.
- **[CQL Manipulation](./examples/simple/cql-manipulation/)**: Perform custom modifications on CQL query strings.
- **[File Operations](./examples/simple/file-operations/)**: Upload and manage files.
- **[Ontology Operations](./examples/simple/ontology-operations/)**: Upload and manage ontologies.
- **[TTL Manipulation](./examples/simple/ttl-manipulation/)**: Perform custom modifications on TTL (Turtle) files.
- **[Delete Operations](./examples/simple/delete-operations/)**: Delete files and ontologies.

### Advanced Example

- **[Graph RAG Reporting Neo4j](./examples/advanced/graph-rag-reporting-neo4j/)**: A complete workflow to turn a PDF into a knowledge graph and generate a report. Graph is saved in Neo4j.
- **[Graph RAG Reporting FalkorDB](./examples/advanced/graph-rag-reporting-falkordb/)**: A complete workflow to turn a PDF into a knowledge graph and generate a report. Graph is saved in FalkorDb.
- **[Finance Compliance](./examples/advanced/finance-compliance/)**: A complete pipeline to convert unstructured sustainability disclosures into a knowledge graph and produce CSRD-compliant reports.

## 🤝 Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.

## 📧 Contact

For support or questions, please reach out at `hello@lettria.com`.

## 📄 License

This SDK is licensed under the MIT License.
