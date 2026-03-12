# Add Metadata to Neo4j Example

This example demonstrates how to add metadata (e.g., the source file name) to nodes and relationships before saving a graph to a Neo4j database using the Perseus Client.

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- An active Lettria API key

## Setup

1. **Clone the repository:**

   ```bash
   git clone https://gitlab.ops.lettria.net/text-to-graph/perseus-client.git
   cd perseus-client/examples/advanced/add-metadata-neo4j
   ```

2. **Set up the environment:**
   - Create a `.env` file from the template:
     ```bash
     cp template.env .env
     ```
   - Edit the `.env` file and add your `PERSEUS_API_KEY`.

3. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

4. **Start the Neo4j database:**
   ```bash
   docker-compose up -d
   ```

## Usage

1. **Run the script:**

   ```bash
   source .env
   python add_metadata.py assets/sample.txt
   ```

2. **Verify the results:**
   - Open the Neo4j Browser at `http://localhost:7474`.
   - Connect to the database using the credentials from your `.env` file (e.g., `neo4j`/`password`).
   - Run the following Cypher query to inspect the nodes and relationships:
     ```cypher
     MATCH (n) RETURN n
     ```
   - You should see the `source_file` property on the nodes and relationships.

## Cleanup

To stop and remove the Neo4j container, run:

```bash
docker-compose down
```
