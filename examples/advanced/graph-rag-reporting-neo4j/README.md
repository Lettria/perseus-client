# Graph RAG Reporting with Neo4j

This example demonstrates how to build a powerful Graph RAG (Retrieval Augmented Generation) application using the Perseus Client and Neo4j. The workflow involves converting a PDF document to Markdown, building a knowledge graph from the content, storing it in Neo4j, and then using this graph to generate insightful, context-aware reports with a Large Language Model (LLM).

## The Workflow

The process is divided into three main scripts:

1.  **`pdf_to_markdown.py`**: Converts a PDF document into a Markdown file using an LLM. This makes the text content easily accessible for graph extraction.
2.  **`index.py`**: This is the core indexing script. It takes the Markdown file, uses the Perseus Client to build a rich knowledge graph, loads this graph into Neo4j, and then indexes the graph's content for efficient retrieval.
3.  **`report.py`**: This script takes a user's question, retrieves the most relevant sub-graph from Neo4j, and then passes this context to an LLM to generate a comprehensive and accurate report.

## How to Run

### 1. Setup Environment

- Requires Docker, Docker Compose, and Python 3.8+.
- Copy `template.env` to `.env` and fill in your credentials for the Perseus API and Google AI (for the LLM).

  ```bash
  cp template.env .env
  ```

  You will need to fill in the following variables in your new `.env` file:
  - `PERSEUS_API_KEY`
  - `GEMINI_API_KEY`

### 2. Install Dependencies & Start Services

```bash
pip install -r requirements.txt
docker compose up -d
```

> **Note:** The embedder service may take a few minutes to boot on the first run as it needs to download the embedding model.

### 3. Run the Full Workflow

Execute the scripts in order.

1.  **Convert the PDF to Markdown:**

    > **Note:** The example already includes the converted Markdown file (`LOREAL_Rapport_Annuel_2024.md`) in the `assets` folder. You can skip this step if you want to use the provided file.

    ```bash
    python pdf_to_markdown.py assets/LOREAL_Rapport_Annuel_2024.pdf
    ```

2.  **Build and Index the Knowledge Graph:**

    ```bash
    python index.py assets/LOREAL_Rapport_Annuel_2024.md
    ```

3.  **Generate a Report from the Graph:**
    ```bash
    python report.py "What are the main activities of L'Oréal?"
    ```

### 4. Cleaning Up

When you are finished, stop and remove the Docker containers:

```bash
docker compose down
```
