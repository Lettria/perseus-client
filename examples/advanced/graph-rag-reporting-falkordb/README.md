# Graph RAG Reporting Example 📊

This guide demonstrates how to leverage the Perseus client for building a powerful Graph RAG (Retrieval Augmented Generation) reporting application. You'll learn to convert a PDF document into a Markdown file using an LLM, construct a rich knowledge graph from its content, and then use this graph to generate insightful, context-aware reports.

## Quick Start 🚀

1.  **Setup Environment**:

    - Requires Docker, Docker Compose, and Python 3.8+. 🐳🐍
    - Copy `template.env` to `.env` and fill your credentials.

    ```bash
    cp template.env .env
    ```

2.  **Install Dependencies & Start Services**:

    ```bash
    pip install -r requirements.txt
    docker compose up -d
    ```

> ⏳ The embedder service may take a few minutes to fully boot on first run, as it needs to download the underlying model.

3.  **Run the Workflow**:

    - **Convert PDF to Markdown**:

      ```bash
      python pdf_to_markdown.py assets/LOREAL_Rapport_Annuel_2024.pdf
      ```

    - **Index the Document**:

      ```bash
      python index.py assets/LOREAL_Rapport_Annuel_2024.md
      ```

    - **Generate a Report**:
      ```bash
      python report.py "What are the main activities of L'Oréal?"
      ```

## Cleaning Up 🧹

When you're done, stop and remove the Docker containers:

```bash
docker compose down
```
