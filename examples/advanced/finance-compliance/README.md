# Finance Compliance Example: From Unstructured Reports to Queryable Knowledge

This project demonstrates how to transform unstructured sustainability and regulatory disclosures into a structured, queryable knowledge graph. Using an ontology-guided approach, we extract both quantitative metrics and narrative context from corporate reports, enabling automated analysis and compliance verification.

## The Challenge: The Limits of Unstructured Data

Regulatory frameworks like CSRD (Corporate Sustainability Reporting Directive) are pushing for more machine-readable disclosures. However, much of the critical information in reports remains unstructured—locked away in PDFs, tables, and narrative text. This makes automated analysis difficult and compliance checking a manual, time-consuming process.

## The Solution: Ontology-Guided Extraction

This project uses the Perseus Text-to-Graph engine, which can be tailored with a domain-specific ontology (`assets/ontology_csrd.ttl`). The ontology ensures that the extracted data is semantically normalized, consistently structured, and linked to its original context, making it ready for reliable, automated analysis.

## The Workflow

This example is split into three main scripts that should be run in order:

1.  **`index.py`**: Builds the Knowledge Graph. It takes a source document (e.g., `assets/ecosteel_annual_report.md`), uses the `ontology_csrd.ttl` to guide the Perseus engine, saves the resulting graph as a local `.ttl` file, and loads it into a Neo4j database.
2.  **`explore.py`**: Explores the Extracted Data. It reads the local `.ttl` files and displays a summary of the extracted entities, showing what the ontology-guided process found in the documents.
3.  **`compliance.py`**: Verifies Compliance. It connects to the Neo4j database and runs a series of Cypher queries to automatically check for key indicators of ESRS E1 compliance, outputting a scorecard for each company.

## How to Run

### 1. Setup Environment

- Requires Docker, Docker Compose, and Python 3.8+.
- Copy `template.env` to `.env` and fill in your `PERSEUS_API_KEY`.
  ```bash
  cp template.env .env
  ```

### 2. Install Dependencies & Start Services

```bash
pip install -r requirements.txt
docker compose up -d
```

### 3. Run the Full Workflow

Execute the scripts in order, providing the path to the documents you want to process.

1.  **Build the Knowledge Graphs:**

    ```bash
    python index.py assets/ecosteel_annual_report.md
    python index.py assets/techgreen_press_release.md
    ```

2.  **Explore the Extracted Data from the TTL files:**

    ```bash
    python explore.py
    ```

3.  **Verify ESRS E1 Compliance from the data in Neo4j:**
    ```bash
    python compliance.py
    ```

### 4. Cleaning Up

When you're done, stop and remove the Docker services:

```bash
docker-compose down
```

## Expected Compliance Results

The verification script will show that **EcoSteel Industries meets all 5 ESRS E1 indicators (100%)** while **TechGreen Solutions meets 3 out of 5 indicators (60%)**, demonstrating how the system can automatically score compliance based on the level of detail in the source documents.
