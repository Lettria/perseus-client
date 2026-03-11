# Build Graph Example 🏗️

This script shows you how to generate a knowledge graph from a text file and an ontology using the Perseus client.

## Quick Start 🚀

1.  **Setup Environment**:

    - Python 3.8+ is required. 🐍
    - Copy `template.env` to `.env` and fill your credentials.

    ```bash
    cp template.env .env
    # Edit .env with your credentials
    ```

2.  **Install Dependencies**:

    ```bash
    pip install -r requirements.txt
    ```

3.  **Run Example**:

    ```bash
    python build_graph.py
    ```

    This will create a knowledge graph from `assets/pizza.txt` and `assets/pizza.ttl`, saving the output to `./output/graph.ttl`. 🍕
