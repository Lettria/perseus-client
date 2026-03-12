import os

def read_ttl_file(file_path: str) -> str:
    """Reads the content of a TTL file."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()
