from typing import Dict, Any, List, Optional
import json
from datetime import date, datetime
from perseus_client.config import settings
from ..models import KnowledgeGraph, LiteralValue
from .cql_service import (
    uri_to_identifier,
    coerce_literal,
    extract_uuid,
    split_cypher_statements,
)
import logging

# Name of the column holding the dynamic label. It must not be called `label`:
# with dynamic properties, a column hides the JSON property of the same name,
# and `rdfs:label` is mapped to the `label` property.
LABEL_COLUMN = "kind"

# Spanner Graph has a single dynamic label per node: this one is used when an entity has no type.
DEFAULT_LABEL = "Resource"


def split_spgl_statements(spgl_query: str) -> List[str]:
    """
    Split a Spanner Graph (GoogleSQL DML) script into individual statements, respecting quoted strings.

    GoogleSQL uses the same single-quoted, backslash-escaped strings as the generated Cypher,
    so the Cypher splitter is reused.
    """
    return split_cypher_statements(spgl_query)


class SPGLService:
    """
    A service for generating Spanner Graph statements (SPGL) from a KnowledgeGraph.

    Spanner Graph queries (GQL / openCypher) are read-only: writes are GoogleSQL DML on the
    tables behind the property graph. The generated statements reproduce the Cypher produced by
    CQLService, on a schemaless graph (one node table, one edge table, dynamic label and properties):

    - `MERGE (n:Label {uuid: ...}) SET n += {...}` becomes `INSERT OR IGNORE` on the node table,
      followed by an `UPDATE` merging the properties with `JSON_SET`.
    - `MATCH (source) MATCH (target) MERGE (source)-[r:REL]->(target) SET r += {...}` becomes
      `INSERT OR IGNORE ... SELECT` from the two nodes (so, like `MATCH`, nothing is written when
      a node is missing), followed by an `UPDATE` merging the properties.
    """

    def __init__(
        self, graph_name: Optional[str] = None, table_prefix: Optional[str] = None
    ):
        self.graph_name = graph_name or settings.spanner_graph_name
        prefix = table_prefix if table_prefix is not None else settings.spanner_table_prefix
        self.node_table = f"{prefix}Node"
        self.edge_table = f"{prefix}Edge"

    def schema_ddl(self) -> List[str]:
        """
        Returns the DDL statements creating the tables and the schemaless property graph.
        All statements use `IF NOT EXISTS`, so they can be run on an existing database.
        """
        return [
            f"""CREATE TABLE IF NOT EXISTS {self.node_table} (
  id STRING(MAX) NOT NULL,
  {LABEL_COLUMN} STRING(MAX) NOT NULL,
  types ARRAY<STRING(MAX)>,
  properties JSON,
) PRIMARY KEY (id)""",
            f"""CREATE TABLE IF NOT EXISTS {self.edge_table} (
  id STRING(MAX) NOT NULL,
  dest_id STRING(MAX) NOT NULL,
  {LABEL_COLUMN} STRING(MAX) NOT NULL,
  properties JSON,
) PRIMARY KEY (id, dest_id, {LABEL_COLUMN}),
  INTERLEAVE IN PARENT {self.node_table}""",
            f"""CREATE PROPERTY GRAPH IF NOT EXISTS {self.graph_name}
  NODE TABLES ({self.node_table} KEY (id)
    DYNAMIC LABEL ({LABEL_COLUMN}) DYNAMIC PROPERTIES (properties))
  EDGE TABLES ({self.edge_table} KEY (id, dest_id, {LABEL_COLUMN})
    SOURCE KEY (id) REFERENCES {self.node_table} (id)
    DESTINATION KEY (dest_id) REFERENCES {self.node_table} (id)
    DYNAMIC LABEL ({LABEL_COLUMN}) DYNAMIC PROPERTIES (properties))""",
        ]

    def add_metadata_to_spgl(self, spgl_query: str, metadata: Dict[str, Any]) -> str:
        """
        Placeholder: adds a dictionary of metadata to nodes and relationships in an SPGL script.

        Not needed by build_graph: metadata is added to the TTL before it is parsed, so the
        KnowledgeGraph passed to `to_spgl` already holds it.
        """
        raise NotImplementedError(
            "add_metadata_to_spgl is not implemented yet. Metadata passed to build_graph "
            "is already part of the KnowledgeGraph, and so of the output of to_spgl."
        )

    def to_spgl(self, kg: KnowledgeGraph, strip_prefixes: bool = True) -> str:
        """
        Serializes the KnowledgeGraph to Spanner Graph statements (GoogleSQL DML).

        Labels, property keys and UUIDs follow the same rules as CQLService.to_cql.

        Args:
            kg: The KnowledgeGraph object to serialize.
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties (e.g., `Person`, `label`).
                            If False, preserves prefixed names (e.g., `dbo_Person`, `rdfs_label`).
        """
        statements = []

        def _identifier(uri: str) -> str:
            return uri_to_identifier(uri, kg.namespaces, strip_prefixes)

        # Helper to escape strings for GoogleSQL single-quoted literals
        def _escape_sql_string(value: str) -> str:
            """Escape backslashes, single quotes and line breaks for use in GoogleSQL strings."""
            return (
                value.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
            )

        def _sql_string(value: str) -> str:
            return f"'{_escape_sql_string(value)}'"

        def _to_json_value(value: Any) -> Any:
            # JSON has no date type
            if isinstance(value, (date, datetime)):
                return value.isoformat()
            if isinstance(value, list):
                return [_to_json_value(v) for v in value]
            return value

        def _json_literal(value: Any) -> str:
            return f"JSON {_sql_string(json.dumps(value, ensure_ascii=False))}"

        # Helper to build the `JSON_SET` call merging properties into the existing ones (`SET x += {...}`)
        def _json_set(properties: Dict[str, LiteralValue]) -> str:
            args = []
            for k_uri, v_obj in properties.items():
                # Quoted JSONPath member, so keys with dots or spaces stay a single key
                key = _identifier(k_uri).replace("\\", "\\\\").replace('"', '\\"')
                path = _sql_string('$."' + key + '"')
                value = _to_json_value(coerce_literal(v_obj))
                args.append(f"{path}, {_json_literal(value)}")
            return f"JSON_SET(COALESCE(properties, JSON '{{}}'), {', '.join(args)})"

        # Create upsert statements for entities
        for entity in kg.entities:
            # Spanner Graph has a single dynamic label per node. Types are sorted because their
            # order isn't stable across parses, and the label must not change between saves.
            types = sorted({_identifier(t) for t in entity.types if t}) or [DEFAULT_LABEL]
            types_array = "[" + ", ".join(_sql_string(t) for t in types) + "]"

            # Extract UUID from URI to use as key (matching server behavior)
            node_id = _sql_string(extract_uuid(entity.uri))

            statements.append(
                f"INSERT OR IGNORE INTO {self.node_table} (id, {LABEL_COLUMN}, types, properties)\n"
                f"VALUES ({node_id}, {_sql_string(types[0])}, {types_array}, JSON '{{}}')"
            )
            if entity.properties:
                statements.append(
                    f"UPDATE {self.node_table}\n"
                    f"SET properties = {_json_set(entity.properties)}\n"
                    f"WHERE id = {node_id}"
                )

        # Create upsert statements for relationships
        for relation in kg.relations:
            kind = _sql_string(_identifier(relation.predicate))
            source_id = _sql_string(extract_uuid(relation.source_uri))
            target_id = _sql_string(extract_uuid(relation.target_uri))

            # Selecting from both nodes skips the edge if one is missing, like Cypher's MATCH.
            # Otherwise a missing source would make Spanner reject the transaction (the edge
            # table is interleaved in the node table), and a missing target would leave a
            # dangling edge (dest_id isn't a foreign key).
            statements.append(
                f"INSERT OR IGNORE INTO {self.edge_table} (id, dest_id, {LABEL_COLUMN}, properties)\n"
                f"SELECT source_node.id, target_node.id, {kind}, JSON '{{}}'\n"
                f"FROM {self.node_table} AS source_node, {self.node_table} AS target_node\n"
                f"WHERE source_node.id = {source_id} AND target_node.id = {target_id}"
            )
            if relation.properties:
                statements.append(
                    f"UPDATE {self.edge_table}\n"
                    f"SET properties = {_json_set(relation.properties)}\n"
                    f"WHERE id = {source_id} AND dest_id = {target_id} AND {LABEL_COLUMN} = {kind}"
                )

        return ";\n".join(statements) + ";"

    def save_spgl(self, kg: KnowledgeGraph, file_path: str, strip_prefixes: bool = True):
        """
        Saves the KnowledgeGraph to a Spanner Graph statements (SPGL) file.

        Args:
            kg: The KnowledgeGraph object to save.
            file_path: The path to save the SPGL file to.
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties (e.g., `Person`, `label`).
                            If False, preserves prefixed names (e.g., `dbo_Person`, `rdfs_label`).
        """
        try:
            spgl_content = self.to_spgl(kg, strip_prefixes=strip_prefixes)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(spgl_content)
        except Exception as e:
            logging.error(f"Failed to save SPGL to file {file_path}: {e}")
