from typing import Dict, Any
import re
from datetime import date, datetime
from ..models import KnowledgeGraph, LiteralValue
import logging


class CQLService:
    """
    A service for manipulating Cypher Query Language (CQL) strings.
    """

    def add_metadata_to_cql(self, cql_query: str, metadata: Dict[str, Any]) -> str:
        """
        Adds a dictionary of metadata to nodes and relationships in a CQL query.

        Args:
            cql_query: The CQL query string.
            metadata: A dictionary where keys are metadata property names and values are their values.

        Returns:
            The modified CQL query string.
        """
        modified_lines = []
        metadata_properties_str = ", ".join(
            f"{key}: '{value}'" for key, value in metadata.items()
        )

        for line in cql_query.split(";"):
            line = line.strip()
            if not line:
                continue

            # Case 1: Node MERGE (e.g., "MERGE (n:Label {prop: 'val'})")
            if line.startswith("MERGE ("):
                if "{" in line:
                    modified_line = line.replace(
                        "{", f"{{{metadata_properties_str}, ", 1
                    )
                else:
                    modified_line = line.replace(")", f" {{{metadata_properties_str}}})", 1)
                modified_lines.append(modified_line)

            # Case 2: Relationship MERGE (e.g., "MATCH... MERGE (a)-[r:REL]->(b)")
            elif line.startswith("MATCH"):

                def modify_relationship(match):
                    rel_def = match.group(0)
                    if "{" in rel_def:
                        return rel_def.replace(
                            "{", f"{{{metadata_properties_str}, ", 1
                        )
                    else:
                        return rel_def.replace("]", f" {{{metadata_properties_str}}}]", 1)

                rel_pattern = re.compile(r"\[.*?\]")
                modified_line = rel_pattern.sub(modify_relationship, line, count=1)
                modified_lines.append(modified_line)

            else:
                modified_lines.append(line)

        return ";\n".join(modified_lines) + ";"

    def to_cql(self, kg: KnowledgeGraph, strip_prefixes: bool = True) -> str:
        """
        Serializes the KnowledgeGraph to a Cypher Query Language (CQL) string.

        Args:
            kg: The KnowledgeGraph object to serialize.
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties for a cleaner, more "native" Neo4j schema (e.g., `Person`, `label`).
                            If False, preserves prefixed names for higher fidelity (e.g., `dbo_Person`, `rdfs_label`).
        """
        cql_statements = []

        # Special mapping for common RDF properties to cleaner Neo4j properties
        PREDICATE_MAP = {
            "http://www.w3.org/2000/01/rdf-schema#label": "label",
            "http://xmlns.com/foaf/0.1/name": "name",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": "type",
        }

        # Helper to convert a URI to a clean, simple, and safe CQL identifier
        def _uri_to_cql_identifier(uri: str) -> str:
            if strip_prefixes:
                if uri in PREDICATE_MAP:
                    return f"`{PREDICATE_MAP[uri]}`"
                for prefix, ns_uri in kg.namespaces.items():
                    if uri.startswith(ns_uri):
                        local_name = uri[len(ns_uri):]
                        return f"`{local_name}`"
                local_name = uri.split("/")[-1].split("#")[-1]
                sanitized_name = local_name.replace(" ", "_").replace("-", "_")
                return f"`{sanitized_name}`"
            else:
                for prefix, ns_uri in kg.namespaces.items():
                    if uri.startswith(ns_uri):
                        local_name = uri[len(ns_uri) :]
                        return f"`{prefix}_{local_name}`"
                local_name = uri.split("/")[-1].split("#")[-1]
                return f"`{local_name}`"

        # Helper to format properties for a Cypher map
        def _properties_to_cql_map(properties: Dict[str, LiteralValue]) -> str:
            if not properties:
                return "{}"
            props = []

            # Helper to format a single value for Cypher
            def _format_value(value: Any) -> str:
                if value is None:
                    return "null"
                if isinstance(value, (date, datetime)):
                    return f"'{value.isoformat()}'"
                if isinstance(value, str):
                    escaped_value = value.replace("\\", "\\\\").replace("'", "\\'")
                    return f"'{escaped_value}'"
                elif isinstance(value, bool):
                    return str(value).lower()
                else:  # Numbers, etc.
                    return str(value)

            for k_uri, v_obj in properties.items():
                key = _uri_to_cql_identifier(k_uri)

                if isinstance(v_obj.value, list):
                    # Format as a Cypher list
                    list_items = [_format_value(item) for item in v_obj.value]
                    props.append(f"{key}: [{', '.join(list_items)}]")
                else:
                    # Format as a single value
                    props.append(f"{key}: {_format_value(v_obj.value)}")

            return "{" + ", ".join(props) + "}"

        # Create MERGE statements for entities
        for entity in kg.entities:
            # Use 'Resource' as a fallback label if no types are specified
            labels = (
                ":".join([_uri_to_cql_identifier(t) for t in entity.types if t])
                or "`Resource`"
            )

            props_map = _properties_to_cql_map(entity.properties)

            # Combine MERGE and SET into a single statement for atomicity
            merge_clause = f"MERGE (n:{labels} {{uri: '{entity.uri}'}})"
            set_clause = f"SET n += {props_map}"

            if entity.properties:
                cql_statements.append(f"{merge_clause}\n{set_clause}")
            else:
                cql_statements.append(merge_clause)

        # Create MERGE statements for relationships
        for relation in kg.relations:
            rel_type = _uri_to_cql_identifier(relation.predicate)
            props_map = _properties_to_cql_map(relation.properties)

            # Efficiently MERGE source and target nodes first to avoid cartesian products
            match_clause = (
                f"MATCH (source_node {{uri: '{relation.source_uri}'}})\n"
                f"MATCH (target_node {{uri: '{relation.target_uri}'}})"
            )

            merge_clause = f"MERGE (source_node)-[r:{rel_type}]->(target_node)"

            # Combine into a single statement and add properties if they exist
            if relation.properties:
                set_clause = f"SET r += {props_map}"
                cql_statements.append(f"{match_clause}\n{merge_clause}\n{set_clause}")
            else:
                cql_statements.append(f"{match_clause}\n{merge_clause}")

        return ";\n".join(cql_statements) + ";"

    def save_cql(self, kg: KnowledgeGraph, file_path: str, strip_prefixes: bool = True):
        """
        Saves the KnowledgeGraph to a Cypher (CQL) file using high-fidelity serialization.

        Args:
            kg: The KnowledgeGraph object to save.
            file_path: The path to save the CQL file to.
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties for a cleaner, more "native" Neo4j schema (e.g., `Person`, `label`).
                            If False, preserves prefixed names for higher fidelity (e.g., `dbo_Person`, `rdfs_label`).
        """
        try:
            cql_content = self.to_cql(kg, strip_prefixes=strip_prefixes)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(cql_content)
        except Exception as e:
            logging.error(f"Failed to save CQL to file {file_path}: {e}")
