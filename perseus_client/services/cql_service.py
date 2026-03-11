from typing import Dict, Any
import re


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
