"""
Data models for the Perseus client, designed for high-fidelity graph representation.
"""

from pydantic import BaseModel, PrivateAttr, Field
from datetime import datetime
from enum import Enum
import logging
from typing import Optional, List, Dict, Any, TYPE_CHECKING
import networkx as nx

# Use TYPE_CHECKING to avoid circular dependencies during runtime
if TYPE_CHECKING:
    from .services.neo4j_service import Neo4jService
    from .services.falkordb_service import FalkorDBService


class LiteralValue(BaseModel):
    """Represents a literal value with an optional datatype URI."""

    value: Any
    datatype: Optional[str] = None


class Entity(BaseModel):
    """Represents an entity with full URI and type information."""

    uri: str
    types: List[str] = Field(default_factory=list)
    # Key is the full predicate URI, value is the LiteralValue object
    properties: Dict[str, LiteralValue] = Field(default_factory=dict)


class Relation(BaseModel):
    """Represents a relation between two entities, including properties on the relation itself."""

    source_uri: str
    target_uri: str
    predicate: str  # The full URI of the relation type
    properties: Dict[str, LiteralValue] = Field(default_factory=dict)


class Document(BaseModel):
    """Represents a document from which the graph was extracted."""

    id: str
    content: str
    metadata: dict = {}


import asyncio


class KnowledgeGraph(BaseModel):
    """Represents a knowledge graph with high-fidelity RDF data."""

    entities: List[Entity] = Field(default_factory=list)
    relations: List[Relation] = Field(default_factory=list)
    documents: List[Document] = Field(default_factory=list)
    # Stores original prefixes, e.g., {'dbo': 'http://dbpedia.org/ontology/'}
    namespaces: Dict[str, str] = Field(default_factory=dict)

    # Original content from API
    ttl_content: Optional[str] = None
    cql_content: Optional[str] = None

    _neo4j_service: Optional["Neo4jService"] = PrivateAttr(default=None)
    _falkordb_service: Optional["FalkorDBService"] = PrivateAttr(default=None)

    def __init__(
        self,
        neo4j_service: Optional["Neo4jService"] = None,
        falkordb_service: Optional["FalkorDBService"] = None,
        **data,
    ):
        super().__init__(**data)
        self._neo4j_service = neo4j_service
        self._falkordb_service = falkordb_service

    def save_ttl(self, file_path: str):
        """
        Saves the KnowledgeGraph to a Turtle (TTL) file using high-fidelity serialization.
        Args:
            file_path: The path to save the TTL file to.
        """
        try:
            ttl_content = self.to_ttl()
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(ttl_content)
        except Exception as e:
            logging.error(f"Failed to save TTL to file {file_path}: {e}")

    def save_cql(self, file_path: str, strip_prefixes: bool = True):
        """
        Saves the KnowledgeGraph to a Cypher (CQL) file using high-fidelity serialization.

        Args:
            file_path: The path to save the CQL file to.
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties for a cleaner, more "native" Neo4j schema (e.g., `Person`, `label`).
                            If False, preserves prefixed names for higher fidelity (e.g., `dbo_Person`, `rdfs_label`).
        """
        try:
            cql_content = self.to_cql(strip_prefixes=strip_prefixes)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(cql_content)
        except Exception as e:
            logging.error(f"Failed to save CQL to file {file_path}: {e}")

    # ... (save_to_neo4j and save_to_falkordb methods remain the same for now) ...
    def save_to_neo4j(self, strip_prefixes: bool = True):
        """
        Synchronously saves the CQL content of the KnowledgeGraph to Neo4j.

        Args:
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties for a cleaner, more "native" Neo4j schema (e.g., `Person`, `label`).
                            If False, preserves prefixed names for higher fidelity (e.g., `dbo_Person`, `rdfs_label`).
        """
        try:
            asyncio.run(self.save_to_neo4j_async(strip_prefixes=strip_prefixes))
        except Exception as e:
            logging.error(f"Failed to save to Neo4j: {e}")

    async def save_to_neo4j_async(self, strip_prefixes: bool = True):
        """
        Asynchronously saves the CQL content of the KnowledgeGraph to Neo4j.

        Args:
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties for a cleaner, more "native" Neo4j schema (e.g., `Person`, `label`).
                            If False, preserves prefixed names for higher fidelity (e.g., `dbo_Person`, `rdfs_label`).
        """
        if not self._neo4j_service:
            logging.error(
                "Neo4j service not initialized for this KnowledgeGraph instance."
            )
            return
        try:
            cql_content = self.to_cql(strip_prefixes=strip_prefixes)
            if cql_content:
                await self._neo4j_service.execute_cql_string_async(cql_content)
            else:
                logging.warning("No CQL content to save to Neo4j.")
        except Exception as e:
            logging.error(f"Failed to generate or save CQL to Neo4j: {e}")

    def save_to_falkordb(self):
        """
        Synchronously saves the CQL content of the KnowledgeGraph to FalkorDB.
        """
        try:
            asyncio.run(self.save_to_falkordb_async())
        except Exception as e:
            logging.error(f"Failed to save to FalkorDB: {e}")

    async def save_to_falkordb_async(self):
        """
        Asynchronously saves the CQL content of the KnowledgeGraph to FalkorDB.
        """
        if not self._falkordb_service:
            logging.error(
                "FalkorDB service not initialized for this KnowledgeGraph instance."
            )
            return
        try:
            cql_content = self.to_cql()
            if cql_content:
                await self._falkordb_service.execute_cql_string_async(cql_content)
            else:
                logging.warning("No CQL content to save to FalkorDB.")
        except Exception as e:
            logging.error(f"Failed to generate or save CQL to FalkorDB: {e}")

    def to_ttl(self) -> str:
        """
        Serializes the KnowledgeGraph to a high-fidelity Turtle (TTL) string.
        """
        try:
            from rdflib import Graph, URIRef, Literal
            from rdflib.namespace import Namespace, RDF
        except ImportError:
            raise ImportError(
                "rdflib is required for TTL serialization. Please run `pip install perseus-client[rdf]`."
            )

        g = Graph()

        # Bind namespaces
        for prefix, uri in self.namespaces.items():
            g.bind(prefix, Namespace(uri))

        # Add triples from entities
        for entity in self.entities:
            entity_uri = URIRef(entity.uri)
            for entity_type in entity.types:
                g.add((entity_uri, RDF.type, URIRef(entity_type)))

            for predicate_uri, literal_value in entity.properties.items():
                literal_args = {}
                if literal_value.datatype:
                    literal_args["datatype"] = URIRef(literal_value.datatype)

                g.add(
                    (
                        entity_uri,
                        URIRef(predicate_uri),
                        Literal(literal_value.value, **literal_args),
                    )
                )

        # Add triples from relations
        for relation in self.relations:
            source = URIRef(relation.source_uri)
            predicate = URIRef(relation.predicate)
            target = URIRef(relation.target_uri)
            g.add((source, predicate, target))
            # Note: Properties on relations (reification) are not handled in this serialization
            # to keep it simpler. A full reification would create more complex structures.

        return g.serialize(format="turtle")

    def to_cql(self, strip_prefixes: bool = True) -> str:
        """
        Serializes the KnowledgeGraph to a Cypher Query Language (CQL) string.

        Args:
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
                # Check for special overrides first
                if uri in PREDICATE_MAP:
                    return f"`{PREDICATE_MAP[uri]}`"

                # Extract local name after the last '/' or '#'
                local_name = uri.split("/")[-1].split("#")[-1]

                # Basic sanitization for safety, though backticks handle most issues
                sanitized_name = local_name.replace(" ", "_").replace("-", "_")
                return f"`{sanitized_name}`"
            else:
                # High-fidelity version: use prefixes
                for prefix, ns_uri in self.namespaces.items():
                    if uri.startswith(ns_uri):
                        local_name = uri[len(ns_uri) :]
                        return f"`{prefix}_{local_name}`"
                # Fallback for URIs without a known prefix
                local_name = uri.split("/")[-1].split("#")[-1]
                return f"`{local_name}`"

        # Helper to format properties for a Cypher map
        def _properties_to_cql_map(properties: Dict[str, LiteralValue]) -> str:
            if not properties:
                return "{}"
            props = []
            for k_uri, v_obj in properties.items():
                key = _uri_to_cql_identifier(k_uri)
                # JSON-like string escaping for Cypher
                if isinstance(v_obj.value, str):
                    escaped_value = v_obj.value.replace("\\", "\\\\").replace(
                        "'", "\\'"
                    )
                    props.append(f"{key}: '{escaped_value}'")
                elif isinstance(v_obj.value, bool):
                    props.append(f"{key}: {str(v_obj.value).lower()}")
                else:  # Numbers, etc.
                    props.append(f"{key}: {v_obj.value}")
            return "{" + ", ".join(props) + "}"

        # Create MERGE statements for entities
        for entity in self.entities:
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
        for relation in self.relations:
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

    def to_json(self) -> dict:
        """Converts the knowledge graph to a JSON serializable dictionary."""
        return self.model_dump()

    @staticmethod
    def interlink(
        kbs: List["KnowledgeGraph"],
        interlinking_key_uri: str = "http://www.w3.org/2000/01/rdf-schema#label",
        immutable_properties: Optional[List[str]] = None,
    ) -> "KnowledgeGraph":
        """
        Merges multiple KnowledgeGraph objects into a single, deduplicated graph,
        with an option to prevent merging if certain properties conflict.

        Args:
            kbs: A list of KnowledgeGraph objects to merge.
            interlinking_key_uri: The URI of the property to use for deduplicating
                                  entities (e.g., rdfs:label).
            immutable_properties: A list of property URIs or simple names (e.g., "label")
                                  that must not conflict. If a duplicate entity has a
                                  conflicting value for one of these properties,
                                  it will not be merged. Defaults to a list of common
                                  name properties.
        Returns:
            A new, unified KnowledgeGraph.
        """
        if not kbs:
            return KnowledgeGraph()

        # By default, prevent merging entities with conflicting names.
        if immutable_properties is None:
            immutable_properties = ["name", "hasName", "hasFullName"]

        # Helper to resolve simple names to full URIs
        def _resolve_property_name(
            name: str, all_entities: List[Entity], namespaces: Dict[str, str]
        ) -> Optional[str]:
            if name.startswith("http://") or name.startswith("https://"):
                return name
            if ":" in name:
                prefix, local = name.split(":", 1)
                if prefix in namespaces:
                    return namespaces[prefix] + local

            # Search for a property URI that ends with the simple name
            for entity in all_entities:
                for prop_uri in entity.properties.keys():
                    if prop_uri.endswith(f"#{name}") or prop_uri.endswith(f"/{name}"):
                        return prop_uri
            logging.warning(f"Could not resolve property name '{name}' to a full URI.")
            return None

        # --- Main interlink logic ---

        # Initialize the new KG, inheriting services from the first one
        first_kg = kbs[0]
        merged_kg = KnowledgeGraph(
            neo4j_service=first_kg._neo4j_service,
            falkordb_service=first_kg._falkordb_service,
        )

        entity_map: Dict[str, Entity] = {}
        uri_redirects: Dict[str, str] = {}

        # Aggregate namespaces and entities from all KGs for resolution
        all_entities = [entity for kg in kbs for entity in kg.entities]
        for kg in kbs:
            merged_kg.namespaces.update(kg.namespaces)

        # Resolve immutable properties to full URIs
        resolved_immutable_properties = []
        if immutable_properties:
            for prop_name in immutable_properties:
                resolved_uri = _resolve_property_name(
                    prop_name, all_entities, merged_kg.namespaces
                )
                if resolved_uri:
                    resolved_immutable_properties.append(resolved_uri)

        # 1. Iterate through all entities to identify duplicates and merge them
        for kg in kbs:
            for entity in kg.entities:
                key_value = None
                if interlinking_key_uri in entity.properties:
                    key_value = str(entity.properties[interlinking_key_uri].value)

                if key_value is None:
                    key_value = entity.uri

                if key_value in entity_map:
                    existing_entity = entity_map[key_value]

                    # --- Conflict Check for Immutable Properties ---
                    has_conflict = False
                    for prop_uri in resolved_immutable_properties:
                        if (
                            prop_uri in existing_entity.properties
                            and prop_uri in entity.properties
                            and existing_entity.properties[prop_uri].value
                            != entity.properties[prop_uri].value
                        ):
                            logging.warning(
                                f"Merge conflict on immutable property '{prop_uri}' for "
                                f"entity with key '{key_value}'. Values: "
                                f"'{existing_entity.properties[prop_uri].value}' vs "
                                f"'{entity.properties[prop_uri].value}'. "
                                f"Entity '{entity.uri}' will not be merged."
                            )
                            has_conflict = True
                            break

                    if has_conflict:
                        merged_kg.entities.append(entity)
                        continue
                    # --- End of Conflict Check ---

                    uri_redirects[entity.uri] = existing_entity.uri

                    for prop_uri, prop_value in entity.properties.items():
                        if prop_uri not in existing_entity.properties:
                            existing_entity.properties[prop_uri] = prop_value

                    for type_uri in entity.types:
                        if type_uri not in existing_entity.types:
                            existing_entity.types.append(type_uri)
                else:
                    entity_map[key_value] = entity
                    merged_kg.entities.append(entity)

        # 2. Iterate through all relations and relink them
        for kg in kbs:
            for relation in kg.relations:
                if relation.source_uri in uri_redirects:
                    source_uri = uri_redirects[relation.source_uri]
                else:
                    source_uri = relation.source_uri

                if relation.target_uri in uri_redirects:
                    target_uri = uri_redirects[relation.target_uri]
                else:
                    target_uri = relation.target_uri

                if not any(
                    r.source_uri == source_uri
                    and r.target_uri == target_uri
                    and r.predicate == relation.predicate
                    for r in merged_kg.relations
                ):
                    updated_relation = relation.model_copy()
                    updated_relation.source_uri = source_uri
                    updated_relation.target_uri = target_uri
                    merged_kg.relations.append(updated_relation)

        return merged_kg


class FileStatus(str, Enum):
    """Enumeration for file statuses."""

    PENDING = "pending"
    UPLOADED = "uploaded"
    FAILED = "failed"


class File(BaseModel):
    """Represents a file object returned by the API."""

    id: str
    name: str
    status: FileStatus
    created_at: datetime


class OntologyStatus(str, Enum):
    """Enumeration for file statuses."""

    PENDING = "pending"
    UPLOADED = "uploaded"
    FAILED = "failed"


class Ontology(BaseModel):
    """Represents a file object returned by the API."""

    id: str
    name: str
    status: OntologyStatus
    created_at: datetime


class JobStatus(str, Enum):
    """Enumeration for job statuses."""

    PENDING = "PENDING"
    RUNNABLE = "RUNNABLE"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"


class Job(BaseModel):
    """Represents a job object returned by the API."""

    id: str
    status: JobStatus
    stopped: bool = False
