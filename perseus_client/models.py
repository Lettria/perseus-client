# SPDX-FileCopyrightText: 2023-present Your Name <you@example.com>
#
# SPDX-License-Identifier: MIT
"""
Data models for the Perseus client.
"""
from pydantic import BaseModel, PrivateAttr
from datetime import datetime
from enum import Enum
import logging
from typing import Optional, List, TYPE_CHECKING

import networkx as nx
from rdflib import Graph

# Use TYPE_CHECKING to avoid circular dependencies during runtime
if TYPE_CHECKING:
    from .services.neo4j_service import Neo4jService
    from .services.falkordb_service import FalkorDBService


class Entity(BaseModel):
    """Represents an entity in the knowledge graph."""

    id: str
    label: str
    properties: dict = {}


class Relation(BaseModel):
    """Represents a relation between two entities in the knowledge graph."""

    source: str
    target: str
    type: str
    properties: dict = {}


class Document(BaseModel):
    """Represents a document from which the graph was extracted."""

    id: str
    content: str
    metadata: dict = {}


import asyncio

class KnowledgeGraph(BaseModel):
    """Represents a knowledge graph extracted from documents."""

    entities: List[Entity] = []
    relations: List[Relation] = []
    documents: List[Document] = []
    ttl_content: Optional[str] = None
    cql_content: Optional[str] = None

    _neo4j_service: Optional["Neo4jService"] = PrivateAttr(default=None)
    _falkordb_service: Optional["FalkorDBService"] = PrivateAttr(default=None)

    def __init__(
        self, 
        neo4j_service: Optional["Neo4jService"] = None, 
        falkordb_service: Optional["FalkorDBService"] = None, 
        **data
    ):
        super().__init__(**data)
        self._neo4j_service = neo4j_service
        self._falkordb_service = falkordb_service

    def save_ttl(self, file_path: str):
        """
        Saves the KnowledgeGraph to a Turtle (TTL) file.
        Args:
            file_path: The path to save the TTL file to.
        """
        try:
            ttl_content = self.to_ttl()
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(ttl_content)
        except Exception as e:
            logging.error(f"Failed to save TTL to file {file_path}: {e}")

    def save_cql(self, file_path: str):
        """
        Saves the KnowledgeGraph to a Cypher (CQL) file.
        Args:
            file_path: The path to save the CQL file to.
        """
        try:
            cql_content = self.to_cql()
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(cql_content)
        except Exception as e:
            logging.error(f"Failed to save CQL to file {file_path}: {e}")

    def save_to_neo4j(self):
        """
        Synchronously saves the CQL content of the KnowledgeGraph to Neo4j.
        """
        try:
            asyncio.run(self.save_to_neo4j_async())
        except Exception as e:
            logging.error(f"Failed to save to Neo4j: {e}")

    async def save_to_neo4j_async(self):
        """
        Asynchronously saves the CQL content of the KnowledgeGraph to Neo4j.
        """
        if not self._neo4j_service:
            logging.error("Neo4j service not initialized for this KnowledgeGraph instance.")
            return
        try:
            cql_content = self.to_cql()
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
            logging.error("FalkorDB service not initialized for this KnowledgeGraph instance.")
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
        Converts the knowledge graph to a Turtle (TTL) string representation.
        """
        from rdflib import Graph, URIRef, Literal, Namespace, RDF, RDFS
        from urllib.parse import quote
        
        g = Graph()

        # Define a base URI for entities and relations if not explicitly full URIs
        base_uri = Namespace("http://example.com/perseus/")
        g.bind("perseus", base_uri)

        for entity in self.entities:
            entity_uri = URIRef(entity.id)
            # URL-encode the label for use in a URI
            g.add((entity_uri, RDF.type, base_uri[quote(entity.label)]))
            g.add((entity_uri, RDFS.label, Literal(entity.label)))
            for prop_key, prop_value in entity.properties.items():
                prop_uri = base_uri[quote(prop_key)]
                g.add((entity_uri, prop_uri, Literal(prop_value)))

        for relation in self.relations:
            source_uri = URIRef(relation.source)
            target_uri = URIRef(relation.target)
            # URL-encode the relation type for use in a URI
            relation_uri = base_uri[quote(relation.type)]
            g.add((source_uri, relation_uri, target_uri))
            # Omitting relation properties for now to avoid complex reification
        
        return g.serialize(format="turtle")

    def to_rdflib(self) -> Graph:
        """
        Converts the knowledge graph to an RDFLib Graph.
        """
        from rdflib import Graph, URIRef, Literal, Namespace, RDF, RDFS
        from urllib.parse import quote
        
        g = Graph()

        # Define a base URI for entities and relations if not explicitly full URIs
        base_uri = Namespace("http://example.com/perseus/")
        g.bind("perseus", base_uri)

        for entity in self.entities:
            entity_uri = URIRef(entity.id)
            # URL-encode the label for use in a URI
            g.add((entity_uri, RDF.type, base_uri[quote(entity.label)]))
            g.add((entity_uri, RDFS.label, Literal(entity.label)))
            for prop_key, prop_value in entity.properties.items():
                prop_uri = base_uri[quote(prop_key)]
                g.add((entity_uri, prop_uri, Literal(prop_value)))

        for relation in self.relations:
            source_uri = URIRef(relation.source)
            target_uri = URIRef(relation.target)
            # URL-encode the relation type for use in a URI
            relation_uri = base_uri[quote(relation.type)]
            g.add((source_uri, relation_uri, target_uri))
            # Omitting relation properties for simplicity.
        
        return g

    def to_networkx(self) -> nx.Graph:
        """Converts the knowledge graph to a NetworkX Graph."""
        # Placeholder for implementation
        pass

    def to_cql(self) -> str:
        """
        Converts the knowledge graph to a Cypher Query Language (CQL) string representation.
        """
        cql_statements = []

        def _properties_to_cql_map(properties: dict) -> str:
            if not properties:
                return ""
            props = []
            for k, v in properties.items():
                if isinstance(v, str):
                    props.append(f"{k}: \'{v}'")
                else:
                    props.append(f"{k}: {v}")
            return "{\"" + ", ".join(props) + "\"}"

        # Create or merge nodes
        for entity in self.entities:
            props_str = _properties_to_cql_map({"id": entity.id, **entity.properties})
            cql_statements.append(f"MERGE (n:{entity.label} {props_str})")

        # Create or merge relationships
        for relation in self.relations:
            # Need to match source and target nodes first
            cql_statements.append(
                f"MATCH (source_node {{id: \'{relation.source}'}}), "
                f"(target_node {{id: \'{relation.target}'}})"
            )
            props_str = _properties_to_cql_map(relation.properties)
            if props_str:
                cql_statements.append(
                    f"MERGE (source_node)-[r:{relation.type} {props_str}]->(target_node)"
                )
            else:
                cql_statements.append(
                    f"MERGE (source_node)-[r:{relation.type}]->(target_node)"
                )

        return ";\n".join(cql_statements) + ";"

    def to_json(self) -> dict:
        """Converts the knowledge graph to a JSON serializable dictionary."""
        return self.model_dump()


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
