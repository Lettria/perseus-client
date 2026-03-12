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
        Saves the TTL content of the KnowledgeGraph to a file.
        Args:
            file_path: The path to save the TTL file to.
        """
        if self.ttl_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(self.ttl_content)
        else:
            logging.warning("No TTL content to save.")

    def save_cql(self, file_path: str):
        """
        Saves the CQL content of the KnowledgeGraph to a file.
        Args:
            file_path: The path to save the CQL file to.
        """
        if self.cql_content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(self.cql_content)
        else:
            logging.warning("No CQL content to save.")

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
        if self.cql_content:
            await self._neo4j_service.execute_cql_string_async(self.cql_content)
        else:
            logging.warning("No CQL content to save to Neo4j.")

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
        if self.cql_content:
            await self._falkordb_service.execute_cql_string_async(self.cql_content)
        else:
            logging.warning("No CQL content to save to FalkorDB.")

    def to_rdflib(self) -> Graph:
        """
        Converts the knowledge graph to an RDFLib Graph.
        """
        # Placeholder for implementation
        pass

    def to_networkx(self) -> nx.Graph:
        """Converts the knowledge graph to a NetworkX Graph."""
        # Placeholder for implementation
        pass

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
