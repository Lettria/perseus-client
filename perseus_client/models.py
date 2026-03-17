"""
Data models for the Perseus client, designed for high-fidelity graph representation.
"""

from pydantic import BaseModel, Field, PrivateAttr
from datetime import datetime
from enum import Enum
import logging
from typing import Optional, List, Dict, Any, TYPE_CHECKING

logger = logging.getLogger(__name__)

# Use TYPE_CHECKING to avoid circular dependencies during runtime
if TYPE_CHECKING:
    from .services.ttl_service import TTLService
    from .services.cql_service import CQLService
    from .services.neo4j_service import Neo4jService
    from .services.falkordb_service import FalkorDBService
    from .services.graph_service import GraphService


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


class KnowledgeGraph(BaseModel):
    """
    Represents a knowledge graph with high-fidelity RDF data.
    This class acts as a data container and provides convenient methods to export
    or save the graph, which are powered by the injected service classes.
    """

    entities: List[Entity] = Field(default_factory=list)
    relations: List[Relation] = Field(default_factory=list)
    documents: List[Document] = Field(default_factory=list)
    namespaces: Dict[str, str] = Field(default_factory=dict)

    ttl_content: Optional[str] = None
    cql_content: Optional[str] = None

    _ttl_service: Optional["TTLService"] = PrivateAttr(default=None)
    _cql_service: Optional["CQLService"] = PrivateAttr(default=None)
    _neo4j_service: Optional["Neo4jService"] = PrivateAttr(default=None)
    _falkordb_service: Optional["FalkorDBService"] = PrivateAttr(default=None)
    _graph_service: Optional["GraphService"] = PrivateAttr(default=None)

    def __init__(
        self,
        ttl_service: Optional["TTLService"] = None,
        cql_service: Optional["CQLService"] = None,
        neo4j_service: Optional["Neo4jService"] = None,
        falkordb_service: Optional["FalkorDBService"] = None,
        graph_service: Optional["GraphService"] = None,
        **data,
    ):
        super().__init__(**data)
        self._ttl_service = ttl_service
        self._cql_service = cql_service
        self._neo4j_service = neo4j_service
        self._falkordb_service = falkordb_service
        self._graph_service = graph_service
        logger.debug(
            f"KnowledgeGraph initialized with services: "
            f"ttl={bool(ttl_service)}, cql={bool(cql_service)}, "
            f"neo4j={bool(neo4j_service)}, falkordb={bool(falkordb_service)}, "
            f"graph={bool(graph_service)}"
        )

    def save_ttl(self, file_path: str):
        if not self._ttl_service:
            raise RuntimeError(
                "TTLService not available on this KnowledgeGraph instance."
            )
        self._ttl_service.save_ttl(self, file_path)

    def to_ttl(self) -> str:
        if not self._ttl_service:
            raise RuntimeError(
                "TTLService not available on this KnowledgeGraph instance."
            )
        return self._ttl_service.to_ttl(self)

    def save_cql(self, file_path: str, strip_prefixes: bool = True):
        if not self._cql_service:
            raise RuntimeError(
                "CQLService not available on this KnowledgeGraph instance."
            )
        self._cql_service.save_cql(self, file_path, strip_prefixes)

    def to_cql(self, strip_prefixes: bool = True) -> str:
        if not self._cql_service:
            raise RuntimeError(
                "CQLService not available on this KnowledgeGraph instance."
            )
        return self._cql_service.to_cql(self, strip_prefixes)

    def save_to_neo4j(self, strip_prefixes: bool = True):
        if not self._neo4j_service:
            raise RuntimeError(
                "Neo4jService not available on this KnowledgeGraph instance."
            )
        self._neo4j_service.save_to_neo4j(self, strip_prefixes)

    async def save_to_neo4j_async(self, strip_prefixes: bool = True):
        if not self._neo4j_service:
            raise RuntimeError(
                "Neo4jService not available on this KnowledgeGraph instance."
            )
        await self._neo4j_service.save_to_neo4j_async(self, strip_prefixes)

    def save_to_falkordb(self):
        if not self._falkordb_service:
            raise RuntimeError(
                "FalkorDBService not available on this KnowledgeGraph instance."
            )
        self._falkordb_service.save_to_falkordb(self)

    async def save_to_falkordb_async(self):
        if not self._falkordb_service:
            raise RuntimeError(
                "FalkorDBService not available on this KnowledgeGraph instance."
            )
        await self._falkordb_service.save_to_falkordb_async(self)

    @staticmethod
    def interlink(
        kbs: List["KnowledgeGraph"],
        interlinking_key_uri: str = "http://www.w3.org/2000/01/rdf-schema#label",
        immutable_properties: Optional[List[str]] = None,
        merge_properties_on_conflict: bool = False,
    ) -> "KnowledgeGraph":
        if not kbs:
            return KnowledgeGraph()

        first_kg = kbs[0]
        if not first_kg._graph_service:
            raise RuntimeError(
                "GraphService not available on the provided KnowledgeGraph instances."
            )

        # Call the graph service's interlink method
        merged_kg = first_kg._graph_service.interlink(
            kbs,
            interlinking_key_uri,
            immutable_properties,
            merge_properties_on_conflict,
        )

        # Inject services into the newly created graph
        merged_kg._ttl_service = first_kg._ttl_service
        merged_kg._cql_service = first_kg._cql_service
        merged_kg._neo4j_service = first_kg._neo4j_service
        merged_kg._falkordb_service = first_kg._falkordb_service
        merged_kg._graph_service = first_kg._graph_service
        merged_kg.ttl_content = merged_kg.to_ttl()
        merged_kg.cql_content = merged_kg.to_cql()

        return merged_kg

    def to_json(self) -> dict:
        """Converts the knowledge graph to a JSON serializable dictionary."""
        return self.model_dump(
            exclude={
                "_ttl_service",
                "_cql_service",
                "_neo4j_service",
                "_falkordb_service",
                "_graph_service",
            }
        )


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
