import logging
from typing import Dict, Optional, Any, List, Union
import aiohttp
import certifi
import ssl
import asyncio
import os

from .services.ttl_service import TTLService
from .services.neo4j_service import Neo4jService
from .services.falkordb_service import FalkorDBService
from .services.cql_service import CQLService
from .services.graph_service import GraphService
from .services.interlink_service import InterlinkService
from .services.rdflib_service import RDFLibService
from .config import Settings
from .models import (
    File,
    Job,
    JobStatus,
    OntologyStatus,
    FileStatus,
    KnowledgeGraph,
    Entity,
    Relation,
    Document,
)
from .exceptions import ConfigurationException
from .services.file_service import FileService
from .services.job_service import JobService
from .services.ontology_service import OntologyService
from .services.build_service import BuildService, _NOT_PROVIDED, DEFAULT_MAX_CONCURRENCY
from .config import settings

logger = logging.getLogger(__name__)


class PerseusClient:
    """
    A client for interacting with the Perseus API.
    This client handles authentication and provides methods for accessing the various
    API endpoints. It requires the `PERSEUS_API_KEY` environment variable to be set.

    Session lifecycle:
        - `build_graph` / `interlink` called on a client that isn't active open a
          session for the duration of the call and close it afterwards.
        - The service properties (`file`, `job`, `ontology`, ...) cannot know when the
          caller is done, so the session they open stays open. Use
          `with PerseusClient() as client:` or call `client.close()` explicitly.
        - In async code, prefer `async with PerseusClient() as client:`. The service
          properties cannot activate the client from inside a running event loop.
        - Services hold the session they were created with. Access them through the
          client (`client.file.<method>`) each time rather than storing `client.file`
          across a `close()`.
    """

    def __init__(self, api_host: Optional[str] = None, base_uri: Optional[str] = None):
        """
        Initializes the PerseusClient.
        Args:
            api_host: The API host to connect to. Defaults to the value of the
                      `PERSEUS_API_HOST` environment variable, or the default staging URL.
            base_uri: The base URI to use for rebasing entity and relation IRIs.
        """
        self.settings = settings
        self.api_host = api_host or self.settings.perseus_api_host
        self.base_uri = base_uri
        self._perseus_api_key = self.settings.perseus_api_key
        logger.debug(f"PerseusClient initialized for API host: {self.api_host}")

        if not self._perseus_api_key:
            raise ConfigurationException(
                "Perseus API key is not configured. Please create a .env file with "
                "PERSEUS_API_KEY='your_key_here' or set the environment variable."
            )

        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
        self._transfer_session: Optional[aiohttp.ClientSession] = None
        self._file: Optional[FileService] = None
        self._job: Optional[JobService] = None
        self._ontology: Optional[OntologyService] = None
        self._neo4j: Optional[Neo4jService] = None
        self._falkordb: Optional[FalkorDBService] = None
        self._cql: Optional[CQLService] = None
        self._ttl: Optional[TTLService] = None
        self._graph: Optional[GraphService] = None
        self._interlink: Optional[InterlinkService] = None
        self._rdflib: Optional[RDFLibService] = None
        self._build: Optional[BuildService] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _is_active(self):
        return self._session and not self._session.closed

    def _ensure_active(self) -> bool:
        """
        Activates the client synchronously if needed.
        Returns True if this call opened the session.
        """
        if self._is_active():
            return False
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            # No loop running: synchronous usage, safe to create our own loop.
            self.__enter__()
            return True
        raise ConfigurationException(
            "PerseusClient is not active. In async code, use "
            "`async with PerseusClient() as client:` before accessing services."
        )

    async def _ensure_active_async(self):
        """
        Activates the client in the running event loop if needed.
        """
        running_loop = asyncio.get_running_loop()
        if self._is_active():
            if self._loop is not running_loop:
                raise ConfigurationException(
                    "This PerseusClient's session is bound to a different event loop. "
                    "Create a new client inside the current loop."
                )
            return
        await self.__aenter__()

    async def __aenter__(self):
        if self._is_active():
            return self
        logger.debug("Starting asynchronous session.")
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._connector = aiohttp.TCPConnector(ssl=ssl_context)
        self._session = aiohttp.ClientSession(
            headers=self._get_headers(), connector=self._connector
        )
        # Presigned URLs must not receive the API's Authorization header.
        self._transfer_session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_context)
        )
        self._loop = asyncio.get_running_loop()
        self._file = FileService(
            self._session, self.api_host, self._loop, self._transfer_session
        )
        self._job = JobService(
            self._session, self.api_host, self._loop, self._transfer_session
        )
        self._ontology = OntologyService(
            self._session, self.api_host, self._loop, self._transfer_session
        )
        self._neo4j = Neo4jService(self._loop)
        self._falkordb = FalkorDBService(self._loop)
        self._cql = CQLService()
        self._ttl = TTLService()
        self._graph = GraphService()
        self._interlink = InterlinkService()
        self._rdflib = RDFLibService()
        self._build = BuildService(
            self._file,
            self._job,
            self._ontology,
            self._ttl,
            self._cql,
            self._neo4j,
            self._falkordb,
            self._graph,
            self._interlink,
            self._rdflib,
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.debug("Closing asynchronous session.")
        if self._session:
            await self._session.close()
        if self._connector:
            await self._connector.close()
        if self._transfer_session:
            await self._transfer_session.close()
        self._session = None
        self._connector = None
        self._transfer_session = None

    def __enter__(self):
        """
        Synchronous entry point for the client context manager.
        Initializes the async session by running __aenter__ in a new event loop.
        """
        if self._is_active():
            return self
        # Create a new loop for synchronous use
        logger.debug("Creating new event loop for synchronous client usage.")
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self.__aenter__())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Synchronous exit point for the client context manager.
        Closes the async session by running __aexit__ in a new event loop.
        """
        if self._loop is None:
            return
        logger.debug("Closing event loop for synchronous client usage.")
        self._loop.run_until_complete(self.__aexit__(exc_type, exc_val, exc_tb))
        self._loop.close()
        # __enter__ installs a fresh loop on next activation
        asyncio.set_event_loop(None)
        self._session = None
        self._connector = None
        self._loop = None

    def close(self):
        """
        Explicitly closes the client's aiohttp session and cleans up resources.
        This method must be called when the client is no longer needed
        to prevent resource leaks.
        """
        if not self._is_active():
            return
        self.__exit__(None, None, None)

    def _get_headers(self) -> Dict[str, str]:
        """
        Returns the headers for the API requests.
        """
        return {
            "Authorization": f"Bearer {self._perseus_api_key}",
            "Content-Type": "application/json",
        }

    @property
    def file(self) -> FileService:
        self._ensure_active()
        if not self._file:
            raise ConfigurationException("File service not initialized.")
        return self._file

    @property
    def job(self) -> JobService:
        self._ensure_active()
        if not self._job:
            raise ConfigurationException("Job service not initialized.")
        return self._job

    @property
    def ontology(self) -> OntologyService:
        self._ensure_active()
        if not self._ontology:
            raise ConfigurationException("Ontology service not initialized.")
        return self._ontology

    @property
    def neo4j(self):
        self._ensure_active()
        if not self._neo4j:
            raise ConfigurationException("Neo4j service not initialized.")
        return self._neo4j

    @property
    def falkordb(self):
        self._ensure_active()
        if not self._falkordb:
            raise ConfigurationException("FalkorDB service not initialized.")
        return self._falkordb

    @property
    def cql(self) -> CQLService:
        self._ensure_active()
        if not self._cql:
            raise ConfigurationException("CQL service not initialized.")
        return self._cql

    @property
    def ttl(self) -> TTLService:
        self._ensure_active()
        if not self._ttl:
            raise ConfigurationException("TTL service not initialized.")
        return self._ttl

    @property
    def graph(self) -> GraphService:
        self._ensure_active()
        if not self._graph:
            raise ConfigurationException("Graph service not initialized.")
        return self._graph

    @property
    def interlink(self) -> InterlinkService:
        self._ensure_active()
        if not self._interlink:
            raise ConfigurationException("Interlink service not initialized.")
        return self._interlink

    @property
    def rdflib(self) -> RDFLibService:
        self._ensure_active()
        if not self._rdflib:
            raise ConfigurationException("RDFLib service not initialized.")
        return self._rdflib

    @property
    def build(self) -> BuildService:
        self._ensure_active()
        if not self._build:
            raise ConfigurationException("Build service not initialized.")
        return self._build

    def build_graph(
        self,
        file_paths: List[str],
        ontology_path: Optional[str] = None,
        refresh_graph: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
        base_uri: Optional[str] = None,
        project_id = _NOT_PROVIDED,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        return_exceptions: bool = False,
    ) -> List[Union[KnowledgeGraph, BaseException]]:
        """
        Synchronously processes one or more files by uploading them, optionally with an ontology,
        running jobs, and returning KnowledgeGraph objects.
        Args:
            file_paths: A list of file paths to process.
            ontology_path: The path to the ontology file to use for all files.
            refresh_graph: Whether to force new jobs to be created (refresh the graph).
            metadata: A dictionary of metadata to add to all nodes and relationships.
            base_uri: The base URI to use for rebasing entity and relation IRIs.
            project_id: The project ID to assign to jobs. Pass explicit None to unassign.
                       If not provided, job's project assignment remains unchanged.
            max_concurrency: The maximum number of files processed at the same time
                (upload, job and download). Must be at least 1.
            return_exceptions: If False, the first failure cancels the remaining files
                and is re-raised. If True, every file is processed and the result list
                holds either a KnowledgeGraph or the exception for each input.
        Returns:
            A list with one entry per input file, in input order.
        """
        opened = self._ensure_active()
        try:
            if not self._loop:
                raise ConfigurationException("Event loop not initialized.")
            return self._loop.run_until_complete(
                self.build_graph_async(
                    file_paths,
                    ontology_path,
                    refresh_graph,
                    metadata,
                    base_uri,
                    project_id,
                    max_concurrency,
                    return_exceptions,
                )
            )
        finally:
            if opened:
                self.close()

    async def build_graph_async(
        self,
        file_paths: List[str],
        ontology_path: Optional[str] = None,
        refresh_graph: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
        base_uri: Optional[str] = None,
        project_id = _NOT_PROVIDED,
        max_concurrency: int = DEFAULT_MAX_CONCURRENCY,
        return_exceptions: bool = False,
    ) -> List[Union[KnowledgeGraph, BaseException]]:
        """
        Asynchronously processes one or more files by uploading them, optionally with an ontology,
        running jobs, and returning KnowledgeGraph objects.

        Prefer `async with PerseusClient() as client:`. If the client isn't active, a
        session is opened in the running loop and stays open until `close()` is called.
        Args:
            file_paths: A list of file paths to process.
            ontology_path: The path to the ontology file to use for all files.
            refresh_graph: Whether to force new jobs to be created (refresh the graph).
            metadata: A dictionary of metadata to add to all nodes and relationships.
            base_uri: The base URI to use for rebasing entity and relation IRIs.
            project_id: The project ID to assign to jobs. Pass explicit None to unassign.
                       If not provided, job's project assignment remains unchanged.
            max_concurrency: The maximum number of files processed at the same time
                (upload, job and download). Must be at least 1.
            return_exceptions: If False, the first failure cancels the remaining files
                and is re-raised. If True, every file is processed and the result list
                holds either a KnowledgeGraph or the exception for each input.
        Returns:
            A list with one entry per input file, in input order.
        """
        await self._ensure_active_async()
        final_base_uri = base_uri if base_uri is not None else self.base_uri
        return await self.build.build_graph_async(
            file_paths=file_paths,
            ontology_path=ontology_path,
            refresh_graph=refresh_graph,
            metadata=metadata,
            base_uri=final_base_uri,
            project_id=project_id,
            max_concurrency=max_concurrency,
            return_exceptions=return_exceptions,
        )

    def interlink(
        self,
        kbs: List[KnowledgeGraph],
        interlinking_key_uris: List[str] = [
            "http://www.w3.org/2000/01/rdf-schema#label"
        ],
        immutable_properties: Optional[List[str]] = None,
        merge_properties_on_conflict: bool = False,
    ) -> KnowledgeGraph:
        """
        Synchronously merges multiple KnowledgeGraph objects into a single one.
        """
        opened = self._ensure_active()
        try:
            if not self._loop:
                raise ConfigurationException("Event loop not initialized.")
            return self._loop.run_until_complete(
                self.interlink_async(
                    kbs,
                    interlinking_key_uris,
                    immutable_properties,
                    merge_properties_on_conflict,
                )
            )
        finally:
            if opened:
                self.close()

    async def interlink_async(
        self,
        kbs: List[KnowledgeGraph],
        interlinking_key_uris: List[str] = [
            "http://www.w3.org/2000/01/rdf-schema#label"
        ],
        immutable_properties: Optional[List[str]] = None,
        merge_properties_on_conflict: bool = False,
    ) -> KnowledgeGraph:
        """
        Asynchronously merges multiple KnowledgeGraph objects into a single one.

        Prefer `async with PerseusClient() as client:`. If the client isn't active, a
        session is opened in the running loop and stays open until `close()` is called.
        """
        await self._ensure_active_async()
        return await self.build.interlink_async(
            kbs=kbs,
            interlinking_key_uris=interlinking_key_uris,
            immutable_properties=immutable_properties,
            merge_properties_on_conflict=merge_properties_on_conflict,
        )
