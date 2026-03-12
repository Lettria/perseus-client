import logging
import tempfile
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
from .config import settings


class PerseusClient:
    """
    A client for interacting with the Perseus API.
    This client handles authentication and provides methods for accessing the various
    API endpoints. It requires the `PERSEUS_API_KEY` environment variable to be set.
    """

    def __init__(self, api_host: Optional[str] = None):
        """
        Initializes the PerseusClient.
        Args:
            api_host: The API host to connect to. Defaults to the value of the
                      `PERSEUS_API_HOST` environment variable, or the default staging URL.
        """
        self.settings = settings
        self.api_host = api_host or self.settings.perseus_api_host
        self._perseus_api_key = self.settings.perseus_api_key

        if not self._perseus_api_key:
            raise ConfigurationException(
                "Perseus API key is not configured. Please create a .env file with "
                "PERSEUS_API_KEY='your_key_here' or set the environment variable."
            )

        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
        self._file: Optional[FileService] = None
        self._job: Optional[JobService] = None
        self._ontology: Optional[OntologyService] = None
        self._neo4j: Optional[Neo4jService] = None
        self._falkordb: Optional[FalkorDBService] = None
        self._cql: Optional[CQLService] = None
        self._ttl: Optional[TTLService] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _is_active(self):
        return self._session and not self._session.closed

    def _ensure_active(self):
        if not self._is_active():
            self.__enter__()

    async def __aenter__(self):
        if self._is_active():
            return self
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._connector = aiohttp.TCPConnector(ssl=ssl_context)
        self._session = aiohttp.ClientSession(
            headers=self._get_headers(), connector=self._connector
        )
        self._loop = asyncio.get_event_loop()  # Get the running loop
        self._file = FileService(self._session, self.api_host, self._loop)
        self._job = JobService(self._session, self.api_host, self._loop)
        self._ontology = OntologyService(self._session, self.api_host, self._loop)
        self._neo4j = Neo4jService(self._loop)
        self._falkordb = FalkorDBService(self._loop)
        self._cql = CQLService()
        self._ttl = TTLService()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()
        if self._connector:
            await self._connector.close()
        self._session = None
        self._connector = None

    def __enter__(self):
        """
        Synchronous entry point for the client context manager.
        Initializes the async session by running __aenter__ in a new event loop.
        """
        if self._is_active():
            return self
        # Create a new loop for synchronous use
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
        self._loop.run_until_complete(self.__aexit__(exc_type, exc_val, exc_tb))
        self._loop.close()
        asyncio.set_event_loop(
            asyncio.new_event_loop()
        )  # Clean up the event loop for synchronous use
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

    def build_graph(
        self,
        file_path: List[str],
        ontology_path: Optional[str] = None,
        refresh_graph: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[KnowledgeGraph]:
        """
        Synchronously processes one or more files by uploading them, optionally with an ontology,
        running jobs, and returning KnowledgeGraph objects.
        Args:
            file_path: A list of file paths to process.
            ontology_path: The path to the ontology file to use for all files.
            refresh_graph: Whether to force new jobs to be created (refresh the graph).
            metadata: A dictionary of metadata to add to all nodes and relationships.
        Returns:
            A list of KnowledgeGraph objects.
        """
        self._ensure_active()
        if not self._loop:
            raise ConfigurationException("Event loop not initialized.")
        return self._loop.run_until_complete(
            self.build_graph_async(
                file_path,
                ontology_path,
                refresh_graph,
                metadata,
            )
        )

    async def _build_single_graph_async(
        self,
        file_path: str,
        ontology_id: Optional[str] = None,
        refresh_graph: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> KnowledgeGraph:
        """
        Processes a single file to build a KnowledgeGraph with resilient job handling.
        """
        created_file = await self.file.upload_file_async(file_path)
        if created_file.status == FileStatus.PENDING:
            await self.file.wait_for_file_upload_async(created_file.id)

        job_to_run = None
        # Check for the latest job, regardless of its status
        latest_job = await self.job.find_latest_job_async(
            file_id=created_file.id, ontology_id=ontology_id
        )

        if latest_job:
            if latest_job.status in [
                JobStatus.PENDING,
                JobStatus.RUNNING,
                JobStatus.STARTING,
                JobStatus.RUNNABLE,
            ]:
                logging.info(f"Attaching to existing in-progress job {latest_job.id}")
                job_to_run = latest_job
            elif latest_job.status == JobStatus.SUCCEEDED:
                if refresh_graph:
                    logging.info("`refresh_graph` is True, submitting new job.")
                else:
                    logging.info(f"Using existing completed job {latest_job.id}")
                    job_to_run = latest_job

        if not job_to_run:
            logging.info("No suitable existing job found, submitting a new one.")
            job_to_run = await self.job.submit_job_async(
                file_id=created_file.id, ontology_id=ontology_id
            )

        # Wait for the job (either new or pre-existing) to complete
        completed_job = await self.job.run_job_async(job_id=job_to_run.id)

        output_dir = "/tmp/perseus-client/output"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/{completed_job.id}_output"

        await self.job.download_job_output_async(completed_job.id, output_path)

        cql_file_path = f"{output_path}.cql"
        ttl_file_path = f"{output_path}.ttl"

        cql_content: Optional[str] = None
        ttl_content: Optional[str] = None

        if metadata:
            if os.path.exists(ttl_file_path):
                with open(ttl_file_path, "r", encoding="utf-8") as f:
                    ttl_content = f.read()
                modified_ttl = self.ttl.add_metadata_to_ttl(ttl_content, metadata)
                with open(ttl_file_path, "w", encoding="utf-8") as f:
                    f.write(modified_ttl)
                ttl_content = modified_ttl

            if os.path.exists(cql_file_path):
                with open(cql_file_path, "r", encoding="utf-8") as f:
                    cql_content = f.read()
                cql_content = self.cql.add_metadata_to_cql(cql_content, metadata)
                with open(cql_file_path, "w", encoding="utf-8") as f:
                    f.write(cql_content)

        if os.path.exists(cql_file_path) and cql_content is None:
            with open(cql_file_path, "r", encoding="utf-8") as f:
                cql_content = f.read()

        if os.path.exists(ttl_file_path):
            if ttl_content is None:
                with open(ttl_file_path, "r", encoding="utf-8") as f:
                    ttl_content = f.read()

            kg = self.ttl.parse_ttl_to_knowledge_graph(
                ttl_content, neo4j_service=self.neo4j, falkordb_service=self.falkordb
            )
            kg.ttl_content = ttl_content
            kg.cql_content = cql_content
            return kg
        else:
            logging.warning(
                f"TTL file not found at {ttl_file_path}. Returning empty KnowledgeGraph."
            )
            return KnowledgeGraph(
                cql_content=cql_content,
                neo4j_service=self.neo4j,
                falkordb_service=self.falkordb,
            )

    async def build_graph_async(
        self,
        file_path: List[str],
        ontology_path: Optional[str] = None,
        refresh_graph: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[KnowledgeGraph]:
        """
        Processes one or more files by uploading them, optionally with an ontology,
        running jobs, and returning KnowledgeGraph objects.
        Args:
            file_path: A list of file paths to process.
            ontology_path: The path to the ontology file to use for all files.
            refresh_graph: Whether to force new jobs to be created (refresh the graph).
            metadata: A dictionary of metadata to add to all nodes and relationships.
        Returns:
            A list of KnowledgeGraph objects.
        """
        created_ontology_id = None
        if ontology_path:
            # This part still runs sequentially as the ontology is shared
            created_ontology = await self.ontology.upload_ontology_async(ontology_path)
            if created_ontology.status == OntologyStatus.PENDING:
                # A single spinner for the ontology upload
                await self.ontology.wait_for_ontology_upload_async(created_ontology.id)
            created_ontology_id = created_ontology.id

        # Create a list of tasks and descriptions for the rich progress display
        tasks = []
        descriptions = []
        for path in file_path:
            descriptions.append(f"Processing file {os.path.basename(path)}...")
            task = self._build_single_graph_async(
                file_path=path,
                ontology_id=created_ontology_id,
                refresh_graph=refresh_graph,
                metadata=metadata,
            )
            tasks.append(task)

        # Use the new rich-based waiter from the job service
        results = await self.job._wait_for_tasks(tasks, descriptions)

        return results
