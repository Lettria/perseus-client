import logging
import tempfile
from typing import Dict, Optional
import aiohttp
import certifi
import ssl
import asyncio
from pydantic import ValidationError
from .services.neo4j_service import Neo4jService
from .config import Settings
from .models import File, Job, OntologyStatus, FileStatus
from .exceptions import ConfigurationException
from .services.file_service import FileService
from .services.job_service import JobService
from .services.ontology_service import OntologyService
from .config import settings


class PerseusClient:
    """
    A client for interacting with the Perseus API.
    This client handles authentication and provides methods for accessing the various
    API endpoints. It requires the `LETTRIA_API_KEY` environment variable to be set.
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
        self._api_token = self.settings.lettria_api_key
        self._session: Optional[aiohttp.ClientSession] = None
        self._connector: Optional[aiohttp.TCPConnector] = None
        self._file: Optional[FileService] = None
        self._job: Optional[JobService] = None
        self._ontology: Optional[OntologyService] = None
        self._neo4j: Optional[Neo4jService] = None
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
            "Authorization": f"Bearer {self._api_token}",
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

    def build_graph(
        self,
        file_path: str,
        ontology_path: Optional[str] = None,
        output_path: Optional[str] = None,
        save_to_neo4j: bool = False,
        refresh_graph: bool = False,
    ) -> Job:
        self._ensure_active()
        if not self._loop:
            raise ConfigurationException("Event loop not initialized.")
        return self._loop.run_until_complete(
            self.build_graph_async(
                file_path,
                ontology_path,
                output_path,
                save_to_neo4j,
                refresh_graph,
            )
        )

    async def build_graph_async(
        self,
        file_path: str,
        ontology_path: Optional[str] = None,
        output_path: Optional[str] = None,
        save_to_neo4j: bool = False,
        refresh_graph: bool = False,
    ) -> Job:
        """
        Processes a file by uploading it, optionally with an ontology, running a
        job, and downloading the output.
        Args:
            file_path: The path to the file to process.
            ontology_path: The path to the ontology file to use.
            output_path: The path to save the output to. If not provided, a default
                         path will be used.
            save_to_neo4j: Whether to save the output to Neo4j.
            refresh_graph: Whether to force a new job to be created (refresh the graph).
        Returns:
            The completed job.
        """
        created_file = await self.file.upload_file_async(file_path)

        if created_file.status == FileStatus.PENDING:
            await self.file.wait_for_file_upload_async(created_file.id)

        created_ontology_id = None
        if ontology_path:
            created_ontology = await self.ontology.upload_ontology_async(ontology_path)
            if created_ontology.status == OntologyStatus.PENDING:
                await self.ontology.wait_for_ontology_upload_async(created_ontology.id)
            created_ontology_id = created_ontology.id

        completed_job = None
        if not refresh_graph:
            completed_job = await self.job.find_latest_job_async(
                file_id=created_file.id, ontology_id=created_ontology_id
            )
        if not completed_job:
            completed_job = await self.job.run_job_async(
                file_id=created_file.id, ontology_id=created_ontology_id
            )

        if not output_path:
            temp_dir = tempfile.gettempdir()
            output_path = f"{temp_dir}/perseus_job_{completed_job.id}_output"

        await self.job.download_job_output_async(completed_job.id, output_path)

        if save_to_neo4j:
            await self.neo4j.save_output_to_neo4j_async(f"{output_path}.cql")

        return completed_job
