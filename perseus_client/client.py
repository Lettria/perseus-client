from typing import Dict, Optional
import aiohttp
import certifi
import ssl
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

    async def __aenter__(self):
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        self._connector = aiohttp.TCPConnector(ssl=ssl_context)
        self._session = aiohttp.ClientSession(
            headers=self._get_headers(), connector=self._connector
        )
        self._file = FileService(self._session, self.api_host)
        self._job = JobService(self._session, self.api_host)
        self._ontology = OntologyService(self._session, self.api_host)
        self._neo4j = Neo4jService()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

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
        if not self._file:
            raise ConfigurationException(
                "The client is not active. Use 'async with PerseusClient() as client:'"
            )
        return self._file

    @property
    def job(self) -> JobService:
        if not self._job:
            raise ConfigurationException(
                "The client is not active. Use 'async with PerseusClient() as client:'"
            )
        return self._job

    @property
    def ontology(self) -> OntologyService:
        if not self._ontology:
            raise ConfigurationException(
                "The client is not active. Use 'async with PerseusClient() as client:'"
            )
        return self._ontology

    @property
    def neo4j(self):
        if not self._neo4j:
            raise ConfigurationException(
                "The client is not active. Use 'async with PerseusClient() as client:'"
            )
        return self._neo4j

    async def index_file(
        self,
        file_path: str,
        ontology_path: Optional[str] = None,
        output_path: Optional[str] = None,
        save_to_neo4j: bool = False,
        refresh_graph: bool = False,
    ) -> Job:
        return await self.build_graph(
            file_path, ontology_path, output_path, save_to_neo4j, refresh_graph
        )

    async def build_graph(
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
        created_file = await self.file.upload_file(file_path)

        if created_file.status == FileStatus.PENDING:
            await self.file.wait_for_file_upload(created_file.id)

        created_ontology_id = None
        if ontology_path:
            created_ontology = await self.ontology.upload_ontology(ontology_path)
            if created_ontology.status == OntologyStatus.PENDING:
                await self.ontology.wait_for_ontology_upload(created_ontology.id)
            created_ontology_id = created_ontology.id

        completed_job = None
        if not refresh_graph:
            completed_job = await self.job.find_latest_job(
                file_id=created_file.id, ontology_id=created_ontology_id
            )
        if not completed_job:
            completed_job = await self.job.run_job(
                file_id=created_file.id, ontology_id=created_ontology_id
            )

        if not output_path:
            output_path = f"./job_{completed_job.id}.output"

        await self.job.download_job_output(completed_job.id, output_path)

        if save_to_neo4j:
            await self.neo4j.save_output_to_neo4j(f"{output_path}.cql")

        return completed_job
