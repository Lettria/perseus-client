import logging
import tempfile
import os
from typing import Dict, Optional, Any, List

from ..models import KnowledgeGraph, FileStatus, JobStatus, OntologyStatus
from .file_service import FileService
from .job_service import JobService
from .ontology_service import OntologyService
from .ttl_service import TTLService
from .cql_service import CQLService
from .neo4j_service import Neo4jService
from .falkordb_service import FalkorDBService
from .graph_service import GraphService
from .interlink_service import InterlinkService
from .rdflib_service import RDFLibService


logger = logging.getLogger(__name__)


class BuildService:
    def __init__(
        self,
        file_service: FileService,
        job_service: JobService,
        ontology_service: OntologyService,
        ttl_service: TTLService,
        cql_service: CQLService,
        neo4j_service: Neo4jService,
        falkordb_service: FalkorDBService,
        graph_service: GraphService,
        interlink_service: InterlinkService,
        rdflib_service: RDFLibService,
    ):
        self._file = file_service
        self._job = job_service
        self._ontology = ontology_service
        self._ttl = ttl_service
        self._cql = cql_service
        self._neo4j = neo4j_service
        self._falkordb = falkordb_service
        self._graph = graph_service
        self._interlink = interlink_service
        self._rdflib = rdflib_service

    def build_graph(
        self,
        file_paths: List[str],
        ontology_path: Optional[str] = None,
        refresh_graph: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[KnowledgeGraph]:
        """
        Synchronously processes one or more files by uploading them, optionally with an ontology,
        running jobs, and returning KnowledgeGraph objects.
        Args:
            file_paths: A list of file paths to process.
            ontology_path: The path to the ontology file to use for all files.
            refresh_graph: Whether to force new jobs to be created (refresh the graph).
            metadata: A dictionary of metadata to add to all nodes and relationships.
        Returns:
            A list of KnowledgeGraph objects.
        """
        return self._job._loop.run_until_complete(
            self.build_graph_async(
                file_paths,
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
        logger.debug(f"Building graph for file: {file_path}")
        created_file = await self._file.upload_file_async(file_path)
        if created_file.status == FileStatus.PENDING:
            await self._file.wait_for_file_upload_async(created_file.id)

        job_to_run = None
        logger.debug(
            f"Searching for existing job for file_id: {created_file.id}, ontology_id: {ontology_id}"
        )
        latest_job = await self._job.find_latest_job_async(
            file_id=created_file.id, ontology_id=ontology_id
        )

        if latest_job:
            logger.debug(
                f"Found latest job {latest_job.id} with status {latest_job.status}"
            )
            if latest_job.status in [
                JobStatus.PENDING,
                JobStatus.RUNNING,
                JobStatus.STARTING,
                JobStatus.RUNNABLE,
            ]:
                logger.debug(f"Attaching to existing in-progress job: {latest_job.id}")
                job_to_run = latest_job
            elif latest_job.status == JobStatus.SUCCEEDED:
                if refresh_graph:
                    logger.info(
                        f"Job {latest_job.id} already succeeded, but refresh_graph=True, so submitting a new job."
                    )
                else:
                    logger.debug(f"Using existing completed job: {latest_job.id}")
                    job_to_run = latest_job
            elif latest_job.status == JobStatus.FAILED:
                logger.warning(
                    f"Latest job {latest_job.id} failed. Submitting a new job."
                )

        if not job_to_run:
            logger.debug("No suitable existing job found, submitting a new job.")
            job_to_run = await self._job.submit_job_async(
                file_id=created_file.id, ontology_id=ontology_id
            )

        # Wait for the job (either new or pre-existing) to complete
        completed_job = await self._job.run_job_async(job_id=job_to_run.id)

        # Create a temporary directory for job outputs
        output_dir = tempfile.mkdtemp(prefix="perseus-client-")
        output_path = os.path.join(output_dir, f"{completed_job.id}_output")
        logger.debug(f"Downloading job output to temporary path: {output_path}")

        await self._job.download_job_output_async(completed_job.id, output_path)

        cql_file_path = f"{output_path}.cql"
        ttl_file_path = f"{output_path}.ttl"

        cql_content: Optional[str] = None
        ttl_content: Optional[str] = None

        if metadata:
            logger.debug(f"Applying metadata: {metadata}")
            if os.path.exists(ttl_file_path):
                with open(ttl_file_path, "r", encoding="utf-8") as f:
                    ttl_content = f.read()
                modified_ttl = self._ttl.add_metadata_to_ttl(ttl_content, metadata)
                with open(ttl_file_path, "w", encoding="utf-8") as f:
                    f.write(modified_ttl)
                ttl_content = modified_ttl
                logger.debug("Successfully applied metadata to TTL content.")

            if os.path.exists(cql_file_path):
                with open(cql_file_path, "r", encoding="utf-8") as f:
                    cql_content = f.read()
                cql_content = self._cql.add_metadata_to_cql(cql_content, metadata)
                with open(cql_file_path, "w", encoding="utf-8") as f:
                    f.write(cql_content)
                logger.debug("Successfully applied metadata to CQL content.")

        if os.path.exists(cql_file_path) and cql_content is None:
            with open(cql_file_path, "r", encoding="utf-8") as f:
                cql_content = f.read()

        if os.path.exists(ttl_file_path):
            logger.debug(
                f"TTL file found at {ttl_file_path}, parsing to KnowledgeGraph."
            )
            if ttl_content is None:
                with open(ttl_file_path, "r", encoding="utf-8") as f:
                    ttl_content = f.read()

            kg = self._ttl.parse_ttl_to_knowledge_graph(ttl_content)
            kg.ttl_content = ttl_content
            kg.cql_content = cql_content
        else:
            logger.warning(
                f"TTL file not found at {ttl_file_path}. Returning empty KnowledgeGraph."
            )
            # Create an empty graph but still attach content if available
            kg = KnowledgeGraph(cql_content=cql_content)

        # Inject services into the created KnowledgeGraph instance
        logger.debug("Injecting services into KnowledgeGraph instance.")
        kg._ttl_service = self._ttl
        kg._cql_service = self._cql
        kg._neo4j_service = self._neo4j
        kg._falkordb_service = self._falkordb
        kg._graph_service = self._graph
        kg._interlink_service = self._interlink
        kg._rdflib_service = self._rdflib

        return kg

    async def build_graph_async(
        self,
        file_paths: List[str],
        ontology_path: Optional[str] = None,
        refresh_graph: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[KnowledgeGraph]:
        """
        Processes one or more files by uploading them, optionally with an ontology,
        running jobs, and returning KnowledgeGraph objects.
        Args:
            file_paths: A list of file paths to process.
            ontology_path: The path to the ontology file to use for all files.
            refresh_graph: Whether to force new jobs to be created (refresh the graph).
            metadata: A dictionary of metadata to add to all nodes and relationships.
        Returns:
            A list of KnowledgeGraph objects.
        """
        created_ontology_id = None
        if ontology_path:
            logger.debug(f"Using ontology from path: {ontology_path}")
            # This part still runs sequentially as the ontology is shared
            created_ontology = await self._ontology.upload_ontology_async(ontology_path)
            if created_ontology.status == OntologyStatus.PENDING:
                # A single spinner for the ontology upload
                await self._ontology.wait_for_ontology_upload_async(created_ontology.id)
            created_ontology_id = created_ontology.id
            logger.debug(f"Using ontology_id: {created_ontology_id}")

        tasks = []
        for path in file_paths:
            task = self._build_single_graph_async(
                file_path=path,
                ontology_id=created_ontology_id,
                refresh_graph=refresh_graph,
                metadata=metadata,
            )
            tasks.append(task)

        logger.debug(f"Processing {len(tasks)} file(s)...")

        results = await self._job._wait_for_tasks(
            tasks, [os.path.basename(p) for p in file_paths]
        )

        logger.info("All files processed.")
        return results
