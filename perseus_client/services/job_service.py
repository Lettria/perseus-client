import os
from time import time
from typing import Any, Dict, List, Optional, cast
import aiohttp
import logging
import asyncio
import ssl
import certifi

from .base_service import BaseService
from ..models import Job, JobStatus
from ..exceptions import PerseusException

logger = logging.getLogger(__name__)


class JobService(BaseService):
    def __init__(self, session, api_host, loop):
        super().__init__(session, api_host, loop)

    def submit_job(self, file_id: str, ontology_id: Optional[str] = None) -> Job:
        return self._loop.run_until_complete(
            self.submit_job_async(file_id, ontology_id)
        )

    def find_jobs(self, ids: List[str]) -> List[Job]:
        return self._loop.run_until_complete(self.find_jobs_async(ids))

    def find_job(self, id: str) -> Optional[Job]:
        return self._loop.run_until_complete(self.find_job_async(id))

    def find_latest_succeeded_job(
        self, file_id: str, ontology_id: Optional[str] = None
    ) -> Optional[Job]:
        return self._loop.run_until_complete(
            self.find_latest_succeeded_job_async(file_id, ontology_id)
        )

    def find_latest_job(
        self, file_id: str, ontology_id: Optional[str] = None
    ) -> Optional[Job]:
        return self._loop.run_until_complete(
            self.find_latest_job_async(file_id, ontology_id)
        )

    def download_job_output(
        self, job_id: str, output_path: Optional[str] = None
    ) -> str:
        return self._loop.run_until_complete(
            self.download_job_output_async(job_id, output_path)
        )

    def run_job(
        self,
        job_id: str,
        polling_interval: int = 5,
        timeout: int = 3600,
    ) -> Job:
        return self._loop.run_until_complete(
            self.run_job_async(job_id, polling_interval, timeout)
        )

    async def submit_job_async(
        self, file_id: str, ontology_id: Optional[str] = None
    ) -> Job:
        """
        Asynchronously submits a job for processing.
        """
        logger.debug(
            f"Submitting job for file_id: {file_id}, ontology_id: {ontology_id}"
        )
        response = await self._request(
            "POST",
            "/api/v0/job/submit",
            json={"fileId": file_id, "ontologyId": ontology_id},
        )
        job_data = response["job"]
        job = Job(id=job_data["id"], status=job_data["status"])
        logger.debug(f"Successfully submitted job with ID: {job.id}")
        return job

    async def find_jobs_async(self, ids: List[str]) -> List[Job]:
        """
        Asynchronously finds one or more jobs by their IDs.
        """
        logger.debug(f"Finding jobs with IDs: {ids}")
        response = await self._request("POST", "/api/v0/job/find", json={"ids": ids})
        jobs = [Job(id=job["id"], status=job["status"]) for job in response["jobs"]]
        logger.debug(f"Found {len(jobs)} jobs.")
        return jobs

    async def find_job_async(self, id: str) -> Optional[Job]:
        """
        Asynchronously finds a job by its ID.
        """
        logger.debug(f"Finding job with ID: {id}")
        jobs = await self.find_jobs_async(ids=[id])
        if not jobs:
            logger.debug(f"Job with ID: {id} not found.")
            return None
        return jobs[0]

    async def find_latest_succeeded_job_async(
        self, file_id: str, ontology_id: Optional[str] = None
    ) -> Optional[Job]:
        """
        Asynchronously finds the latest succeeded job by its file_id.
        """
        logger.debug(
            f"Finding latest succeeded job for file_id: {file_id}, ontology_id: {ontology_id}"
        )
        payload: Dict[str, Any] = {"fileId": file_id, "status": JobStatus.SUCCEEDED}
        if ontology_id:
            payload["ontologyIds"] = [ontology_id]
        response = await self._request(
            "POST",
            "/api/v0/job/find",
            params={
                "limit": 1,
                "orderBy": "createdAt",
                "orderDirection": "DESC",
            },
            json=payload,
        )
        if not response["jobs"]:
            logger.debug(
                f"No succeeded job found for file_id: {file_id}, ontology_id: {ontology_id}"
            )
            return None
        job_data = response["jobs"][0]
        job = Job(id=job_data["id"], status=job_data["status"])
        logger.debug(f"Found latest succeeded job with ID: {job.id}")
        return job

    async def find_latest_job_async(
        self, file_id: str, ontology_id: Optional[str] = None
    ) -> Optional[Job]:
        """
        Asynchronously finds the latest job by its file_id, regardless of status.
        """
        logger.debug(
            f"Finding latest job for file_id: {file_id}, ontology_id: {ontology_id}"
        )
        payload: Dict[str, Any] = {"fileId": file_id}
        if ontology_id:
            payload["ontologyIds"] = [ontology_id]
        response = await self._request(
            "POST",
            "/api/v0/job/find",
            params={
                "limit": 1,
                "orderBy": "createdAt",
                "orderDirection": "DESC",
            },
            json=payload,
        )
        if not response["jobs"]:
            logger.debug(
                f"No job found for file_id: {file_id}, ontology_id: {ontology_id}"
            )
            return None
        job_data = response["jobs"][0]
        job = Job(id=job_data["id"], status=job_data["status"])
        logger.debug(f"Found latest job with ID: {job.id} and status: {job.status}")
        return job

    async def download_job_output_async(
        self, job_id: str, output_path: Optional[str] = None
    ) -> str:
        """
        Asynchronously fetches a presigned URL and downloads the job output.
        """
        logger.debug(f"Starting download of outputs for job ID: {job_id}")
        if output_path is None:
            output_path = f"{job_id}.output"

        try:
            download_urls = await self._get_download_urls_async(job_id)
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)

            async with aiohttp.ClientSession(connector=connector) as download_session:
                # Download TTL file
                if download_urls.get("ttlFileDownloadUrl"):
                    ttl_output_path = f"{output_path}.ttl"
                    logger.debug(f"Downloading TTL file to {ttl_output_path}")
                    await self._download_file_async(
                        download_session,
                        download_urls["ttlFileDownloadUrl"],
                        ttl_output_path,
                    )
                    logger.debug(
                        f"Successfully downloaded TTL output to {ttl_output_path}"
                    )

                # Download CQL file
                if download_urls.get("cqlFileDownloadUrl"):
                    cql_output_path = f"{output_path}.cql"
                    logger.debug(f"Downloading CQL file to {cql_output_path}")
                    await self._download_file_async(
                        download_session,
                        download_urls["cqlFileDownloadUrl"],
                        cql_output_path,
                    )
                    logger.debug(
                        f"Successfully downloaded CQL output to {cql_output_path}"
                    )
        except Exception as e:
            logger.error(
                f"Failed to download outputs for job {job_id}: {e}", exc_info=True
            )
            raise

        return output_path

    async def _get_download_urls_async(self, job_id: str) -> Dict[str, str]:
        """
        Asynchronously fetches a presigned URL to download the output of a job.
        """
        logger.debug(f"Fetching download URLs for job ID: {job_id}")
        response = await self._request("GET", f"/api/v0/job/{job_id}/download-output")
        logger.debug(f"Successfully fetched download URLs for job ID: {job_id}")
        return {
            "ttlFileDownloadUrl": cast(Dict[str, Any], response)["ttlFileDownloadUrl"],
            "cqlFileDownloadUrl": cast(Dict[str, Any], response)["cqlFileDownloadUrl"],
        }

    async def _download_file_async(
        self, session: aiohttp.ClientSession, url: str, output_path: str
    ):
        """
        Asynchronously downloads a file from a URL and saves it.
        """
        logger.debug(f"Downloading file from pre-signed URL to {output_path}")
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        try:
            async with await session.get(url) as response:
                response.raise_for_status()
                with open(output_path, "wb") as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
        except aiohttp.ClientResponseError as e:
            logger.error(
                f"Failed to download file from {url}. Status: {e.status}", exc_info=True
            )
            raise PerseusException(
                f"Failed to download file. Status: {e.status}, " f"Message: {e.message}"
            ) from e
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during file download: {e}", exc_info=True
            )
            raise PerseusException(
                f"An unexpected error occurred during file download: {e}"
            ) from e
        logger.debug(f"File downloaded successfully to {output_path}")

    async def run_job_async(
        self,
        job_id: str,
        polling_interval: int = 5,
        timeout: int = 3600,
    ) -> Job:
        """
        Asynchronously waits for a job to complete by polling its status.
        """
        logger.info(f"Waiting for job {job_id} to complete.")
        start_time = time()

        job = await self.find_job_async(job_id)
        if not job:
            raise PerseusException(f"Could not find job with id {job_id}")

        while job.status not in [JobStatus.SUCCEEDED, JobStatus.FAILED]:
            if time() - start_time > timeout:
                raise PerseusException(f"Timeout reached for job {job.id}")

            await asyncio.sleep(polling_interval)

            updated_job = await self.find_job_async(job.id)
            if not updated_job:
                raise PerseusException(f"Could not find job {job.id} during polling.")
            if updated_job.status != job.status:
                logger.debug(f"Job {job.id} status changed to: {updated_job.status}")

            job = updated_job

        if job.status == JobStatus.FAILED:
            logger.error(f"Job {job.id} failed.")
            raise PerseusException(f"Job {job.id} failed.")

        logger.info(f"Job {job.id} completed successfully.")
        return job
