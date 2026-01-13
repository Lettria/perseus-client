import os
from time import time
from typing import Any, Dict, List, Optional, cast
import aiohttp
import logging
import asyncio

from .base_service import BaseService
from ..models import Job, JobStatus
from ..exceptions import PerseusException

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
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
        file_id: str,
        ontology_id: Optional[str] = None,
        polling_interval: int = 5,
        timeout: int = 3600,
    ) -> Job:
        return self._loop.run_until_complete(
            self.run_job_async(file_id, ontology_id, polling_interval, timeout)
        )

    async def submit_job_async(
        self, file_id: str, ontology_id: Optional[str] = None
    ) -> Job:
        """
        Asynchronously submits a job for processing.
        """
        response = await self._request(
            "POST",
            "/api/v0/job/submit",
            json={"fileId": file_id, "ontologyId": ontology_id},
        )
        job_data = response["job"]
        return Job(id=job_data["id"], status=job_data["status"])

    async def find_jobs_async(self, ids: List[str]) -> List[Job]:
        """
        Asynchronously finds one or more jobs by their IDs.
        """
        response = await self._request("POST", "/api/v0/job/find", json={"ids": ids})
        return [Job(id=job["id"], status=job["status"]) for job in response["jobs"]]

    async def find_job_async(self, id: str) -> Optional[Job]:
        """
        Asynchronously finds a job by its ID.
        """
        jobs = await self.find_jobs_async(ids=[id])
        if not jobs:
            return None
        return jobs[0]

    async def find_latest_job_async(
        self, file_id: str, ontology_id: Optional[str] = None
    ) -> Optional[Job]:
        """
        Asynchronously finds the latest job by its file_id.
        """
        payload: Dict[str, Any] = {"fileId": file_id, "status": JobStatus.SUCCEEDED}
        if ontology_id:
            payload["ontologyId"] = ontology_id
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
            return None
        job_data = response["jobs"][0]
        return Job(id=job_data["id"], status=job_data["status"])

    async def download_job_output_async(
        self, job_id: str, output_path: Optional[str] = None
    ) -> str:
        """
        Asynchronously fetches a presigned URL and downloads the job output.
        """
        if output_path is None:
            output_path = f"{job_id}.output"
        download_urls = await self._get_download_urls_async(job_id)
        async with aiohttp.ClientSession() as download_session:
            await self._download_file_async(
                download_session,
                download_urls["ttlFileDownloadUrl"],
                f"{output_path}.ttl",
            )
            await self._download_file_async(
                download_session,
                download_urls["cqlFileDownloadUrl"],
                f"{output_path}.cql",
            )
        return output_path

    async def _get_download_urls_async(self, job_id: str) -> Dict[str, str]:
        """
        Asynchronously fetches a presigned URL to download the output of a job.
        """
        response = await self._request("GET", f"/api/v0/job/{job_id}/download-output")
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
        logger.debug(f"Downloading file from {url}")
        logger.info(f"Downloading file to {output_path}")
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                with open(output_path, "wb") as f:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f.write(chunk)
        except aiohttp.ClientResponseError as e:
            raise PerseusException(
                f"Failed to download file. Status: {e.status}, " f"Message: {e.message}"
            ) from e
        except Exception as e:
            raise PerseusException(
                f"An unexpected error occurred during file download: {e}"
            ) from e
        logger.info(f"File downloaded successfully to {output_path}")

    async def run_job_async(
        self,
        file_id: str,
        ontology_id: Optional[str] = None,
        polling_interval: int = 5,
        timeout: int = 3600,
    ) -> Job:
        """
        Asynchronously submits a job and polls for its completion with a terminal spinner.
        """
        job = await self.submit_job_async(file_id, ontology_id)
        logger.debug(f"Job {job.id} submitted, status: {job.status}")

        job = await self._wait_with_spinner(
            wait_message=f"Waiting for job {job.id}...",
            polling_fct=self.find_job_async,
            polling_fct_args=[job.id],
            status_attribute="status",
            end_statuses=[JobStatus.SUCCEEDED, JobStatus.FAILED],
            polling_interval=polling_interval,
            timeout=timeout,
        )

        if job.status == JobStatus.FAILED:
            raise PerseusException(f"Job {job.id} failed.")
        return job
