from unittest.mock import AsyncMock, patch, MagicMock, mock_open
import pytest
import aiohttp
from perseus_client.client import PerseusClient
from perseus_client.models import JobStatus, KnowledgeGraph
from perseus_client.exceptions import PerseusException, APIException
from faker import Faker
from time import time

fake = Faker()


@pytest.mark.asyncio
async def test_submit_job(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test submitting a job.
    """
    file_id = "file1"
    ontology_id = "ontology1"

    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "job": {"id": "job1", "status": "PENDING"}
    }
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = (
        200
    )

    result = await client.job.submit_job_async(file_id=file_id, ontology_id=ontology_id)

    assert result.id == "job1"
    assert result.status == JobStatus.PENDING
    mock_aiohttp_session_instance.request.assert_called_once_with(
        "POST",
        f"{client.api_host}/api/v0/job/submit",
        json={"fileId": file_id, "ontologyId": ontology_id},
    )


@pytest.mark.asyncio
async def test_get_job(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test getting a job.
    """
    job_id = "job123"

    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "jobs": [{"id": job_id, "status": "SUCCEEDED"}]
    }
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = (
        200
    )

    result = await client.job.find_job_async(job_id)

    assert result.id == job_id
    assert result.status == JobStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_run_job_completion(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test running a job to completion.
    """
    job_id = "job123"
    file_id = "file1"

    # Mock submit_job_async
    submit_job_response = AsyncMock()
    submit_job_response.__aenter__.return_value.json.return_value = {
        "job": {"id": job_id, "status": "PENDING"}
    }
    submit_job_response.__aenter__.return_value.status = 200

    # Mock find_job_async (RUNNING)
    find_job_running_response = AsyncMock()
    find_job_running_response.__aenter__.return_value.json.return_value = {
        "jobs": [{"id": job_id, "status": "RUNNING"}]
    }
    find_job_running_response.__aenter__.return_value.status = 200

    # Mock find_job_async (SUCCEEDED)
    find_job_succeeded_response = AsyncMock()
    find_job_succeeded_response.__aenter__.return_value.json.return_value = {
        "jobs": [{"id": job_id, "status": "SUCCEEDED"}]
    }
    find_job_succeeded_response.__aenter__.return_value.status = 200

    # Mocks for download_job_output_async - _get_download_urls_async
    download_urls_response = AsyncMock()
    download_urls_response.__aenter__.return_value.json.return_value = {
        "ttlFileDownloadUrl": "http://ttl.test",
        "cqlFileDownloadUrl": "http://cql.test",
    }
    download_urls_response.__aenter__.return_value.status = 200

    mock_aiohttp_session_instance.request.side_effect = [
        find_job_running_response,
        find_job_succeeded_response,
        download_urls_response,
    ]

    with patch("aiohttp.ClientSession") as MockDownloadClientSession:
        mock_download_session_instance = MockDownloadClientSession.return_value
        mock_download_session_instance.get = AsyncMock()

        # Mock for TTL file download
        mock_ttl_response = AsyncMock()
        mock_ttl_response.__aenter__.return_value.raise_for_status.return_value = None
        mock_ttl_response.__aenter__.return_value.content.read.side_effect = [
            b"ttl_content",
            b"",
        ]

        # Mock for CQL file download
        mock_cql_response = AsyncMock()
        mock_cql_response.__aenter__.return_value.raise_for_status.return_value = None
        mock_cql_response.__aenter__.return_value.content.read.side_effect = [
            b"cql_content",
            b"",
        ]

        mock_download_session_instance.get.side_effect = [
            mock_ttl_response,
            mock_cql_response,
        ]

        result = await client.job.run_job_async(job_id=job_id, polling_interval=0.01)

        assert result.id == job_id
        assert result.status == JobStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_run_job_timeout(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test that run_job_async times out correctly.
    """
    job_id = "job_timeout"

    # Mock find_job_async to always return RUNNING
    find_job_running_response = AsyncMock()
    find_job_running_response.__aenter__.return_value.json.return_value = {
        "jobs": [{"id": job_id, "status": "RUNNING"}]
    }
    find_job_running_response.__aenter__.return_value.status = 200

    mock_aiohttp_session_instance.request.side_effect = [
        find_job_running_response,
        *[
            find_job_running_response for _ in range(15)
        ],  # Increased to cover more polling attempts
    ]

    start_time = time()
    with pytest.raises(PerseusException, match=f"Timeout reached for job {job_id}"):
        await client.job.run_job_async(
            job_id=job_id, timeout=0.1, polling_interval=0.01
        )
    end_time = time()

    assert end_time - start_time >= 0.1


@pytest.mark.asyncio
async def test_run_job_failure(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test that run_job_async handles a failed job.
    """
    job_id = "job_fail"

    # Mock find_job_async to return FAILED immediately
    find_job_failed_response = AsyncMock()
    find_job_failed_response.__aenter__.return_value.json.return_value = {
        "jobs": [{"id": job_id, "status": "FAILED"}]
    }
    find_job_failed_response.__aenter__.return_value.status = 200

    mock_aiohttp_session_instance.request.side_effect = [
        find_job_failed_response,
    ]

    with pytest.raises(PerseusException, match=f"Job {job_id} failed."):
        await client.job.run_job_async(
            job_id=job_id, timeout=0.1, polling_interval=0.01
        )


@pytest.mark.asyncio
async def test_find_latest_job_async_no_job(
    client: PerseusClient, mock_aiohttp_session_instance
):
    """
    Test that find_latest_job_async returns None when no jobs are found.
    """
    file_id = "non_existent_file"
    ontology_id = "non_existent_ontology"

    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "jobs": []
    }
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = (
        200
    )

    result = await client.job.find_latest_job_async(
        file_id=file_id, ontology_id=ontology_id
    )
    assert result is None
    mock_aiohttp_session_instance.request.assert_called_once_with(
        "POST",
        f"{client.api_host}/api/v0/job/find",
        params={"limit": 1, "orderBy": "createdAt", "orderDirection": "DESC"},
        json={"fileId": file_id, "ontologyId": ontology_id},
    )


@pytest.mark.asyncio
async def test_find_latest_job_async_existing_job_running(
    client: PerseusClient, mock_aiohttp_session_instance
):
    """
    Test that find_latest_job_async returns an existing running job.
    """
    file_id = "file123"
    ontology_id = "ontology123"
    job_id = "job_running"

    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "jobs": [{"id": job_id, "status": "RUNNING"}]
    }
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = (
        200
    )

    result = await client.job.find_latest_job_async(
        file_id=file_id, ontology_id=ontology_id
    )
    assert result.id == job_id
    assert result.status == JobStatus.RUNNING


@pytest.mark.asyncio
async def test_find_latest_job_async_existing_job_succeeded_no_refresh(
    client: PerseusClient, mock_aiohttp_session_instance
):
    """
    Test that find_latest_job_async returns an existing succeeded job when refresh_graph=False.
    """
    file_id = "file_succeeded"
    ontology_id = "ontology_succeeded"
    job_id = "job_succeeded"

    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "jobs": [{"id": job_id, "status": "SUCCEEDED"}]
    }
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = (
        200
    )

    result = await client.job.find_latest_job_async(
        file_id=file_id, ontology_id=ontology_id
    )
    assert result.id == job_id
    assert result.status == JobStatus.SUCCEEDED


@pytest.mark.asyncio
async def test_download_job_output_async_missing_urls(client: PerseusClient):
    """
    Test download_job_output_async when no download URLs are provided by the API.
    """
    job_id = "job_no_urls"
    output_path = "/tmp/test_output"

    # Mock _get_download_urls_async to return no URLs
    with (
        patch.object(
            client.job,
            "_get_download_urls_async",
            new_callable=AsyncMock,
            return_value={},
        ) as mock_get_urls,
        patch.object(
            client.job, "_download_file_async", new_callable=AsyncMock
        ) as mock_download,
    ):

        await client.job.download_job_output_async(job_id, output_path)

        mock_get_urls.assert_called_once_with(job_id)
        mock_download.assert_not_called()



@pytest.mark.asyncio
async def test_submit_job_api_error(
    client: PerseusClient, mock_aiohttp_session_instance
):
    """
    Test that submit_job_async raises an APIException on API error.
    """
    file_id = "file1"
    ontology_id = "ontology1"

    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = (
        500
    )
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "message": "Internal Server Error"
    }

    with pytest.raises(APIException):
        await client.job.submit_job_async(file_id=file_id, ontology_id=ontology_id)


@pytest.mark.asyncio
async def test_find_job_api_error(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test that find_job_async raises an APIException on API error.
    """
    job_id = "job123"

    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = (
        404
    )
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "message": "Not Found"
    }

    with pytest.raises(APIException):
        await client.job.find_job_async(job_id)


@pytest.mark.asyncio
async def test_wait_for_tasks(client: PerseusClient):
    """
    Test the _wait_for_tasks method.
    """

    async def dummy_task(i):
        return i

    tasks = [dummy_task(i) for i in range(3)]
    descriptions = [f"Task {i}" for i in range(3)]

    results = await client.job._wait_for_tasks(tasks, descriptions)
    assert results == [0, 1, 2]
