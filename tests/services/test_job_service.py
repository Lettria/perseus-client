from unittest.mock import AsyncMock, patch
import pytest
from perseus_client.client import PerseusClient
from perseus_client.models import JobStatus
from perseus_client.exceptions import PerseusException
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

    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {"job": {"id": "job1", "status": "PENDING"}}
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 200

    result = await client.job.submit_job_async(file_id=file_id, ontology_id=ontology_id)
    
    assert result.id == "job1"
    assert result.status == JobStatus.PENDING
    mock_aiohttp_session_instance.request.assert_called_once_with(
        "POST",
        f"{client.api_host}/api/v0/job/submit",
        json={'fileId': file_id, 'ontologyId': ontology_id}
    )

@pytest.mark.asyncio
async def test_get_job(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test getting a job.
    """
    job_id = "job123"
    
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {"jobs": [{"id": job_id, "status": "SUCCEEDED"}]}
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 200

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
    submit_job_response.__aenter__.return_value.json.return_value = {"job": {"id": job_id, "status": "PENDING"}}
    submit_job_response.__aenter__.return_value.status = 200

    # Mock find_job_async (RUNNING)
    find_job_running_response = AsyncMock()
    find_job_running_response.__aenter__.return_value.json.return_value = {"jobs": [{"id": job_id, "status": "RUNNING"}]}
    find_job_running_response.__aenter__.return_value.status = 200

    # Mock find_job_async (SUCCEEDED)
    find_job_succeeded_response = AsyncMock()
    find_job_succeeded_response.__aenter__.return_value.json.return_value = {"jobs": [{"id": job_id, "status": "SUCCEEDED"}]}
    find_job_succeeded_response.__aenter__.return_value.status = 200

    # Mocks for download_job_output_async - _get_download_urls_async
    download_urls_response = AsyncMock()
    download_urls_response.__aenter__.return_value.json.return_value = {"ttlFileDownloadUrl": "http://ttl.test", "cqlFileDownloadUrl": "http://cql.test"}
    download_urls_response.__aenter__.return_value.status = 200

    mock_aiohttp_session_instance.request.side_effect = [
        find_job_running_response,
        find_job_succeeded_response,
        download_urls_response,
    ]

    with patch('aiohttp.ClientSession') as MockDownloadClientSession:
        mock_download_session_instance = MockDownloadClientSession.return_value
        mock_download_session_instance.get = AsyncMock()

        # Mock for TTL file download
        mock_ttl_response = AsyncMock()
        mock_ttl_response.__aenter__.return_value.raise_for_status.return_value = None
        mock_ttl_response.__aenter__.return_value.content.read.side_effect = [b"ttl_content", b""]
        
        # Mock for CQL file download
        mock_cql_response = AsyncMock()
        mock_cql_response.__aenter__.return_value.raise_for_status.return_value = None
        mock_cql_response.__aenter__.return_value.content.read.side_effect = [b"cql_content", b""]

        mock_download_session_instance.get.side_effect = [
            mock_ttl_response,
            mock_cql_response
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
    find_job_running_response.__aenter__.return_value.json.return_value = {"jobs": [{"id": job_id, "status": "RUNNING"}]}
    find_job_running_response.__aenter__.return_value.status = 200

    mock_aiohttp_session_instance.request.side_effect = [
        find_job_running_response,
        *[find_job_running_response for _ in range(15)], # Increased to cover more polling attempts
    ]

    start_time = time()
    with pytest.raises(PerseusException, match=f"Timeout reached for job {job_id}"):
        await client.job.run_job_async(job_id=job_id, timeout=0.1, polling_interval=0.01)
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
    find_job_failed_response.__aenter__.return_value.json.return_value = {"jobs": [{"id": job_id, "status": "FAILED"}]}
    find_job_failed_response.__aenter__.return_value.status = 200

    mock_aiohttp_session_instance.request.side_effect = [
        find_job_failed_response,
    ]

    with pytest.raises(PerseusException, match=f"Job {job_id} failed."):
        await client.job.run_job_async(job_id=job_id, timeout=0.1, polling_interval=0.01)
