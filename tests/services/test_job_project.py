import pytest
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
from perseus_client.services.build_service import BuildService
from perseus_client.models import Job, JobStatus, File, FileStatus
import tempfile
import os
import asyncio
from datetime import datetime


@pytest.fixture
def mock_services():
    """Create mock services for BuildService."""
    file_service = MagicMock()
    job_service = MagicMock()
    ontology_service = MagicMock()
    ttl_service = MagicMock()
    cql_service = MagicMock()
    neo4j_service = MagicMock()
    falkordb_service = MagicMock()
    graph_service = MagicMock()
    interlink_service = MagicMock()
    rdflib_service = MagicMock()

    return {
        "file": file_service,
        "job": job_service,
        "ontology": ontology_service,
        "ttl": ttl_service,
        "cql": cql_service,
        "neo4j": neo4j_service,
        "falkordb": falkordb_service,
        "graph": graph_service,
        "interlink": interlink_service,
        "rdflib": rdflib_service,
    }


@pytest.fixture
def build_service(mock_services):
    """Create BuildService with mocked dependencies."""
    return BuildService(
        file_service=mock_services["file"],
        job_service=mock_services["job"],
        ontology_service=mock_services["ontology"],
        ttl_service=mock_services["ttl"],
        cql_service=mock_services["cql"],
        neo4j_service=mock_services["neo4j"],
        falkordb_service=mock_services["falkordb"],
        graph_service=mock_services["graph"],
        interlink_service=mock_services["interlink"],
        rdflib_service=mock_services["rdflib"],
    )


@pytest.mark.asyncio
async def test_project_id_set_when_provided(build_service, mock_services):
    """Test that project_id is set when explicitly provided."""
    # Setup mocks
    mock_file = File(
        id="file123",
        name="test.txt",
        status=FileStatus.UPLOADED,
        created_at=datetime.now()
    )
    mock_job = Job(id="job123", status=JobStatus.SUCCEEDED)

    mock_services["file"].upload_file_async = AsyncMock(return_value=mock_file)
    mock_services["job"].find_latest_job_async = AsyncMock(return_value=None)
    mock_services["job"].submit_job_async = AsyncMock(return_value=mock_job)
    mock_services["job"].update_job_project_async = AsyncMock(return_value=None)
    mock_services["job"].run_job_async = AsyncMock(return_value=mock_job)
    mock_services["job"].download_job_output_async = AsyncMock(return_value="output_path")
    async def wait_for_tasks_mock(tasks, descs):
        return await asyncio.gather(*tasks)
    mock_services["job"]._wait_for_tasks = wait_for_tasks_mock
    mock_services["ttl"].parse_ttl_to_knowledge_graph = MagicMock()
    mock_services["ttl"].to_ttl = MagicMock(return_value="ttl_content")
    mock_services["cql"].to_cql = MagicMock(return_value="cql_content")

    with patch("builtins.open", mock_open(read_data="test content")):
        with patch("os.path.exists", return_value=True):
            with patch("tempfile.mkdtemp", return_value="/tmp/test"):
                await build_service._build_single_graph_async(
                    file_path="test.txt",
                    project_id="project456",
                )

    # Verify update_job_project_async was called with the project_id
    mock_services["job"].update_job_project_async.assert_called_once_with(
        "job123", "project456"
    )


@pytest.mark.asyncio
async def test_project_id_set_to_none_when_explicitly_none(build_service, mock_services):
    """Test that project_id is set to None when explicitly None is passed."""
    # Setup mocks
    mock_file = File(
        id="file123",
        name="test.txt",
        status=FileStatus.UPLOADED,
        created_at=datetime.now()
    )
    mock_job = Job(id="job123", status=JobStatus.SUCCEEDED)

    mock_services["file"].upload_file_async = AsyncMock(return_value=mock_file)
    mock_services["job"].find_latest_job_async = AsyncMock(return_value=None)
    mock_services["job"].submit_job_async = AsyncMock(return_value=mock_job)
    mock_services["job"].update_job_project_async = AsyncMock(return_value=None)
    mock_services["job"].run_job_async = AsyncMock(return_value=mock_job)
    mock_services["job"].download_job_output_async = AsyncMock(return_value="output_path")
    async def wait_for_tasks_mock(tasks, descs):
        return await asyncio.gather(*tasks)
    mock_services["job"]._wait_for_tasks = wait_for_tasks_mock
    mock_services["ttl"].parse_ttl_to_knowledge_graph = MagicMock()
    mock_services["ttl"].to_ttl = MagicMock(return_value="ttl_content")
    mock_services["cql"].to_cql = MagicMock(return_value="cql_content")

    with patch("builtins.open", mock_open(read_data="test content")):
        with patch("os.path.exists", return_value=True):
            with patch("tempfile.mkdtemp", return_value="/tmp/test"):
                await build_service._build_single_graph_async(
                    file_path="test.txt",
                    project_id=None,
                )

    # Verify update_job_project_async was called with None
    mock_services["job"].update_job_project_async.assert_called_once_with(
        "job123", None
    )


@pytest.mark.asyncio
async def test_no_update_when_project_id_not_provided(build_service, mock_services):
    """Test that no update happens when project_id is not provided."""
    # Setup mocks
    mock_file = File(
        id="file123",
        name="test.txt",
        status=FileStatus.UPLOADED,
        created_at=datetime.now()
    )
    mock_job = Job(id="job123", status=JobStatus.SUCCEEDED)

    mock_services["file"].upload_file_async = AsyncMock(return_value=mock_file)
    mock_services["job"].find_latest_job_async = AsyncMock(return_value=None)
    mock_services["job"].submit_job_async = AsyncMock(return_value=mock_job)
    mock_services["job"].update_job_project_async = AsyncMock(return_value=None)
    mock_services["job"].run_job_async = AsyncMock(return_value=mock_job)
    mock_services["job"].download_job_output_async = AsyncMock(return_value="output_path")
    async def wait_for_tasks_mock(tasks, descs):
        return await asyncio.gather(*tasks)
    mock_services["job"]._wait_for_tasks = wait_for_tasks_mock
    mock_services["ttl"].parse_ttl_to_knowledge_graph = MagicMock()
    mock_services["ttl"].to_ttl = MagicMock(return_value="ttl_content")
    mock_services["cql"].to_cql = MagicMock(return_value="cql_content")

    with patch("builtins.open", mock_open(read_data="test content")):
        with patch("os.path.exists", return_value=True):
            with patch("tempfile.mkdtemp", return_value="/tmp/test"):
                # Call without project_id parameter (using default _NOT_PROVIDED)
                await build_service._build_single_graph_async(
                    file_path="test.txt",
                )

    # Verify update_job_project_async was NOT called
    mock_services["job"].update_job_project_async.assert_not_called()


@pytest.mark.asyncio
async def test_project_updated_for_reused_job(build_service, mock_services):
    """Test that project is updated even when reusing an existing job (refresh_graph=False)."""
    # Setup mocks - return an existing succeeded job
    mock_file = File(
        id="file123",
        name="test.txt",
        status=FileStatus.UPLOADED,
        created_at=datetime.now()
    )
    mock_existing_job = Job(id="job_existing", status=JobStatus.SUCCEEDED)

    mock_services["file"].upload_file_async = AsyncMock(return_value=mock_file)
    # Return existing job instead of None
    mock_services["job"].find_latest_job_async = AsyncMock(return_value=mock_existing_job)
    mock_services["job"].submit_job_async = AsyncMock()  # Should not be called
    mock_services["job"].update_job_project_async = AsyncMock(return_value=mock_existing_job)
    mock_services["job"].run_job_async = AsyncMock(return_value=mock_existing_job)
    mock_services["job"].download_job_output_async = AsyncMock(return_value="output_path")
    async def wait_for_tasks_mock(tasks, descs):
        return await asyncio.gather(*tasks)
    mock_services["job"]._wait_for_tasks = wait_for_tasks_mock
    mock_services["ttl"].parse_ttl_to_knowledge_graph = MagicMock()
    mock_services["ttl"].to_ttl = MagicMock(return_value="ttl_content")
    mock_services["cql"].to_cql = MagicMock(return_value="cql_content")

    with patch("builtins.open", mock_open(read_data="test content")):
        with patch("os.path.exists", return_value=True):
            with patch("tempfile.mkdtemp", return_value="/tmp/test"):
                await build_service._build_single_graph_async(
                    file_path="test.txt",
                    refresh_graph=False,  # Not refreshing, should reuse job
                    project_id="project789",
                )

    # Verify submit_job_async was NOT called (job was reused)
    mock_services["job"].submit_job_async.assert_not_called()

    # Verify update_job_project_async WAS called even though job was reused
    mock_services["job"].update_job_project_async.assert_called_once_with(
        "job_existing", "project789"
    )
