import pytest
from unittest.mock import patch, MagicMock, AsyncMock
import perseus_client
from perseus_client.models import KnowledgeGraph, File, Job, Ontology

@patch('perseus_client._get_client')
def test_build_graph(mock_get_client):
    """
    Test the top-level build_graph function.
    """
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    file_paths = ["test.txt"]
    ontology_path = "test.ttl"
    metadata = {"test": "test"}
    
    perseus_client.build_graph(file_paths=file_paths, ontology_path=ontology_path, metadata=metadata, refresh_graph=True)
    
    mock_client.build_graph.assert_called_once_with(
        file_paths=file_paths,
        ontology_path=ontology_path,
        metadata=metadata,
        refresh_graph=True
    )

@patch('perseus_client._get_client')
def test_upload_file(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    file_path = "test.txt"
    
    perseus_client.upload_file(file_path=file_path)
    
    mock_client.file.upload_file.assert_called_once_with(file_path)

@patch('perseus_client._get_client')
def test_find_files(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    ids = ["1", "2"]
    source_hashes = ["a", "b"]
    
    perseus_client.find_files(ids=ids, source_hashes=source_hashes)
    
    mock_client.file.find_files.assert_called_once_with(ids, source_hashes)

@patch('perseus_client._get_client')
def test_find_file(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    file_id = "1"
    
    perseus_client.find_file(id=file_id)
    
    mock_client.file.find_file.assert_called_once_with(file_id)

@patch('perseus_client._get_client')
def test_delete_file(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    file_id = "1"
    
    perseus_client.delete_file(file_id=file_id)
    
    mock_client.file.delete_file.assert_called_once_with(file_id)

@patch('perseus_client._get_client')
def test_wait_for_file_upload(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    file_id = "1"
    
    perseus_client.wait_for_file_upload(file_id=file_id)
    
    mock_client.file.wait_for_file_upload.assert_called_once_with(file_id, 0.5, 3600)

@patch('perseus_client._get_client')
def test_submit_job(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    file_id = "1"
    ontology_id = "2"
    
    perseus_client.submit_job(file_id=file_id, ontology_id=ontology_id)
    
    mock_client.job.submit_job.assert_called_once_with(file_id, ontology_id)

@patch('perseus_client._get_client')
def test_find_jobs(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    ids = ["1", "2"]
    
    perseus_client.find_jobs(ids=ids)
    
    mock_client.job.find_jobs.assert_called_once_with(ids)

@patch('perseus_client._get_client')
def test_find_job(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    job_id = "1"
    
    perseus_client.find_job(id=job_id)
    
    mock_client.job.find_job.assert_called_once_with(job_id)

@patch('perseus_client._get_client')
def test_find_latest_succeeded_job(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    file_id = "1"
    ontology_id = "2"
    
    perseus_client.find_latest_succeeded_job(file_id=file_id, ontology_id=ontology_id)
    
    mock_client.job.find_latest_succeeded_job.assert_called_once_with(file_id, ontology_id)

@patch('perseus_client._get_client')
def test_find_latest_job(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    file_id = "1"
    ontology_id = "2"
    
    perseus_client.find_latest_job(file_id=file_id, ontology_id=ontology_id)
    
    mock_client.job.find_latest_job.assert_called_once_with(file_id, ontology_id)

@patch('perseus_client._get_client')
def test_download_job_output(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    job_id = "1"
    output_path = "/tmp/test"
    
    perseus_client.download_job_output(job_id=job_id, output_path=output_path)
    
    mock_client.job.download_job_output.assert_called_once_with(job_id, output_path)

@patch('perseus_client._get_client')
def test_run_job(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    job_id = "1"
    
    perseus_client.run_job(job_id=job_id)
    
    mock_client.job.run_job.assert_called_once_with(job_id, 5, 3600)

@patch('perseus_client._get_client')
def test_upload_ontology(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    ontology_path = "test.ttl"
    
    perseus_client.upload_ontology(ontology_path=ontology_path)
    
    mock_client.ontology.upload_ontology.assert_called_once_with(ontology_path)

@patch('perseus_client._get_client')
def test_find_ontologies(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    ids = ["1", "2"]
    source_hashes = ["a", "b"]
    
    perseus_client.find_ontologies(ids=ids, source_hashes=source_hashes)
    
    mock_client.ontology.find_ontologies.assert_called_once_with(ids, source_hashes)

@patch('perseus_client._get_client')
def test_find_ontology(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    ontology_id = "1"
    
    perseus_client.find_ontology(id=ontology_id)
    
    mock_client.ontology.find_ontology.assert_called_once_with(ontology_id)

@patch('perseus_client._get_client')
def test_delete_ontology(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    ontology_id = "1"
    
    perseus_client.delete_ontology(ontology_id=ontology_id)
    
    mock_client.ontology.delete_ontology.assert_called_once_with(ontology_id)

@patch('perseus_client._get_client')
def test_wait_for_ontology_upload(mock_get_client):
    mock_client = MagicMock()
    mock_get_client.return_value = mock_client
    
    ontology_id = "1"
    
    perseus_client.wait_for_ontology_upload(ontology_id=ontology_id)
    
    mock_client.ontology.wait_for_ontology_upload.assert_called_once_with(ontology_id, 0.5, 3600)

@patch('perseus_client._client', MagicMock())
def test_close():
    """
    Test the top-level close function.
    """
    # Call the close function which should use the patched _client
    perseus_client.close()

    # Assert that the close method on the patched client was called
    perseus_client._client.close.assert_called_once()

@patch('perseus_client._client', None)
def test_close_with_no_client():
    """
    Test that calling close when no client has been initialized does not raise an error.
    """
    try:
        perseus_client.close()
    except Exception as e:
        pytest.fail(f"perseus_client.close() raised an exception when no client was initialized: {e}")

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_build_graph_async(mock_perseus_client):
    """
    Test the top-level build_graph_async function.
    """
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    file_paths = ["test.txt"]
    ontology_path = "test.ttl"
    metadata = {"test": "test"}
    
    await perseus_client.build_graph_async(file_paths=file_paths, ontology_path=ontology_path, metadata=metadata, refresh_graph=True)
    
    mock_client_instance.build_graph_async.assert_called_once_with(
        file_paths=file_paths,
        ontology_path=ontology_path,
        metadata=metadata,
        refresh_graph=True
    )

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_interlink_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    kbs = []
    
    await perseus_client.interlink_async(kbs=kbs)
    
    mock_client_instance.interlink_async.assert_called_once_with(
        kbs=kbs,
        interlinking_key_uris=['http://www.w3.org/2000/01/rdf-schema#label'],
        immutable_properties=None,
        merge_properties_on_conflict=False
    )

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_upload_file_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    file_path = "test.txt"
    
    await perseus_client.upload_file_async(file_path=file_path)
    
    mock_client_instance.file.upload_file_async.assert_called_once_with(file_path)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_find_files_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    ids = ["1", "2"]
    source_hashes = ["a", "b"]
    
    await perseus_client.find_files_async(ids=ids, source_hashes=source_hashes)
    
    mock_client_instance.file.find_files_async.assert_called_once_with(ids, source_hashes)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_find_file_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    file_id = "1"
    
    await perseus_client.find_file_async(id=file_id)
    
    mock_client_instance.file.find_file_async.assert_called_once_with(file_id)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_delete_file_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    file_id = "1"
    
    await perseus_client.delete_file_async(file_id=file_id)
    
    mock_client_instance.file.delete_file_async.assert_called_once_with(file_id)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_wait_for_file_upload_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    file_id = "1"
    
    await perseus_client.wait_for_file_upload_async(file_id=file_id)
    
    mock_client_instance.file.wait_for_file_upload_async.assert_called_once_with(file_id, 0.5, 3600)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_submit_job_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    file_id = "1"
    ontology_id = "2"
    
    await perseus_client.submit_job_async(file_id=file_id, ontology_id=ontology_id)
    
    mock_client_instance.job.submit_job_async.assert_called_once_with(file_id, ontology_id)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_find_jobs_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    ids = ["1", "2"]
    
    await perseus_client.find_jobs_async(ids=ids)
    
    mock_client_instance.job.find_jobs_async.assert_called_once_with(ids)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_find_job_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    job_id = "1"
    
    await perseus_client.find_job_async(id=job_id)
    
    mock_client_instance.job.find_job_async.assert_called_once_with(job_id)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_find_latest_succeeded_job_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    file_id = "1"
    ontology_id = "2"
    
    await perseus_client.find_latest_succeeded_job_async(file_id=file_id, ontology_id=ontology_id)
    
    mock_client_instance.job.find_latest_succeeded_job_async.assert_called_once_with(file_id, ontology_id)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_find_latest_job_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    file_id = "1"
    ontology_id = "2"
    
    await perseus_client.find_latest_job_async(file_id=file_id, ontology_id=ontology_id)
    
    mock_client_instance.job.find_latest_job_async.assert_called_once_with(file_id, ontology_id)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_download_job_output_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    job_id = "1"
    output_path = "/tmp/test"
    
    await perseus_client.download_job_output_async(job_id=job_id, output_path=output_path)
    
    mock_client_instance.job.download_job_output_async.assert_called_once_with(job_id, output_path)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_run_job_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    job_id = "1"
    
    await perseus_client.run_job_async(job_id=job_id)
    
    mock_client_instance.job.run_job_async.assert_called_once_with(job_id, 5, 3600)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_upload_ontology_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    ontology_path = "test.ttl"
    
    await perseus_client.upload_ontology_async(ontology_path=ontology_path)
    
    mock_client_instance.ontology.upload_ontology_async.assert_called_once_with(ontology_path)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_find_ontologies_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    ids = ["1", "2"]
    source_hashes = ["a", "b"]
    
    await perseus_client.find_ontologies_async(ids=ids, source_hashes=source_hashes)
    
    mock_client_instance.ontology.find_ontologies_async.assert_called_once_with(ids, source_hashes)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_find_ontology_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    ontology_id = "1"
    
    await perseus_client.find_ontology_async(id=ontology_id)
    
    mock_client_instance.ontology.find_ontology_async.assert_called_once_with(ontology_id)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_delete_ontology_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    ontology_id = "1"
    
    await perseus_client.delete_ontology_async(ontology_id=ontology_id)
    
    mock_client_instance.ontology.delete_ontology_async.assert_called_once_with(ontology_id)

@pytest.mark.asyncio
@patch('perseus_client.PerseusClient')
async def test_wait_for_ontology_upload_async(mock_perseus_client):
    mock_client_instance = AsyncMock()
    mock_perseus_client.return_value.__aenter__.return_value = mock_client_instance
    
    ontology_id = "1"
    
    await perseus_client.wait_for_ontology_upload_async(ontology_id=ontology_id)
    
    mock_client_instance.ontology.wait_for_ontology_upload_async.assert_called_once_with(ontology_id, 0.5, 3600)
