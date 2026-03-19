import pytest
from unittest.mock import patch, MagicMock
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
