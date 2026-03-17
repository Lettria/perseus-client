import os
from unittest import mock
from perseus_client.client import PerseusClient
from perseus_client.services.file_service import FileService
from perseus_client.services.ontology_service import OntologyService
from perseus_client.services.job_service import JobService
from perseus_client.services.neo4j_service import Neo4jService
import importlib

def test_client_initialization(client):
    """
    Test that the client and its services are initialized correctly.
    """
    assert isinstance(client.file, FileService)
    assert isinstance(client.ontology, OntologyService)
    assert isinstance(client.job, JobService)
    assert isinstance(client.neo4j, Neo4jService)

@mock.patch.dict(os.environ, {"PERSEUS_API_KEY": "env_token", "PERSEUS_API_HOST": "https://env.test.com"}, clear=True)
def test_client_initialization_from_env():
    """
    Test that the client can be initialized from environment variables.
    """
    import perseus_client
    importlib.reload(perseus_client.config)
    importlib.reload(perseus_client.client)
    
    client = perseus_client.client.PerseusClient()
    assert client._perseus_api_key == "env_token"
    assert client.api_host == "https://env.test.com"

def test_client_file_property(client: PerseusClient):
    """
    Test that the 'file' property returns a FileService instance.
    """
    assert isinstance(client.file, FileService)

def test_client_job_property(client: PerseusClient):
    """
    Test that the 'job' property returns a JobService instance.
    """
    assert isinstance(client.job, JobService)

def test_client_ontology_property(client: PerseusClient):
    """
    Test that the 'ontology' property returns an OntologyService instance.
    """
    assert isinstance(client.ontology, OntologyService)

def test_client_neo4j_property(client: PerseusClient):
    """
    Test that the 'neo4j' property returns a Neo4jService instance.
    """
    assert isinstance(client.neo4j, Neo4jService)

def test_client_falkordb_property(client: PerseusClient):
    """
    Test that the 'falkordb' property returns a FalkorDBService instance.
    """
    from perseus_client.services.falkordb_service import FalkorDBService
    assert isinstance(client.falkordb, FalkorDBService)

def test_client_cql_property(client: PerseusClient):
    """
    Test that the 'cql' property returns a CQLService instance.
    """
    from perseus_client.services.cql_service import CQLService
    assert isinstance(client.cql, CQLService)

def test_client_ttl_property(client: PerseusClient):
    """
    Test that the 'ttl' property returns a TTLService instance.
    """
    from perseus_client.services.ttl_service import TTLService
    assert isinstance(client.ttl, TTLService)

def test_client_graph_property(client: PerseusClient):
    """
    Test that the 'graph' property returns a GraphService instance.
    """
    from perseus_client.services.graph_service import GraphService
    assert isinstance(client.graph, GraphService)

import pytest
from unittest.mock import AsyncMock, MagicMock, patch, mock_open
from perseus_client.models import KnowledgeGraph

@pytest.mark.asyncio
async def test_build_graph_async(client: PerseusClient):
    """
    Test the build_graph_async method.
    """
    file_path = ["test.txt"]
    ontology_path = "test.ttl"
    
    with patch.object(client.file, "upload_file_async", new_callable=AsyncMock) as mock_upload_file, \
         patch.object(client.file, "wait_for_file_upload_async", new_callable=AsyncMock) as mock_wait_file, \
         patch.object(client.ontology, "upload_ontology_async", new_callable=AsyncMock) as mock_upload_ontology, \
         patch.object(client.ontology, "wait_for_ontology_upload_async", new_callable=AsyncMock) as mock_wait_ontology, \
         patch.object(client.job, "find_latest_job_async", new_callable=AsyncMock) as mock_find_job, \
         patch.object(client.job, "submit_job_async", new_callable=AsyncMock) as mock_submit_job, \
         patch.object(client.job, "run_job_async", new_callable=AsyncMock) as mock_run_job, \
         patch.object(client.job, "download_job_output_async", new_callable=AsyncMock) as mock_download, \
         patch.object(client.ttl, "parse_ttl_to_knowledge_graph", new_callable=MagicMock) as mock_parse, \
         patch("builtins.open", new_callable=mock_open, read_data=""), \
         patch("os.path.exists", return_value=True):

        # Mock service responses
        mock_upload_file.return_value = MagicMock(id="file123", status="COMPLETED")
        mock_upload_ontology.return_value = MagicMock(id="onto123", status="COMPLETED")
        mock_find_job.return_value = None
        mock_submit_job.return_value = MagicMock(id="job123")
        mock_run_job.return_value = MagicMock(id="job123")
        mock_parse.return_value = KnowledgeGraph()
        
        await client.build_graph_async(file_path, ontology_path)
        
        mock_upload_file.assert_called_once_with("test.txt")
        mock_upload_ontology.assert_called_once_with("test.ttl")
        mock_submit_job.assert_called_once()
        mock_run_job.assert_called_once_with(job_id="job123")
        mock_download.assert_called_once()
        mock_parse.assert_called_once()
