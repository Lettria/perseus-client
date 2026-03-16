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
