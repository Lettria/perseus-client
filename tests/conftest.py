import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from perseus_client.client import PerseusClient
from perseus_client.config import Settings
from perseus_client.services.file_service import FileService
from perseus_client.services.job_service import JobService
from perseus_client.services.ontology_service import OntologyService
from perseus_client.services.neo4j_service import Neo4jService
from perseus_client.services.falkordb_service import FalkorDBService
from perseus_client.services.cql_service import CQLService
from perseus_client.services.ttl_service import TTLService
from perseus_client.services.graph_service import GraphService
import aiohttp
import asyncio

@pytest.fixture
def mock_token():
    """
    Fixture for providing a mock authentication token.
    """
    return "mock_token"

@pytest.fixture
def mock_api_url():
    """
    Fixture for providing a mock API URL.
    """
    return "https://perseus.test"

@pytest.fixture
def mock_aiohttp_session_instance():
    """
    Fixture for providing a mock aiohttp.ClientSession instance.
    """
    mock_session = AsyncMock(spec=aiohttp.ClientSession)
    mock_response_context = AsyncMock()
    mock_response_context.__aenter__.return_value = AsyncMock(spec=aiohttp.ClientResponse)
    mock_response_context.__aexit__.return_value = None
    mock_session.request.return_value = mock_response_context
    return mock_session

@pytest.fixture
def mock_event_loop():
    """
    Provides a mock event loop that can handle `run_until_complete`.
    """
    loop = MagicMock(spec=asyncio.AbstractEventLoop)
    loop.run_until_complete.side_effect = lambda coro: asyncio.get_event_loop().run_until_complete(coro)
    return loop

@pytest.fixture
def client(mock_token, mock_api_url, mock_aiohttp_session_instance, mock_event_loop):
    """
    Fixture for initializing the PerseusClient with a mock token, URL, aiohttp session, and a mock event loop.
    """
    with patch('perseus_client.client.settings') as mock_settings:
        mock_settings.perseus_api_host = mock_api_url
        mock_settings.perseus_api_key = mock_token
        
        client_instance = PerseusClient(api_host=mock_api_url)
        client_instance._session = mock_aiohttp_session_instance
        client_instance._loop = mock_event_loop
        
        # Patch _ensure_active to do nothing, as services are manually initialized
        client_instance._ensure_active = MagicMock()

        # Manually initialize services as __aenter__ is not called in this test context
        client_instance._file = FileService(client_instance._session, client_instance.api_host, client_instance._loop)
        client_instance._job = JobService(client_instance._session, client_instance.api_host, client_instance._loop)
        client_instance._ontology = OntologyService(client_instance._session, client_instance.api_host, client_instance._loop)
        client_instance._neo4j = Neo4jService(client_instance._loop)
        client_instance._falkordb = FalkorDBService(client_instance._loop)
        client_instance._cql = CQLService()
        client_instance._ttl = TTLService()
        client_instance._graph = GraphService()

        return client_instance