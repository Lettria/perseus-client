import pytest
from perseus_client.services.graph_service import GraphService

@pytest.fixture
def graph_service():
    return GraphService()

def test_graph_service_is_empty(graph_service):
    assert not hasattr(graph_service, 'interlink')
