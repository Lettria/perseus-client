import os
from unittest import mock
from perseus_client.client import PerseusClient
from perseus_client.services.file_service import FileService
from perseus_client.services.ontology_service import OntologyService
from perseus_client.services.job_service import JobService
from perseus_client.services.neo4j_service import Neo4jService
import importlib
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

# ... (existing imports and other test functions)
from perseus_client.models import KnowledgeGraph
from perseus_client.services.build_service import BuildService

@pytest.mark.asyncio
async def test_build_graph_async(client: PerseusClient):
    """
    Test the build_graph_async method.
    """
    file_path = ["test.txt"]
    ontology_path = "test.ttl"
    metadata = {"test": "test"}
    
    mock_build_service = MagicMock(spec=BuildService)
    mock_build_service.build_graph_async = AsyncMock(return_value=[KnowledgeGraph()])

    with patch('perseus_client.client.PerseusClient.build', new_callable=PropertyMock) as mock_build_property:
        mock_build_property.return_value = mock_build_service
        
        await client.build_graph_async(file_paths=file_path, ontology_path=ontology_path, metadata=metadata)
        
        mock_build_service.build_graph_async.assert_called_once_with(
            file_paths=file_path,
            ontology_path=ontology_path,
            refresh_graph=False,
            metadata=metadata,
        )


@pytest.mark.asyncio
async def test_interlink_async(client: PerseusClient):
    """
    Test the interlink_async method.
    """
    kbs = [KnowledgeGraph()]
    
    mock_build_service = MagicMock(spec=BuildService)
    mock_build_service.interlink_async = AsyncMock(return_value=KnowledgeGraph())

    with patch('perseus_client.client.PerseusClient.build', new_callable=PropertyMock) as mock_build_property:
        mock_build_property.return_value = mock_build_service
        
        await client.interlink_async(kbs=kbs)
        
        mock_build_service.interlink_async.assert_called_once_with(
            kbs=kbs,
            interlinking_key_uris=["http://www.w3.org/2000/01/rdf-schema#label"],
            immutable_properties=None,
            merge_properties_on_conflict=False,
        )
