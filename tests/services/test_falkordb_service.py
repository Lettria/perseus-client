import asyncio
from unittest.mock import MagicMock, patch, mock_open
import pytest
import logging

from perseus_client.services.falkordb_service import FalkorDBService
from perseus_client.models import KnowledgeGraph, Entity, Relation
from perseus_client.exceptions import ConfigurationException

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@patch("builtins.open", new_callable=mock_open, read_data="CREATE (n:Test)")
@patch(
    "perseus_client.services.falkordb_service.FalkorDBService.execute_cql_string_async"
)
async def test_save_output_to_falkordb_async_from_file(mock_execute, mock_file):
    """
    Test save_output_to_falkordb_async reads a file and executes the content.
    """
    # Arrange
    service = FalkorDBService(asyncio.get_event_loop())
    file_path = "test.cql"
    # Act
    await service.save_output_to_falkordb_async(file_path)
    # Assert
    mock_file.assert_called_once_with(file_path, "r", encoding="utf-8")
    mock_execute.assert_called_once_with("CREATE (n:Test)")


@pytest.mark.asyncio
@patch("asyncio.to_thread")
@patch("perseus_client.services.falkordb_service.FalkorDB")
async def test_execute_cql_string_async(mock_falkordb, mock_to_thread):
    """
    Test execute_cql_string_async executes a CQL string.
    """

    service = FalkorDBService(asyncio.get_event_loop())
    service._falkordb_ready = False  # Skip waiting for FalkorDB to be ready
    mock_driver = MagicMock()
    mock_graph = MagicMock()
    mock_falkordb.return_value = mock_driver
    mock_driver.select_graph.return_value = mock_graph
    cql_query = "CREATE (n:Test)"
    # # Act
    await service.execute_cql_string_async(cql_query)
    # # Assert
    mock_driver.select_graph.assert_called_once()
    assert mock_to_thread.call_args_list[1][0][1] == cql_query
    assert mock_to_thread.call_count == 2  # 1 for ping, 1 for query


@pytest.mark.asyncio
@patch(
    "perseus_client.services.falkordb_service.FalkorDBService.execute_cql_string_async"
)
async def test_save_to_falkordb_async_from_kg(mock_execute):
    """
    Test save_to_falkordb_async converts a KnowledgeGraph and executes the CQL.
    """
    # Arrange
    from perseus_client.models import LiteralValue

    service = FalkorDBService(asyncio.get_event_loop())
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="e1", types=["Test"], properties={"p1": LiteralValue(value="v1")}
            )
        ],
        relations=[
            Relation(
                source_uri="e1",
                target_uri="e1",
                predicate="REL",
                properties={"p2": LiteralValue(value="v2")},
            )
        ],
    )
    # Act
    await service.save_to_falkordb_async(kg)
    # Assert
    assert mock_execute.call_args[0][0].startswith("MERGE")


@pytest.mark.asyncio
@patch(
    "perseus_client.services.falkordb_service.FalkorDBService._wait_for_falkordb_async",
)
async def test_execute_cql_string_async_config_exception(mock_wait):
    """
    Test execute_cql_string_async with incomplete configuration.
    """
    service = FalkorDBService(asyncio.get_event_loop())

    with patch('perseus_client.services.falkordb_service.settings') as mock_settings:
        mock_settings.falkordb_host = ""
        mock_settings.falkordb_port = "6379"
        mock_settings.falkordb_graph_name = "test"


        with pytest.raises(ConfigurationException):
            await service.execute_cql_string_async("CREATE (n:Test)")


@pytest.mark.asyncio
@patch("perseus_client.services.falkordb_service.FALKORDB_AVAILABLE", False)
async def test_missing_falkordb_library():
    """
    Test that an ImportError is raised if the falkordb library is not available.
    """
    service = FalkorDBService(asyncio.get_event_loop())
    with pytest.raises(ImportError, match="The 'FalkorDB' library is not installed."):
        await service.execute_cql_string_async("CREATE (n:Test)")


@pytest.mark.asyncio
async def test_file_not_found():
    """
    Test that FileNotFoundError is raised when the input file does not exist.
    """
    service = FalkorDBService(asyncio.get_event_loop())
    with pytest.raises(FileNotFoundError):
        await service.save_output_to_falkordb_async("non_existent_file.cql")


@patch(
    "perseus_client.services.falkordb_service.FalkorDBService.execute_cql_string_async",
    new_callable=MagicMock,
)
def test_empty_knowledge_graph(mock_execute):
    """
    Test that execute_cql_string_async is not called when the KnowledgeGraph is empty.
    """
    service = FalkorDBService(MagicMock())
    kg = KnowledgeGraph(entities=[], relations=[])
    service.save_to_falkordb(kg)
    mock_execute.assert_not_called()




