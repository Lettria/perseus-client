import asyncio
from unittest.mock import MagicMock, patch, mock_open, AsyncMock
import pytest
import logging

from perseus_client.services.neo4j_service import Neo4jService, NEO4J_AVAILABLE
from perseus_client.models import KnowledgeGraph, Entity, Relation
from perseus_client.exceptions import ConfigurationException

logger = logging.getLogger(__name__)


@pytest.mark.asyncio
@patch("builtins.open", new_callable=mock_open, read_data="CREATE (n:Test)")
@patch(
    "perseus_client.services.neo4j_service.Neo4jService.execute_cql_string_async"
)
async def test_save_output_to_neo4j_async_from_file(mock_execute, mock_file):
    """
    Test save_output_to_neo4j_async reads a file and executes the content.
    """
    # Arrange
    service = Neo4jService(asyncio.get_event_loop())
    file_path = "test.cql"
    # Act
    await service.save_output_to_neo4j_async(file_path)
    # Assert
    mock_file.assert_called_once_with(file_path, "r", encoding="utf-8")
    mock_execute.assert_called_once_with("CREATE (n:Test)")


@pytest.mark.asyncio
@pytest.mark.skipif(not NEO4J_AVAILABLE, reason="neo4j not installed")
@patch(
    "perseus_client.services.neo4j_service.Neo4jService._wait_for_neo4j_async",
    new_callable=AsyncMock,
)
@patch("perseus_client.services.neo4j_service.GraphDatabase")
async def test_execute_cql_string_async(mock_db, mock_wait):
    """
    Test execute_cql_string_async executes a CQL string.
    """
    service = Neo4jService(asyncio.get_event_loop())

    mock_driver = mock_db.driver.return_value
    mock_driver.verify_connectivity.return_value = True
    mock_session = mock_driver.session.return_value.__enter__.return_value
    mock_session.run.return_value = None

    cql_query = "CREATE (n:Test {name: 'test'})"

    # Mock settings for Neo4j credentials
    with patch('perseus_client.services.neo4j_service.settings') as mock_settings:
        mock_settings.neo4j_uri = "bolt://localhost:7687"
        mock_settings.neo4j_user = "neo4j"
        mock_settings.neo4j_password = "password"

        await service.execute_cql_string_async(cql_query)

        mock_db.driver.assert_called_once_with(
            mock_settings.neo4j_uri,
            auth=(mock_settings.neo4j_user, mock_settings.neo4j_password),
        )
        mock_driver.verify_connectivity.assert_called_once()
        mock_session.run.assert_called_once_with("CREATE (n:Test {name: 'test'})")
        mock_driver.close.assert_called_once()


@pytest.mark.asyncio
@patch(
    "perseus_client.services.neo4j_service.Neo4jService.execute_cql_string_async"
)
async def test_save_to_neo4j_async_from_kg(mock_execute):
    """
    Test save_to_neo4j_async converts a KnowledgeGraph and executes the CQL.
    """
    # Arrange
    from perseus_client.models import LiteralValue

    service = Neo4jService(asyncio.get_event_loop())
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
    await service.save_to_neo4j_async(kg)
    # Assert
    assert mock_execute.call_args[0][0].startswith("MERGE")


@pytest.mark.asyncio
@pytest.mark.skipif(not NEO4J_AVAILABLE, reason="neo4j not installed")
@patch(
    "perseus_client.services.neo4j_service.Neo4jService._wait_for_neo4j_async",
    new_callable=AsyncMock,
)
async def test_execute_cql_string_async_config_exception(mock_wait, tmp_path):
    """
    Test execute_cql_string_async with incomplete configuration.
    """
    service = Neo4jService(asyncio.get_event_loop())

    with patch('perseus_client.services.neo4j_service.settings') as mock_settings:
        mock_settings.neo4j_uri = ""
        mock_settings.neo4j_user = "neo4j"
        mock_settings.neo4j_password = "password"

        with pytest.raises(ConfigurationException):
            await service.execute_cql_string_async("CREATE (n:Test)")


@pytest.mark.asyncio
@patch("perseus_client.services.neo4j_service.NEO4J_AVAILABLE", False)
async def test_missing_neo4j_library():
    """
    Test that an ImportError is raised if the neo4j library is not available.
    """
    service = Neo4jService(asyncio.get_event_loop())
    with pytest.raises(ImportError, match="The 'neo4j' library is not installed."):
        await service.execute_cql_string_async("CREATE (n:Test)")


@pytest.mark.asyncio
async def test_file_not_found():
    """
    Test that FileNotFoundError is raised when the input file does not exist.
    """
    service = Neo4jService(asyncio.get_event_loop())
    with pytest.raises(FileNotFoundError):
        await service.save_output_to_neo4j_async("non_existent_file.cql")


@patch(
    "perseus_client.services.neo4j_service.Neo4jService.execute_cql_string_async",
    new_callable=MagicMock,
)
def test_empty_knowledge_graph(mock_execute):
    """
    Test that execute_cql_string_async is not called when the KnowledgeGraph is empty.
    """
    service = Neo4jService(MagicMock())
    kg = KnowledgeGraph(entities=[], relations=[])
    service.save_to_neo4j(kg)
    mock_execute.assert_not_called()


