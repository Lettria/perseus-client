import asyncio
from unittest.mock import MagicMock, patch, mock_open
import pytest

from perseus_client.services.spanner_graph_service import (
    SpannerGraphService,
    SPANNER_AVAILABLE,
    BATCH_SIZE,
    _StatementError,
)
from perseus_client.models import KnowledgeGraph, Entity, Relation, LiteralValue
from perseus_client.exceptions import ConfigurationException, PerseusException


def _sample_kg():
    return KnowledgeGraph(
        entities=[
            Entity(uri="e1", types=["Test"], properties={"p1": LiteralValue(value="v1")})
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


@pytest.mark.asyncio
@patch("builtins.open", new_callable=mock_open, read_data="INSERT INTO T (id) VALUES ('a')")
@patch(
    "perseus_client.services.spanner_graph_service.SpannerGraphService.execute_spgl_string_async"
)
async def test_save_output_to_spanner_graph_async_from_file(mock_execute, mock_file):
    service = SpannerGraphService(asyncio.get_event_loop())
    await service.save_output_to_spanner_graph_async("test.spgl")
    mock_file.assert_called_once_with("test.spgl", "r", encoding="utf-8")
    mock_execute.assert_called_once_with("INSERT INTO T (id) VALUES ('a')")


@pytest.mark.asyncio
@pytest.mark.skipif(not SPANNER_AVAILABLE, reason="google-cloud-spanner not installed")
async def test_execute_spgl_string_async_batches():
    """
    Statements are run in order, in one transaction per batch.
    """
    service = SpannerGraphService(asyncio.get_event_loop())
    database = MagicMock()
    service._database = database  # Skip connecting to Spanner
    statements = [f"INSERT INTO T (id) VALUES ('{i}')" for i in range(BATCH_SIZE + 1)]

    await service.execute_spgl_string_async(";\n".join(statements) + ";")

    calls = database.run_in_transaction.call_args_list
    assert len(calls) == 2
    assert calls[0][0][1] == statements[:BATCH_SIZE]
    assert calls[1][0][1] == statements[BATCH_SIZE:]


def test_execute_batch_raises_on_failed_statement():
    transaction = MagicMock()
    status = MagicMock(code=3, message="bad statement")
    transaction.batch_update.return_value = (status, [1])

    with pytest.raises(_StatementError) as error:
        SpannerGraphService._execute_batch(transaction, ["first", "second"])
    assert error.value.index == 1
    assert error.value.message == "bad statement"


@pytest.mark.asyncio
@pytest.mark.skipif(not SPANNER_AVAILABLE, reason="google-cloud-spanner not installed")
async def test_execute_spgl_string_async_skips_failed_statement(caplog):
    """
    Like Neo4j and FalkorDB, a failing statement is logged and the others are still run:
    the batch is run again without it.
    """
    service = SpannerGraphService(asyncio.get_event_loop())
    database = MagicMock()
    service._database = database  # Skip connecting to Spanner
    batches = []

    def run_in_transaction(func, batch):
        batches.append(list(batch))
        if "bad" in batch:
            raise _StatementError(batch.index("bad"), "bad statement")

    database.run_in_transaction.side_effect = run_in_transaction

    await service.execute_spgl_string_async("first;\nbad;\nlast;")

    assert batches == [["first", "bad", "last"], ["first", "last"]]
    assert "bad statement" in caplog.text


@pytest.mark.asyncio
@patch(
    "perseus_client.services.spanner_graph_service.SpannerGraphService.execute_spgl_string_async"
)
async def test_save_to_spanner_graph_async_from_kg(mock_execute):
    service = SpannerGraphService(asyncio.get_event_loop())
    await service.save_to_spanner_graph_async(_sample_kg())
    assert mock_execute.call_args[0][0].startswith("INSERT OR IGNORE INTO")


@pytest.mark.asyncio
@pytest.mark.skipif(not SPANNER_AVAILABLE, reason="google-cloud-spanner not installed")
async def test_execute_spgl_string_async_config_exception():
    service = SpannerGraphService(asyncio.get_event_loop())

    with patch("perseus_client.services.spanner_graph_service.settings") as mock_settings:
        mock_settings.spanner_project_id = "project"
        mock_settings.spanner_instance_id = ""
        mock_settings.spanner_database_id = "database"

        with pytest.raises(ConfigurationException):
            await service.execute_spgl_string_async("INSERT INTO T (id) VALUES ('a')")


@pytest.mark.asyncio
@patch("perseus_client.services.spanner_graph_service.SPANNER_AVAILABLE", False)
async def test_missing_spanner_library():
    service = SpannerGraphService(asyncio.get_event_loop())
    with pytest.raises(ImportError, match="The 'google-cloud-spanner' library is not installed."):
        await service.execute_spgl_string_async("INSERT INTO T (id) VALUES ('a')")


@pytest.mark.asyncio
async def test_file_not_found():
    service = SpannerGraphService(asyncio.get_event_loop())
    with pytest.raises(FileNotFoundError):
        await service.save_output_to_spanner_graph_async("non_existent_file.spgl")


@patch(
    "perseus_client.services.spanner_graph_service.SpannerGraphService.execute_spgl_string_async",
    new_callable=MagicMock,
)
def test_empty_knowledge_graph(mock_execute):
    service = SpannerGraphService(MagicMock())
    service.save_to_spanner_graph(KnowledgeGraph(entities=[], relations=[]))
    mock_execute.assert_not_called()


@pytest.mark.asyncio
@patch(
    "perseus_client.services.spanner_graph_service.SpannerGraphService.execute_spgl_string_async",
    side_effect=PerseusException("boom"),
)
async def test_save_to_spanner_graph_async_raises(mock_execute):
    """
    Like Neo4j and FalkorDB, the async save raises connection errors.
    """
    service = SpannerGraphService(asyncio.get_event_loop())
    with pytest.raises(PerseusException):
        await service.save_to_spanner_graph_async(_sample_kg())


def test_save_to_spanner_graph_logs_error(caplog):
    """
    Like Neo4j and FalkorDB, the sync save only logs errors.
    """
    loop = MagicMock()
    loop.run_until_complete.side_effect = PerseusException("boom")
    service = SpannerGraphService(loop)
    with patch.object(service, "save_to_spanner_graph_async", new_callable=MagicMock):
        service.save_to_spanner_graph(_sample_kg())
    assert "Failed to save to Spanner Graph: boom" in caplog.text


def test_close():
    service = SpannerGraphService(MagicMock())
    client = MagicMock()
    service._client = client
    service._database = MagicMock()
    service.close()
    client.close.assert_called_once()
    assert service._database is None
