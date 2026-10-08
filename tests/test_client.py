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

    with patch(
        "perseus_client.client.PerseusClient.build", new_callable=PropertyMock
    ) as mock_build_property:
        mock_build_property.return_value = mock_build_service

        await client.build_graph_async(
            file_paths=file_path, ontology_path=ontology_path, metadata=metadata
        )

        mock_build_service.build_graph_async.assert_called_once_with(
            file_paths=file_path,
            ontology_path=ontology_path,
            refresh_graph=False,
            metadata=metadata,
            base_uri=None,
        )


@pytest.mark.asyncio
async def test_interlink_async(client: PerseusClient):
    """
    Test the interlink_async method.
    """
    kbs = [KnowledgeGraph()]

    mock_build_service = MagicMock(spec=BuildService)
    mock_build_service.interlink_async = AsyncMock(return_value=KnowledgeGraph())

    with patch(
        "perseus_client.client.PerseusClient.build", new_callable=PropertyMock
    ) as mock_build_property:
        mock_build_property.return_value = mock_build_service

        await client.interlink_async(kbs=kbs)

        mock_build_service.interlink_async.assert_called_once_with(
            kbs=kbs,
            interlinking_key_uris=["http://www.w3.org/2000/01/rdf-schema#label"],
            immutable_properties=None,
            merge_properties_on_conflict=False,
        )


# --- Session lifecycle (issues #28 and #29) ---------------------------------
# These tests use a real PerseusClient (not the `client` fixture, which patches
# `_ensure_active`) so the actual activation path runs.

import asyncio
import aiohttp
from perseus_client.exceptions import ConfigurationException


@pytest.fixture
def lifecycle_env():
    """
    Patches settings, aiohttp.ClientSession/TCPConnector and BuildService async
    methods. Yields the list of created mock sessions.
    """
    sessions = []
    real_session_cls = aiohttp.ClientSession
    real_connector_cls = aiohttp.TCPConnector

    def make_session(*args, **kwargs):
        session = MagicMock(spec=real_session_cls)
        session.closed = False

        async def close():
            session.closed = True

        session.close = AsyncMock(side_effect=close)
        sessions.append(session)
        return session

    def make_connector(*args, **kwargs):
        connector = MagicMock(spec=real_connector_cls)
        connector.close = AsyncMock()
        return connector

    with patch("perseus_client.client.settings") as mock_settings, patch(
        "perseus_client.client.aiohttp.ClientSession", side_effect=make_session
    ), patch(
        "perseus_client.client.aiohttp.TCPConnector", side_effect=make_connector
    ), patch.object(
        BuildService, "build_graph_async", new=AsyncMock(return_value=[KnowledgeGraph()])
    ), patch.object(
        BuildService, "interlink_async", new=AsyncMock(return_value=KnowledgeGraph())
    ):
        mock_settings.perseus_api_host = "https://perseus.test"
        mock_settings.perseus_api_key = "mock_token"
        yield sessions


@pytest.mark.asyncio
async def test_build_graph_async_on_fresh_client(lifecycle_env):
    client = PerseusClient()
    try:
        result = await client.build_graph_async(file_paths=["test.txt"])
        assert len(result) == 1
        assert client._is_active()
        assert client._loop is asyncio.get_running_loop()
    finally:
        await client.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_interlink_async_on_fresh_client(lifecycle_env):
    client = PerseusClient()
    try:
        result = await client.interlink_async(kbs=[KnowledgeGraph()])
        assert isinstance(result, KnowledgeGraph)
        assert client._loop is asyncio.get_running_loop()
    finally:
        await client.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_property_in_running_loop_on_inactive_client_raises(lifecycle_env):
    client = PerseusClient()
    with pytest.raises(ConfigurationException):
        client.build
    assert lifecycle_env == []


def test_client_awaited_from_different_loop_raises(lifecycle_env):
    client = PerseusClient()
    with client:
        other_loop = asyncio.new_event_loop()
        try:
            with pytest.raises(ConfigurationException):
                other_loop.run_until_complete(
                    client.build_graph_async(file_paths=["test.txt"])
                )
        finally:
            other_loop.close()


def test_build_graph_sync_closes_session_it_opened(lifecycle_env):
    client = PerseusClient()
    result = client.build_graph(file_paths=["test.txt"])
    assert len(result) == 1
    assert not client._is_active()
    assert len(lifecycle_env) == 1
    lifecycle_env[0].close.assert_awaited_once()


def test_interlink_sync_closes_session_it_opened(lifecycle_env):
    client = PerseusClient()
    client.interlink(kbs=[KnowledgeGraph()])
    assert not client._is_active()
    lifecycle_env[0].close.assert_awaited_once()


def test_build_graph_sync_inside_with_keeps_session_open(lifecycle_env):
    with PerseusClient() as client:
        client.build_graph(file_paths=["test.txt"])
        assert client._is_active()
        lifecycle_env[0].close.assert_not_awaited()
    lifecycle_env[0].close.assert_awaited_once()


def test_build_graph_sync_closes_session_on_error(lifecycle_env):
    BuildService.build_graph_async.side_effect = RuntimeError("boom")
    client = PerseusClient()
    with pytest.raises(RuntimeError):
        client.build_graph(file_paths=["test.txt"])
    assert not client._is_active()
    lifecycle_env[0].close.assert_awaited_once()


def test_build_graph_sync_twice_opens_and_closes_each_time(lifecycle_env):
    client = PerseusClient()
    client.build_graph(file_paths=["a.txt"])
    client.build_graph(file_paths=["b.txt"])
    assert len(lifecycle_env) == 2
    for session in lifecycle_env:
        session.close.assert_awaited_once()
    assert not client._is_active()


def test_build_graph_sync_creates_and_closes_one_loop(lifecycle_env):
    real_new_event_loop = asyncio.new_event_loop
    created = []

    def tracking_new_event_loop():
        loop = real_new_event_loop()
        created.append(loop)
        return loop

    with patch(
        "perseus_client.client.asyncio.new_event_loop",
        side_effect=tracking_new_event_loop,
    ):
        PerseusClient().build_graph(file_paths=["test.txt"])

    assert len(created) == 1
    assert created[0].is_closed()


def test_module_level_build_graph_reuses_single_session(lifecycle_env):
    import perseus_client as sdk

    with patch.object(sdk, "_client", None), patch.object(
        sdk, "_atexit_registered", True
    ):
        sdk.build_graph(file_paths=["a.txt"])
        sdk.build_graph(file_paths=["b.txt"])
        assert len(lifecycle_env) == 1
        lifecycle_env[0].close.assert_not_awaited()
        sdk.close()
        lifecycle_env[0].close.assert_awaited_once()
