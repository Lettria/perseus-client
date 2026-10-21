"""
Integration tests on the Spanner emulator.

Run with a Spanner emulator and SPANNER_EMULATOR_HOST set, e.g.:
    SPANNER_EMULATOR_HOST=localhost:9010 pytest -m spanner
Each test uses its own database, dropped at the end.
"""

import asyncio
import os
import uuid

import pytest

from perseus_client.config import settings
from perseus_client.models import KnowledgeGraph, Entity, Relation, LiteralValue
from perseus_client.services.spanner_graph_service import SpannerGraphService

spanner = pytest.importorskip("google.cloud.spanner")

pytestmark = pytest.mark.spanner

# A table that isn't part of the Perseus schema, to check it is left untouched
OTHER_TABLE_DDL = "CREATE TABLE Other (id STRING(MAX) NOT NULL) PRIMARY KEY (id)"


@pytest.fixture
def spanner_service(monkeypatch):
    """A SpannerGraphService on a new emulator database, dropped after the test."""
    monkeypatch.setattr(settings, "spanner_emulator_host", os.environ["SPANNER_EMULATOR_HOST"])
    monkeypatch.setattr(settings, "spanner_project_id", settings.spanner_project_id or "perseus-test")
    monkeypatch.setattr(settings, "spanner_instance_id", settings.spanner_instance_id or "perseus-test")
    monkeypatch.setattr(settings, "spanner_database_id", f"test_{uuid.uuid4().hex[:12]}")
    monkeypatch.setattr(settings, "spanner_auto_create_schema", True)

    service = SpannerGraphService(asyncio.new_event_loop())
    yield service
    service.close()
    database = _database(settings.spanner_database_id)
    if database.exists():
        database.drop()


def _database(database_id: str, ddl_statements=()):
    client = spanner.Client(project=settings.spanner_project_id)
    instance = client.instance(settings.spanner_instance_id)
    return instance.database(database_id, ddl_statements=list(ddl_statements))


def _create_database_without_graph(service, monkeypatch):
    """Creates the database with an unrelated table only, like an admin would."""
    monkeypatch.setattr(settings, "spanner_auto_create_schema", False)
    service._connect()  # Creates the emulator instance, not the database
    service.close()
    database = _database(settings.spanner_database_id, [OTHER_TABLE_DDL])
    database.create().result(service.timeout)


def _sample_kg() -> KnowledgeGraph:
    return KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/data/n-1",
                types=["http://example.org/Person"],
                properties={"http://example.org/name": LiteralValue(value="Ada")},
            ),
            Entity(uri="http://example.org/data/n-2", types=["http://example.org/City"]),
        ],
        relations=[
            Relation(
                source_uri="http://example.org/data/n-1",
                target_uri="http://example.org/data/n-2",
                predicate="http://example.org/livesIn",
            )
        ],
        namespaces={"ex": "http://example.org/"},
    )


def _tables(service) -> set:
    with service._database.snapshot() as snapshot:
        rows = snapshot.execute_sql(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = ''"
        )
        return {row[0] for row in rows}


def _count(service, table: str) -> int:
    with service._database.snapshot() as snapshot:
        return list(snapshot.execute_sql(f"SELECT COUNT(*) FROM {table}"))[0][0]


def test_existing_database_without_graph_gets_schema(spanner_service, monkeypatch):
    _create_database_without_graph(spanner_service, monkeypatch)
    monkeypatch.setattr(settings, "spanner_auto_create_schema", True)

    spanner_service.save_to_spanner_graph(_sample_kg())

    assert _tables(spanner_service) == {"Other", "PerseusNode", "PerseusEdge"}
    assert spanner_service._graph_exists(spanner_service._database)
    assert _count(spanner_service, "PerseusNode") == 2
    assert _count(spanner_service, "PerseusEdge") == 1


def test_auto_create_schema_off(spanner_service, monkeypatch, caplog):
    """
    Nothing is created: the save only logs an error, like Neo4j/FalkorDB.
    The schema is then created with ensure_schema().
    """
    _create_database_without_graph(spanner_service, monkeypatch)

    spanner_service.save_to_spanner_graph(_sample_kg())

    assert _tables(spanner_service) == {"Other"}
    assert not spanner_service._graph_exists(spanner_service._database)
    assert "PerseusNode" in caplog.text

    spanner_service.ensure_schema()
    spanner_service.save_to_spanner_graph(_sample_kg())

    assert _tables(spanner_service) == {"Other", "PerseusNode", "PerseusEdge"}
    assert _count(spanner_service, "PerseusNode") == 2
    assert _count(spanner_service, "PerseusEdge") == 1
