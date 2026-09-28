import pytest
from datetime import date, datetime
from unittest.mock import mock_open, patch
from perseus_client.services.cql_service import CQLService
from perseus_client.models import KnowledgeGraph, Entity, Relation, LiteralValue

@pytest.fixture
def cql_service():
    return CQLService()

@pytest.fixture
def sample_kg():
    return KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person1",
                types=["foaf:Person"],
                properties={
                    "rdfs:label": LiteralValue(value="John Doe"),
                    "ex:age": LiteralValue(value=30),
                    "ex:isStudent": LiteralValue(value=False),
                    "ex:birthDate": LiteralValue(value=date(1994, 2, 1)),
                    "ex:hobbies": LiteralValue(value=["reading", "coding"]),
                },
            )
        ],
        relations=[
            Relation(
                source_uri="ex:Person1",
                target_uri="ex:Person1",
                predicate="foaf:knows",
                properties={"ex:since": LiteralValue(value=datetime(2022, 1, 1, 12, 0, 0))},
            )
        ],
        namespaces={
            "ex": "http://example.org/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        },
    )

def test_add_metadata_to_cql(cql_service):
    cql = "MERGE (n:Person {uri: 'ex:1'});MATCH (a {uri: 'ex:1'}) MATCH (b {uri: 'ex:2'}) MERGE (a)-[r:KNOWS]->(b);"
    metadata = {"source": "test", "timestamp": "2024-01-01"}
    modified_cql = cql_service.add_metadata_to_cql(cql, metadata)
    assert "source: 'test'" in modified_cql
    assert "timestamp: '2024-01-01'" in modified_cql
    assert "{source: 'test', timestamp: '2024-01-01', uri: 'ex:1'}" in modified_cql
    assert "[r:KNOWS {source: 'test', timestamp: '2024-01-01'}]" in modified_cql

def test_to_cql_with_strip_prefixes(cql_service, sample_kg):
    cql = cql_service.to_cql(sample_kg, strip_prefixes=True)
    assert "MERGE (n:`foaf:Person` {uri: 'ex:Person1'})" in cql
    assert "SET n += {`rdfs:label`: 'John Doe', `ex:age`: 30, `ex:isStudent`: false, `ex:birthDate`: '1994-02-01', `ex:hobbies`: ['reading', 'coding']}" in cql
    assert "MATCH (source_node {uri: 'ex:Person1'})" in cql
    assert "MATCH (target_node {uri: 'ex:Person1'})" in cql
    assert "MERGE (source_node)-[r:`foaf:knows`]->(target_node)" in cql
    assert "SET r += {`ex:since`: '2022-01-01T12:00:00'}" in cql

def test_to_cql_without_strip_prefixes(cql_service, sample_kg):
    cql = cql_service.to_cql(sample_kg, strip_prefixes=False)
    assert "MERGE (n:`foaf:Person` {uri: 'ex:Person1'})" in cql
    assert "SET n += {`rdfs:label`: 'John Doe', `ex:age`: 30, `ex:isStudent`: false, `ex:birthDate`: '1994-02-01', `ex:hobbies`: ['reading', 'coding']}" in cql
    assert "MERGE (source_node)-[r:`foaf:knows`]->(target_node)" in cql

def test_save_cql(cql_service, sample_kg):
    file_path = "test.cql"
    cql_content = cql_service.to_cql(sample_kg)
    with patch("builtins.open", mock_open()) as mocked_file:
        cql_service.save_cql(sample_kg, file_path)
        mocked_file.assert_called_once_with(file_path, "w", encoding="utf-8")
        mocked_file().write.assert_called_once_with(cql_content)


def test_uri_escaping_single_quote(cql_service):
    """Test that URIs with single quotes are properly escaped."""
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/john's-diner",
                types=["http://example.org/Restaurant"],
                properties={"http://example.org/name": LiteralValue(value="John's Diner")},
            )
        ]
    )
    cql = cql_service.to_cql(kg)

    # URI should be escaped
    assert "uri: 'http://example.org/john\\'s-diner'" in cql
    # Property value should also be escaped
    assert "name`: 'John\\'s Diner'" in cql
    # Make sure the quote is escaped with backslash
    assert "john's-diner" not in cql  # Unescaped version should not appear


def test_uri_escaping_backslash(cql_service):
    """Test that URIs with backslashes are properly escaped."""
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/path\\with\\backslash",
                types=["http://example.org/Resource"],
                properties={},
            )
        ]
    )
    cql = cql_service.to_cql(kg)

    # Backslashes should be escaped
    assert "uri: 'http://example.org/path\\\\with\\\\backslash'" in cql


def test_uri_escaping_in_relations(cql_service):
    """Test that URIs in relations are properly escaped."""
    kg = KnowledgeGraph(
        entities=[
            Entity(uri="http://example.org/alice's-account", types=["http://example.org/Account"]),
            Entity(uri="http://example.org/bob's-account", types=["http://example.org/Account"]),
        ],
        relations=[
            Relation(
                source_uri="http://example.org/alice's-account",
                target_uri="http://example.org/bob's-account",
                predicate="http://example.org/follows",
            )
        ]
    )
    cql = cql_service.to_cql(kg)

    # Both source and target URIs should be escaped in MATCH clauses
    assert "uri: 'http://example.org/alice\\'s-account'" in cql
    assert "uri: 'http://example.org/bob\\'s-account'" in cql
    # Unescaped versions should not appear
    assert "alice's-account'}" not in cql
    assert "bob's-account'}" not in cql


def test_property_value_escaping(cql_service):
    """Test that property values with special characters are properly escaped."""
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/entity1",
                types=["http://example.org/Resource"],
                properties={
                    "http://example.org/quote": LiteralValue(value="It's a test"),
                    "http://example.org/backslash": LiteralValue(value="C:\\path\\to\\file"),
                    "http://example.org/both": LiteralValue(value="Path: C:\\user's\\folder"),
                },
            )
        ]
    )
    cql = cql_service.to_cql(kg)

    # Property values should be escaped
    assert "quote`: 'It\\'s a test'" in cql
    assert "backslash`: 'C:\\\\path\\\\to\\\\file'" in cql
    assert "both`: 'Path: C:\\\\user\\'s\\\\folder'" in cql


def test_ttl_to_cql_with_special_characters_integration(cql_service):
    """Integration test: TTL parsing through CQL generation with special characters.

    This reproduces the example from issue #12 where URIs with single quotes
    need to be properly escaped in the generated Cypher output.
    """
    from perseus_client.services.ttl_service import TTLService

    ttl_content = """
@prefix ex: <http://example.org/> .
<http://example.org/john's-diner> a ex:Restaurant ;
    ex:name "John's Diner" .
"""

    ttl_service = TTLService()
    kg = ttl_service.parse_ttl_to_knowledge_graph(ttl_content)
    cypher = cql_service.to_cql(kg)

    # URI should be escaped in MERGE statement
    assert "uri: 'http://example.org/john\\'s-diner'" in cypher
    # Property value should be escaped
    assert "name`: 'John\\'s Diner'" in cypher
    # Unescaped versions should NOT appear
    assert "john's-diner'}" not in cypher
    assert "John's Diner'}" not in cypher
