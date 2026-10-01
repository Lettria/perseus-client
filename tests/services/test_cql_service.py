import pytest
from datetime import date, datetime
from unittest.mock import mock_open, patch
from perseus_client.services.cql_service import CQLService, split_cypher_statements
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
    # Now uses uuid (extracted from URI) instead of uri
    assert "MERGE (n:`foaf:Person` {uuid: 'Person1'})" in cql
    assert "SET n += {`rdfs:label`: 'John Doe', `ex:age`: 30, `ex:isStudent`: false, `ex:birthDate`: '1994-02-01', `ex:hobbies`: ['reading', 'coding']}" in cql
    assert "MATCH (source_node {uuid: 'Person1'})" in cql
    assert "MATCH (target_node {uuid: 'Person1'})" in cql
    assert "MERGE (source_node)-[r:`foaf:knows`]->(target_node)" in cql
    assert "SET r += {`ex:since`: '2022-01-01T12:00:00'}" in cql

def test_to_cql_without_strip_prefixes(cql_service, sample_kg):
    cql = cql_service.to_cql(sample_kg, strip_prefixes=False)
    # Now uses uuid (extracted from URI) instead of uri
    assert "MERGE (n:`foaf:Person` {uuid: 'Person1'})" in cql
    assert "SET n += {`rdfs:label`: 'John Doe', `ex:age`: 30, `ex:isStudent`: false, `ex:birthDate`: '1994-02-01', `ex:hobbies`: ['reading', 'coding']}" in cql
    assert "MERGE (source_node)-[r:`foaf:knows`]->(target_node)" in cql

def test_save_cql(cql_service, sample_kg):
    file_path = "test.cql"
    cql_content = cql_service.to_cql(sample_kg)
    with patch("builtins.open", mock_open()) as mocked_file:
        cql_service.save_cql(sample_kg, file_path)
        mocked_file.assert_called_once_with(file_path, "w", encoding="utf-8")
        mocked_file().write.assert_called_once_with(cql_content)


def test_uuid_extraction_from_full_uri(cql_service):
    """Test that UUIDs are correctly extracted from full URIs and used as merge keys.

    Reproduces issue #15 where SDK should use uuid (like the server) instead of uri.
    """
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/data/c5d7d866-4af5-5361-bf45-69ac9f845cd1",
                types=["http://example.org/Company"],
                properties={},
            )
        ]
    )
    cql = cql_service.to_cql(kg)

    # Should use uuid as merge key, matching server behavior
    # Label gets stripped to just 'Company' by default
    assert "MERGE (n:`Company` {uuid: 'c5d7d866-4af5-5361-bf45-69ac9f845cd1'})" in cql
    # Should NOT use uri as merge key
    assert "{uri:" not in cql


def test_uuid_extraction_from_prefixed_uri(cql_service):
    """Test that UUIDs are correctly extracted from prefixed URIs."""
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:alice-123",
                types=["ex:Person"],
                properties={},
            )
        ]
    )
    cql = cql_service.to_cql(kg)
    
    # Should extract 'alice-123' as the uuid
    assert "MERGE (n:`ex:Person` {uuid: 'alice-123'})" in cql


def test_uuid_in_relations(cql_service):
    """Test that relations use uuid to match nodes."""
    kg = KnowledgeGraph(
        entities=[
            Entity(uri="http://example.org/data/alice-uuid", types=["http://example.org/Person"]),
            Entity(uri="http://example.org/data/bob-uuid", types=["http://example.org/Person"]),
        ],
        relations=[
            Relation(
                source_uri="http://example.org/data/alice-uuid",
                target_uri="http://example.org/data/bob-uuid",
                predicate="http://example.org/knows",
            )
        ]
    )
    cql = cql_service.to_cql(kg)
    
    # Relations should match nodes by uuid
    assert "MATCH (source_node {uuid: 'alice-uuid'})" in cql
    assert "MATCH (target_node {uuid: 'bob-uuid'})" in cql


def test_uuid_escaping_single_quote(cql_service):
    """Test that UUIDs with single quotes are properly escaped."""
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/john's-diner",
                types=["http://example.org/Restaurant"],
                properties={},
            )
        ]
    )
    cql = cql_service.to_cql(kg)
    
    # UUID should be escaped
    assert "uuid: 'john\\'s-diner'" in cql
    # Unescaped version should not appear
    assert "john's-diner'}" not in cql


def test_uuid_escaping_in_relations(cql_service):
    """Test that UUIDs with special characters in relations are properly escaped."""
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
    
    # Both source and target UUIDs should be escaped in MATCH clauses
    assert "uuid: 'alice\\'s-account'" in cql
    assert "uuid: 'bob\\'s-account'" in cql
    # Unescaped versions should not appear
    assert "alice's-account'}" not in cql
    assert "bob's-account'}" not in cql
def test_split_cypher_statements_simple():
    """Test splitting simple Cypher statements."""
    cql = "MERGE (n:Person {uri: 'ex:1'});MERGE (n:Person {uri: 'ex:2'});"
    statements = split_cypher_statements(cql)
    assert len(statements) == 2
    assert statements[0] == "MERGE (n:Person {uri: 'ex:1'})"
    assert statements[1] == "MERGE (n:Person {uri: 'ex:2'})"


def test_split_cypher_statements_with_semicolon_in_value():
    """Test that semicolons inside quoted strings are not treated as terminators."""
    cql = (
        "MERGE (n:Person {uri: 'https://example.org/alice'})\n"
        "SET n += {name: 'Smith; John'};\n"
        "MERGE (n:Person {uri: 'https://example.org/bob'});\n"
        "MERGE (n:Person {uri: 'https://example.org/carol'});"
    )
    statements = split_cypher_statements(cql)

    assert len(statements) == 3
    # First statement should contain the semicolon in the value
    assert "name: 'Smith; John'" in statements[0]
    assert "alice" in statements[0]
    assert "bob" in statements[1]
    assert "carol" in statements[2]


def test_split_cypher_statements_with_escaped_quotes():
    """Test that escaped quotes inside strings are handled correctly."""
    cql = "MERGE (n:Person {name: 'O\\'Brien; PhD'});MERGE (m:Person {name: 'Smith'});"
    statements = split_cypher_statements(cql)

    assert len(statements) == 2
    assert "O\\'Brien; PhD" in statements[0]
    assert "Smith" in statements[1]


def test_split_cypher_statements_multiline():
    """Test splitting multiline Cypher statements."""
    cql = """MERGE (n:Person {uri: 'ex:1'})
SET n += {name: 'Alice; Test'};
MATCH (a {uri: 'ex:1'}) MATCH (b {uri: 'ex:2'})
MERGE (a)-[r:KNOWS]->(b);"""

    statements = split_cypher_statements(cql)

    assert len(statements) == 2
    assert "Alice; Test" in statements[0]
    assert "MATCH (a" in statements[1]


def test_add_metadata_to_cql_with_semicolon_in_value(cql_service):
    """Test that add_metadata_to_cql handles semicolons in property values correctly."""
    cql = (
        "MERGE (n:Person {uri: 'https://example.org/alice'})\n"
        "SET n += {name: 'Smith; John'};"
    )
    metadata = {"source": "test.txt"}
    modified_cql = cql_service.add_metadata_to_cql(cql, metadata)

    # Should have exactly one statement
    statements = split_cypher_statements(modified_cql)
    assert len(statements) == 1
    # The semicolon in the value should still be there
    assert "Smith; John" in modified_cql
    # Metadata should be added
    assert "source: 'test.txt'" in modified_cql


def test_knowledge_graph_to_cypher_statements_with_semicolon():
    """Test KnowledgeGraph.to_cypher_statements() with semicolons in values."""
    cql_content = (
        "MERGE (n:Person {uri: 'https://example.org/alice'})\n"
        "SET n += {name: 'Smith; John'};\n"
        "MERGE (n:Person {uri: 'https://example.org/bob'});"
    )

    kg = KnowledgeGraph(cql_content=cql_content)
    statements = kg.to_cypher_statements()

    assert len(statements) == 2
    assert "Smith; John" in statements[0]
    assert "bob" in statements[1]
