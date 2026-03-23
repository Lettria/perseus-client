import pytest
from perseus_client.models import KnowledgeGraph

def test_to_cypher_statements_with_content():
    """
    Test that to_cypher_statements correctly splits a CQL string into statements.
    """
    cql_content = """CREATE (n:Person {uri: 'http://example.com/person/1', name: 'Alice'});
CREATE (n:Company {uri: 'http://example.com/company/1', name: 'Acme Inc.'});
MATCH (a:Person), (b:Company) WHERE a.name = 'Alice' AND b.name = 'Acme Inc.' CREATE (a)-[:WORKS_FOR]->(b);"""

    kg = KnowledgeGraph(cql_content=cql_content)
    
    statements = kg.to_cypher_statements()
    
    assert len(statements) == 3
    assert statements[0] == "CREATE (n:Person {uri: 'http://example.com/person/1', name: 'Alice'})"
    assert statements[1] == "CREATE (n:Company {uri: 'http://example.com/company/1', name: 'Acme Inc.'})"
    assert "MATCH (a:Person), (b:Company)" in statements[2]
    assert "CREATE (a)-[:WORKS_FOR]->(b)" in statements[2]

def test_to_cypher_statements_empty_and_whitespace():
    """
    Test that to_cypher_statements handles empty strings, whitespace, and missing semicolons.
    """
    cql_content = "CREATE (n:Person); ; CREATE (n:Company)  "
    kg = KnowledgeGraph(cql_content=cql_content)
    
    statements = kg.to_cypher_statements()
    
    assert len(statements) == 2
    assert statements[0] == "CREATE (n:Person)"
    assert statements[1] == "CREATE (n:Company)"

def test_to_cypher_statements_no_content():
    """
    Test that to_cypher_statements returns an empty list when cql_content is None.
    """
    kg = KnowledgeGraph(cql_content=None)
    statements = kg.to_cypher_statements()
    assert statements == []

    kg_empty = KnowledgeGraph(cql_content="")
    statements_empty = kg_empty.to_cypher_statements()
    assert statements_empty == []
