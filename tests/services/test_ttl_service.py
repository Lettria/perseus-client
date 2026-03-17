import pytest
from unittest.mock import patch, mock_open
from perseus_client.services.ttl_service import TTLService
from perseus_client.models import KnowledgeGraph, Entity, Relation, LiteralValue

@pytest.fixture
def ttl_service():
    return TTLService()

@pytest.fixture
def sample_ttl_content():
    return """
@prefix pmeta: <https://lettria.com/perseus/metadata#> .
@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:Person1 a foaf:Person ;
    foaf:name "John Doe" .
"""

def test_add_metadata_to_ttl(ttl_service, sample_ttl_content):
    metadata = {"source": "test.txt"}
    modified_ttl = ttl_service.add_metadata_to_ttl(sample_ttl_content, metadata)
    assert "pmeta:source" in modified_ttl
    assert '"test.txt"' in modified_ttl

def test_parse_ttl_to_knowledge_graph(ttl_service, sample_ttl_content):
    kg = ttl_service.parse_ttl_to_knowledge_graph(sample_ttl_content)
    assert len(kg.entities) == 1
    assert len(kg.relations) == 0
    entity = kg.entities[0]
    assert entity.uri == "http://example.org/Person1"
    assert "http://xmlns.com/foaf/0.1/Person" in entity.types
    assert "http://xmlns.com/foaf/0.1/name" in entity.properties
    assert entity.properties["http://xmlns.com/foaf/0.1/name"].value == "John Doe"

def test_to_ttl(ttl_service):
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/Person1",
                types=["http://xmlns.com/foaf/0.1/Person"],
                properties={"http://xmlns.com/foaf/0.1/name": LiteralValue(value="John Doe")},
            )
        ],
        namespaces={"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/"},
    )
    ttl = ttl_service.to_ttl(kg)
    assert "@prefix ex:" in ttl
    assert "ex:Person1" in ttl
    assert "a foaf:Person" in ttl
    assert 'foaf:name "John Doe"' in ttl

def test_save_ttl(ttl_service):
    kg = KnowledgeGraph()
    file_path = "test.ttl"
    with patch("builtins.open", mock_open()) as mocked_file:
        ttl_service.save_ttl(kg, file_path)
        mocked_file.assert_called_once_with(file_path, "w", encoding="utf-8")
        mocked_file().write.assert_called_once_with(ttl_service.to_ttl(kg))
