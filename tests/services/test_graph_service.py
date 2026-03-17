import pytest
from perseus_client.services.graph_service import GraphService
from perseus_client.models import KnowledgeGraph, Entity, Relation, LiteralValue

@pytest.fixture
def graph_service():
    return GraphService()

@pytest.fixture
def sample_kbs():
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person1",
                types=["foaf:Person"],
                properties={"rdfs:label": LiteralValue(value="John Doe"), "ex:age": LiteralValue(value=30)},
            ),
            Entity(
                uri="ex:City1",
                types=["ex:City"],
                properties={"rdfs:label": LiteralValue(value="New York")},
            ),
        ],
        relations=[
            Relation(
                source_uri="ex:Person1",
                target_uri="ex:City1",
                predicate="ex:livesIn",
            )
        ],
        namespaces={"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person2",
                types=["foaf:Person", "ex:Student"],
                properties={"rdfs:label": LiteralValue(value="John Doe"), "ex:major": LiteralValue(value="Computer Science")},
            )
        ],
        relations=[
             Relation(
                source_uri="ex:Person2",
                target_uri="ex:City1",
                predicate="ex:bornIn",
            )
        ],
        namespaces={"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
    )
    return [kg1, kg2]

def test_interlink_simple_merge(graph_service, sample_kbs):
    merged_kg = graph_service.interlink(sample_kbs, interlinking_key_uri="uri")
    assert len(merged_kg.entities) == 3
    assert len(merged_kg.relations) == 2
    
    person_entity = next(e for e in merged_kg.entities if e.uri == "ex:Person1")
    assert "ex:age" in person_entity.properties
    
    person_2_entity = next(e for e in merged_kg.entities if e.uri == "ex:Person2")
    assert "ex:major" in person_2_entity.properties
    assert "ex:Student" in person_2_entity.types
    
    born_in_relation = next(r for r in merged_kg.relations if r.predicate == "ex:bornIn")
    assert born_in_relation.source_uri == "ex:Person2"

def test_interlink_empty_list(graph_service):
    merged_kg = graph_service.interlink([])
    assert len(merged_kg.entities) == 0
    assert len(merged_kg.relations) == 0

def test_interlink_merge_properties_on_conflict(graph_service):
    kg1 = KnowledgeGraph(entities=[Entity(uri="ex:1", properties={"ex:prop": LiteralValue(value="A")})])
    kg2 = KnowledgeGraph(entities=[Entity(uri="ex:2", properties={"ex:prop": LiteralValue(value="B"), "rdfs:label": LiteralValue(value="Same")})])
    kg3 = KnowledgeGraph(entities=[Entity(uri="ex:3", properties={"ex:prop": LiteralValue(value="C"), "rdfs:label": LiteralValue(value="Same")})])
    
    merged_kg = graph_service.interlink([kg1, kg2, kg3], interlinking_key_uri="rdfs:label", merge_properties_on_conflict=True)
    assert len(merged_kg.entities) == 2
    merged_entity = next(e for e in merged_kg.entities if "rdfs:label" in e.properties)
    assert sorted(merged_entity.properties["ex:prop"].value) == ["B", "C"]

def test_interlink_immutable_property_conflict(graph_service):
    kg1 = KnowledgeGraph(entities=[Entity(uri="ex:1", properties={"rdfs:label": LiteralValue(value="Same"), "ex:name": LiteralValue(value="John")})])
    kg2 = KnowledgeGraph(entities=[Entity(uri="ex:2", properties={"rdfs:label": LiteralValue(value="Same"), "ex:name": LiteralValue(value="Jon")})])
    
    merged_kg = graph_service.interlink([kg1, kg2], immutable_properties=["ex:name"])
    assert len(merged_kg.entities) == 2
