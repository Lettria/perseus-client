from perseus_client.models import KnowledgeGraph, Entity, Relation, LiteralValue
from perseus_client.services.rdflib_service import RDFLibService
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF, XSD

def test_knowledge_graph_to_rdflib_empty_graph():
    kg = KnowledgeGraph()
    service = RDFLibService()
    g = service.to_rdflib(kg)
    assert isinstance(g, Graph)
    assert len(g) == 0

def test_knowledge_graph_to_rdflib_with_entities():
    kg = KnowledgeGraph(
        entities=[
            Entity(uri="http://example.com/person/1", types=["http://xmlns.com/foaf/0.1/Person"], properties={"http://xmlns.com/foaf/0.1/name": LiteralValue(value="Alice", datatype=str(XSD.string))}),
            Entity(uri="http://example.com/organization/abc", types=["http://xmlns.com/foaf/0.1/Organization"], properties={"http://www.w3.org/2000/01/rdf-schema#label": LiteralValue(value="ABC Corp", datatype=str(XSD.string))})
        ]
    )
    service = RDFLibService()
    g = service.to_rdflib(kg)
    assert isinstance(g, Graph)
    assert len(g) == 4  # 2 entities, each with a type and a property

    # Check for Alice
    alice_uri = URIRef("http://example.com/person/1")
    assert (alice_uri, RDF.type, URIRef("http://xmlns.com/foaf/0.1/Person")) in g
    assert (alice_uri, URIRef("http://xmlns.com/foaf/0.1/name"), Literal("Alice", datatype=XSD.string)) in g

    # Check for ABC Corp
    abc_uri = URIRef("http://example.com/organization/abc")
    assert (abc_uri, RDF.type, URIRef("http://xmlns.com/foaf/0.1/Organization")) in g
    assert (abc_uri, URIRef("http://www.w3.org/2000/01/rdf-schema#label"), Literal("ABC Corp", datatype=XSD.string)) in g

def test_knowledge_graph_to_rdflib_with_relations():
    kg = KnowledgeGraph(
        entities=[
            Entity(uri="http://example.com/person/1", types=["http://xmlns.com/foaf/0.1/Person"]),
            Entity(uri="http://example.com/person/2", types=["http://xmlns.com/foaf/0.1/Person"])
        ],
        relations=[
            Relation(source_uri="http://example.com/person/1", target_uri="http://example.com/person/2", predicate="http://example.com/knows")
        ]
    )
    service = RDFLibService()
    g = service.to_rdflib(kg)
    assert isinstance(g, Graph)
    assert len(g) == 3  # 2 entity types + 1 relation

    subject = URIRef("http://example.com/person/1")
    predicate = URIRef("http://example.com/knows")
    obj = URIRef("http://example.com/person/2")
    assert (subject, predicate, obj) in g

def test_knowledge_graph_to_rdflib_with_namespaces():
    kg = KnowledgeGraph(
        namespaces={"foaf": "http://xmlns.com/foaf/0.1/", "ex": "http://example.com/"},
        entities=[
            Entity(uri="http://example.com/person/1", types=["http://xmlns.com/foaf/0.1/Person"])
        ]
    )
    service = RDFLibService()
    g = service.to_rdflib(kg)
    assert isinstance(g, Graph)
    assert ("foaf", URIRef("http://xmlns.com/foaf/0.1/")) in g.namespace_manager.namespaces()
    assert ("ex", URIRef("http://example.com/")) in g.namespace_manager.namespaces()
    assert len(g) == 1

def test_knowledge_graph_to_rdflib_with_literal_datatypes():
    kg = KnowledgeGraph(
        entities=[
            Entity(uri="http://example.com/data/1", properties={
                "http://example.com/hasValue": LiteralValue(value="123", datatype="http://www.w3.org/2001/XMLSchema#integer")
            })
        ]
    )
    service = RDFLibService()
    g = service.to_rdflib(kg)
    assert isinstance(g, Graph)
    assert len(g) == 1

    subject = URIRef("http://example.com/data/1")
    predicate = URIRef("http://example.com/hasValue")
    obj = Literal("123", datatype=XSD.integer)
    assert (subject, predicate, obj) in g

def test_knowledge_graph_to_rdflib_relation_with_properties():
    kg = KnowledgeGraph(
        entities=[
            Entity(uri="http://example.com/person/1", types=["http://xmlns.com/foaf/0.1/Person"]),
            Entity(uri="http://example.com/person/2", types=["http://xmlns.com/foaf/0.1/Person"])
        ],
        relations=[
            Relation(
                source_uri="http://example.com/person/1",
                target_uri="http://example.com/person/2",
                predicate="http://example.com/knows",
                properties={"http://example.com/since": LiteralValue(value="2020-01-01", datatype="http://www.w3.org/2001/XMLSchema#date")}
            )
        ]
    )
    service = RDFLibService()
    g = service.to_rdflib(kg)
    assert isinstance(g, Graph)
    assert len(g) == 4

    # Check relation triple
    subject = URIRef("http://example.com/person/1")
    predicate = URIRef("http://example.com/knows")
    obj = URIRef("http://example.com/person/2")
    assert (subject, predicate, obj) in g

    # Check relation property
    prop_predicate = URIRef("http://example.com/since")
    prop_value = Literal("2020-01-01", datatype=XSD.date)
    assert (subject, prop_predicate, prop_value) in g
