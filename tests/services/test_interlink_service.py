import pytest
from perseus_client.services.interlink_service import InterlinkService
from perseus_client.models import KnowledgeGraph, Entity, Relation, LiteralValue

@pytest.fixture
def interlink_service():
    return InterlinkService()

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
                types=["foaf:Person"],
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

def test_interlink_simple_merge(interlink_service, sample_kbs):
    merged_kg = interlink_service.interlink(sample_kbs, interlinking_key_uris=["rdfs:label"])
    assert len(merged_kg.entities) == 2  # John Doe and New York
    assert len(merged_kg.relations) == 2
    
    # Assert properties of the merged John Doe entity
    john_doe_entity = next(e for e in merged_kg.entities if e.properties.get("rdfs:label") and e.properties["rdfs:label"].value == "John Doe")
    assert john_doe_entity.uri == "ex:Person1"  # Expecting ex:Person1 to be the canonical URI
    assert "ex:age" in john_doe_entity.properties
    assert john_doe_entity.properties["ex:age"].value == 30
    assert "ex:major" in john_doe_entity.properties
    assert john_doe_entity.properties["ex:major"].value == "Computer Science"
    assert "foaf:Person" in john_doe_entity.types
    
    # Assert relation source URI is updated to the merged entity's URI
    lives_in_relation = next(r for r in merged_kg.relations if r.predicate == "ex:livesIn")
    assert lives_in_relation.source_uri == "ex:Person1"
    
    born_in_relation = next(r for r in merged_kg.relations if r.predicate == "ex:bornIn")
    assert born_in_relation.source_uri == "ex:Person1" # Should also be redirected to ex:Person1

def test_interlink_empty_list(interlink_service):
    merged_kg = interlink_service.interlink([])
    assert len(merged_kg.entities) == 0
    assert len(merged_kg.relations) == 0

def test_interlink_merge_properties_on_conflict(interlink_service):
    kg1 = KnowledgeGraph(entities=[Entity(uri="ex:1", properties={"ex:prop": LiteralValue(value="A")})])
    kg2 = KnowledgeGraph(entities=[Entity(uri="ex:2", properties={"ex:prop": LiteralValue(value="B"), "rdfs:label": LiteralValue(value="Same")})])
    kg3 = KnowledgeGraph(entities=[Entity(uri="ex:3", properties={"ex:prop": LiteralValue(value="C"), "rdfs:label": LiteralValue(value="Same")})])
    
    merged_kg = interlink_service.interlink([kg1, kg2, kg3], interlinking_key_uris=["rdfs:label"], merge_properties_on_conflict=True)
    assert len(merged_kg.entities) == 2
    merged_entity = next(e for e in merged_kg.entities if "rdfs:label" in e.properties)
    assert sorted(merged_entity.properties["ex:prop"].value) == ["B", "C"]

def test_interlink_immutable_property_conflict(interlink_service):
    kg1 = KnowledgeGraph(entities=[Entity(uri="ex:1", properties={"rdfs:label": LiteralValue(value="Same"), "ex:name": LiteralValue(value="John")})])
    kg2 = KnowledgeGraph(entities=[Entity(uri="ex:2", properties={"rdfs:label": LiteralValue(value="Same"), "ex:name": LiteralValue(value="Jon")})])
    
    merged_kg = interlink_service.interlink([kg1, kg2], immutable_properties=["ex:name"])
    assert len(merged_kg.entities) == 2

def test_interlink_property_merge_single_to_list(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:1",
                properties={
                    "rdfs:label": LiteralValue(value="Person"),
                    "ex:attribute": LiteralValue(value="valueA"),
                },
            )
        ]
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:2",
                properties={
                    "rdfs:label": LiteralValue(value="Person"),
                    "ex:attribute": LiteralValue(value="valueB"),
                },
            )
        ]
    )

    merged_kg = interlink_service.interlink(
        [kg1, kg2],
        interlinking_key_uris=["rdfs:label"],
        merge_properties_on_conflict=True,
    )
    assert len(merged_kg.entities) == 1
    merged_entity = merged_kg.entities[0]
    assert merged_entity.properties["ex:attribute"].value == ["valueA", "valueB"]

def test_interlink_property_merge_list_deduplication(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:1",
                properties={
                    "rdfs:label": LiteralValue(value="Person"),
                    "ex:tags": LiteralValue(value=["tagA", "tagB"]),
                },
            )
        ]
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:2",
                properties={
                    "rdfs:label": LiteralValue(value="Person"),
                    "ex:tags": LiteralValue(value=["tagB", "tagC"]),
                },
            )
        ]
    )

    merged_kg = interlink_service.interlink(
        [kg1, kg2],
        interlinking_key_uris=["rdfs:label"],
        merge_properties_on_conflict=True,
    )
    assert len(merged_kg.entities) == 1
    merged_entity = merged_kg.entities[0]
    assert sorted(merged_entity.properties["ex:tags"].value) == ["tagA", "tagB", "tagC"]

def test_interlink_property_no_overwrite(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:1",
                properties={
                    "rdfs:label": LiteralValue(value="Person"),
                    "ex:attribute1": LiteralValue(value="valueA"),
                    "ex:attribute2": LiteralValue(value="valueB"),
                },
            )
        ]
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:2",
                properties={
                    "rdfs:label": LiteralValue(value="Person"),
                    "ex:attribute1": LiteralValue(value="newValue"), # This should NOT overwrite
                    "ex:attribute3": LiteralValue(value="valueC"),
                },
            )
        ]
    )

    merged_kg = interlink_service.interlink(
        [kg1, kg2],
        interlinking_key_uris=["rdfs:label"],
        merge_properties_on_conflict=False,
    )
    assert len(merged_kg.entities) == 1
    merged_entity = merged_kg.entities[0]
    assert merged_entity.properties["ex:attribute1"].value == "valueA"  # Should remain original
    assert merged_entity.properties["ex:attribute2"].value == "valueB"
    assert merged_entity.properties["ex:attribute3"].value == "valueC" # New property should be added

def test_interlink_resolve_property_name_various_formats(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:1",
                properties={
                    "rdfs:label": LiteralValue(value="Test Entity"),
                    "http://example.org/fullName": LiteralValue(value="Full Name"),
                    "ex:shortName": LiteralValue(value="Short Name"),
                    "http://example.org/#hashName": LiteralValue(value="Hash Name"),
                    "ex:uriSlashName/": LiteralValue(value="Slash Name"),
                },
            )
        ],
        namespaces={"ex": "http://example.org/", "rdfs": "http://www.w3.org/2000/01/rdf-schema#"},
    )

    # Test with full URI
    merged_kg_1 = interlink_service.interlink(
        [kg1, KnowledgeGraph()],
        immutable_properties=["http://example.org/fullName"],
    )
    assert len(merged_kg_1.entities) == 1  # Should merge, no conflict

    # Test with prefixed name
    merged_kg_2 = interlink_service.interlink(
        [kg1, KnowledgeGraph()],
        immutable_properties=["ex:shortName"],
    )
    assert len(merged_kg_2.entities) == 1  # Should merge, no conflict

    # Test with short name (hash)
    merged_kg_3 = interlink_service.interlink(
        [kg1, KnowledgeGraph()],
        immutable_properties=["hashName"],
    )
    assert len(merged_kg_3.entities) == 1  # Should merge, no conflict

    # Test with short name (slash)
    merged_kg_4 = interlink_service.interlink(
        [kg1, KnowledgeGraph()],
        immutable_properties=["uriSlashName"],
    )
    assert len(merged_kg_4.entities) == 1  # Should merge, no conflict

    # Test with a non-existent property (should not resolve)
    merged_kg_5 = interlink_service.interlink(
        [kg1, KnowledgeGraph()],
        immutable_properties=["nonExistentProperty"],
    )
    assert len(merged_kg_5.entities) == 1  # Should merge, no conflict

def test_interlink_no_merge_different_types_default(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person1",
                types=["foaf:Person"],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person2",
                types=["ex:Agent"],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    merged_kg = interlink_service.interlink([kg1, kg2])
    assert len(merged_kg.entities) == 2

def test_interlink_merge_different_types_explicitly(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person1",
                types=["foaf:Person"],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person2",
                types=["ex:Agent"],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    merged_kg = interlink_service.interlink([kg1, kg2], interlinking_key_uris=["rdfs:label"], merge_different_entity_types=True)
    assert len(merged_kg.entities) == 1
    merged_entity = merged_kg.entities[0]
    assert "foaf:Person" in merged_entity.types
    assert "ex:Agent" in merged_entity.types
def test_interlink_merge_same_types(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person1",
                types=["foaf:Person"],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person2",
                types=["foaf:Person"],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    merged_kg = interlink_service.interlink([kg1, kg2], interlinking_key_uris=["rdfs:label"])
    assert len(merged_kg.entities) == 1

def test_interlink_no_merge_one_entity_has_no_type(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person1",
                types=["foaf:Person"],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person2",
                types=[],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    merged_kg = interlink_service.interlink([kg1, kg2])
    assert len(merged_kg.entities) == 2

def test_interlink_merge_both_entities_have_no_types(interlink_service):
    kg1 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person1",
                types=[],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    kg2 = KnowledgeGraph(
        entities=[
            Entity(
                uri="ex:Person2",
                types=[],
                properties={"rdfs:label": LiteralValue(value="John Doe")},
            )
        ]
    )
    merged_kg = interlink_service.interlink([kg1, kg2], interlinking_key_uris=["rdfs:label"])
    assert len(merged_kg.entities) == 1


def test_interlink_does_not_mutate_input_graphs(interlink_service):
    """Test that interlink() does not mutate the input KnowledgeGraph objects.

    Reproduces the issue from #14 where interlink() modified the input graphs
    when merge_properties_on_conflict=True.
    """
    def person(uri, job):
        entity = Entity(uri=uri, types=["http://example.org#Person"])
        entity.properties = {
            "http://www.w3.org/2000/01/rdf-schema#label": LiteralValue(value="Alice"),
            "http://example.org#jobTitle": LiteralValue(value=job),
        }
        return entity

    kg_a = KnowledgeGraph(entities=[person("http://example.org#alice-1", "Engineer")])
    kg_b = KnowledgeGraph(entities=[person("http://example.org#alice-2", "Manager")])

    # Store original value
    original_job_a = kg_a.entities[0].properties["http://example.org#jobTitle"].value

    # Call interlink with merge_properties_on_conflict=True
    merged_graph = interlink_service.interlink(
        kbs=[kg_a, kg_b],
        merge_properties_on_conflict=True
    )

    # Check that kg_a was NOT mutated
    assert kg_a.entities[0].properties["http://example.org#jobTitle"].value == original_job_a
    assert kg_a.entities[0].properties["http://example.org#jobTitle"].value == "Engineer"

    # Check that kg_b was NOT mutated
    assert kg_b.entities[0].properties["http://example.org#jobTitle"].value == "Manager"

    # Check that the merged graph has the combined values
    assert len(merged_graph.entities) == 1
    merged_job_value = merged_graph.entities[0].properties["http://example.org#jobTitle"].value
    assert isinstance(merged_job_value, list)
    assert "Engineer" in merged_job_value
    assert "Manager" in merged_job_value


def test_interlink_does_not_mutate_on_type_mismatch(interlink_service):
    """Test that interlink() does not mutate input graphs when types don't match."""
    kg_a = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org#entity-1",
                types=["http://example.org#Person"],
                properties={"http://www.w3.org/2000/01/rdf-schema#label": LiteralValue(value="Alice")},
            )
        ]
    )
    kg_b = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org#entity-2",
                types=["http://example.org#Company"],
                properties={"http://www.w3.org/2000/01/rdf-schema#label": LiteralValue(value="Alice")},
            )
        ]
    )

    # Store original values
    original_type_a = kg_a.entities[0].types[:]
    original_type_b = kg_b.entities[0].types[:]

    # Call interlink (should not merge due to type mismatch)
    merged_graph = interlink_service.interlink(
        kbs=[kg_a, kg_b],
        merge_different_entity_types=False
    )

    # Check that input graphs were NOT mutated
    assert kg_a.entities[0].types == original_type_a
    assert kg_b.entities[0].types == original_type_b

    # Both entities should be in the merged graph separately
    assert len(merged_graph.entities) == 2


def test_interlink_does_not_mutate_on_conflict(interlink_service):
    """Test that interlink() does not mutate input graphs when there's an immutable property conflict."""
    kg_a = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org#entity-1",
                types=["http://example.org#Person"],
                properties={
                    "http://www.w3.org/2000/01/rdf-schema#label": LiteralValue(value="Alice"),
                    "http://example.org#name": LiteralValue(value="Alice Smith"),
                },
            )
        ]
    )
    kg_b = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org#entity-2",
                types=["http://example.org#Person"],
                properties={
                    "http://www.w3.org/2000/01/rdf-schema#label": LiteralValue(value="Alice"),
                    "http://example.org#name": LiteralValue(value="Alice Jones"),
                },
            )
        ]
    )

    # Store original values
    original_name_a = kg_a.entities[0].properties["http://example.org#name"].value
    original_name_b = kg_b.entities[0].properties["http://example.org#name"].value

    # Call interlink with "name" as immutable property (should not merge due to conflict)
    merged_graph = interlink_service.interlink(
        kbs=[kg_a, kg_b],
        immutable_properties=["name"]
    )

    # Check that input graphs were NOT mutated
    assert kg_a.entities[0].properties["http://example.org#name"].value == original_name_a
    assert kg_b.entities[0].properties["http://example.org#name"].value == original_name_b

    # Both entities should be in the merged graph separately due to conflict
    assert len(merged_graph.entities) == 2
