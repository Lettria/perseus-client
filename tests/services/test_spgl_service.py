import pytest
from datetime import date, datetime
from unittest.mock import mock_open, patch
from perseus_client.services.spgl_service import SPGLService, split_spgl_statements
from perseus_client.models import KnowledgeGraph, Entity, Relation, LiteralValue


@pytest.fixture
def spgl_service():
    return SPGLService(graph_name="TestGraph", table_prefix="Test")


@pytest.fixture
def sample_kg():
    return KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/data/person-1",
                types=["http://example.org/Student", "http://xmlns.com/foaf/0.1/Person"],
                properties={
                    "http://www.w3.org/2000/01/rdf-schema#label": LiteralValue(value="L'Oréal\nfan"),
                    "http://example.org/age": LiteralValue(
                        value="30", datatype="http://www.w3.org/2001/XMLSchema#integer"
                    ),
                    "http://example.org/birthDate": LiteralValue(value=date(1994, 2, 1)),
                    "http://example.org/hobbies": LiteralValue(value=["reading", "coding"]),
                },
            ),
            Entity(uri="http://example.org/data/person-2"),
        ],
        relations=[
            Relation(
                source_uri="http://example.org/data/person-1",
                target_uri="http://example.org/data/person-2",
                predicate="http://xmlns.com/foaf/0.1/knows",
                properties={"http://example.org/since": LiteralValue(value=datetime(2022, 1, 1, 12, 0, 0))},
            ),
            Relation(
                source_uri="http://example.org/data/person-1",
                target_uri="http://example.org/data/person-2",
                predicate="http://example.org/likes",
            ),
        ],
        namespaces={
            "ex": "http://example.org/",
            "foaf": "http://xmlns.com/foaf/0.1/",
        },
    )


def test_schema_ddl(spgl_service):
    ddl = spgl_service.schema_ddl()
    assert len(ddl) == 3
    assert ddl[0].startswith("CREATE TABLE IF NOT EXISTS TestNode")
    assert ddl[1].startswith("CREATE TABLE IF NOT EXISTS TestEdge")
    assert "INTERLEAVE IN PARENT TestNode" in ddl[1]
    assert ddl[2].startswith("CREATE PROPERTY GRAPH IF NOT EXISTS TestGraph")
    assert "DYNAMIC LABEL (kind)" in ddl[2]


def test_to_spgl_nodes(spgl_service, sample_kg):
    statements = split_spgl_statements(spgl_service.to_spgl(sample_kg))
    # 2 nodes (1 with properties), 2 edges (1 with properties)
    assert len(statements) == 6

    assert statements[0] == (
        "INSERT OR IGNORE INTO TestNode (id, kind, types, properties)\n"
        "VALUES ('person-1', 'Person', ['Person', 'Student'], JSON '{}')"
    )
    update = statements[1]
    assert update.startswith("UPDATE TestNode\nSET properties = JSON_SET(COALESCE(properties, JSON '{}'), ")
    assert update.endswith("WHERE id = 'person-1'")
    assert """'$."label"', JSON '"L\\'Oréal\\\\nfan"'""" in update
    assert """'$."age"', JSON '30'""" in update
    assert """'$."birthDate"', JSON '"1994-02-01"'""" in update
    assert """'$."hobbies"', JSON '["reading", "coding"]'""" in update

    # No type: same fallback label as Cypher, and no UPDATE without properties
    assert statements[2] == (
        "INSERT OR IGNORE INTO TestNode (id, kind, types, properties)\n"
        "VALUES ('person-2', 'Resource', ['Resource'], JSON '{}')"
    )


def test_to_spgl_relations(spgl_service, sample_kg):
    statements = split_spgl_statements(spgl_service.to_spgl(sample_kg))

    assert statements[3] == (
        "INSERT OR IGNORE INTO TestEdge (id, dest_id, kind, properties)\n"
        "SELECT source_node.id, target_node.id, 'knows', JSON '{}'\n"
        "FROM TestNode AS source_node, TestNode AS target_node\n"
        "WHERE source_node.id = 'person-1' AND target_node.id = 'person-2'"
    )
    assert statements[4] == (
        "UPDATE TestEdge\n"
        """SET properties = JSON_SET(COALESCE(properties, JSON '{}'), '$."since"', JSON '"2022-01-01T12:00:00"')\n"""
        "WHERE id = 'person-1' AND dest_id = 'person-2' AND kind = 'knows'"
    )
    assert "'likes'" in statements[5]


def test_to_spgl_label_independent_of_type_order(spgl_service):
    # Type order isn't stable across parses: the label must not depend on it
    def node_insert(types):
        kg = KnowledgeGraph(
            entities=[Entity(uri="http://example.org/data/n-1", types=types)],
            namespaces={"ex": "http://example.org/"},
        )
        return spgl_service.to_spgl(kg)

    a, b = "http://example.org/Zebra", "http://example.org/Antelope"
    assert node_insert([a, b]) == node_insert([b, a])
    assert "VALUES ('n-1', 'Antelope', ['Antelope', 'Zebra'], JSON '{}')" in node_insert([a, b])


def test_to_spgl_types_deduplicated_and_empty_ignored(spgl_service):
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/data/n-1",
                types=["http://example.org/Fox", "http://example.org/Fox", ""],
            ),
            Entity(uri="http://example.org/data/n-2", types=[""]),
        ],
        namespaces={"ex": "http://example.org/"},
    )
    spgl = spgl_service.to_spgl(kg)
    assert "VALUES ('n-1', 'Fox', ['Fox'], JSON '{}')" in spgl
    assert "VALUES ('n-2', 'Resource', ['Resource'], JSON '{}')" in spgl


def test_to_spgl_relation_without_properties_has_no_update(spgl_service, sample_kg):
    statements = split_spgl_statements(spgl_service.to_spgl(sample_kg))
    likes = [s for s in statements if "'likes'" in s]
    assert len(likes) == 1
    assert likes[0].startswith("INSERT OR IGNORE INTO TestEdge")


def test_to_spgl_property_keys_stay_single_json_keys(spgl_service):
    kg = KnowledgeGraph(
        entities=[
            Entity(
                uri="http://example.org/data/n-1",
                properties={
                    "http://example.org/v1.2": LiteralValue(value="a"),
                    'http://example.org/say"hi"': LiteralValue(value="b"),
                },
            )
        ],
        namespaces={"ex": "http://example.org/"},
    )
    spgl = spgl_service.to_spgl(kg)
    # Quoted JSONPath member: the dot doesn't create a nested key
    assert """'$."v1.2"', JSON '"a"'""" in spgl
    assert """'$."say\\\\"hi\\\\""', JSON '"b"'""" in spgl


def test_to_spgl_keep_prefixes(spgl_service, sample_kg):
    spgl = spgl_service.to_spgl(sample_kg, strip_prefixes=False)
    assert "'ex_Student', ['ex_Student', 'foaf_Person']" in spgl
    assert "'foaf_knows'" in spgl
    assert """'$."ex_age"'""" in spgl


def test_to_spgl_empty_graph(spgl_service):
    assert spgl_service.to_spgl(KnowledgeGraph()) == ";"


def test_add_metadata_to_spgl_placeholder(spgl_service):
    with pytest.raises(NotImplementedError):
        spgl_service.add_metadata_to_spgl("", {"source": "test"})


def test_save_spgl(spgl_service, sample_kg):
    with patch("builtins.open", mock_open()) as mock_file:
        spgl_service.save_spgl(sample_kg, "graph.spgl")
    mock_file.assert_called_once_with("graph.spgl", "w", encoding="utf-8")
    mock_file().write.assert_called_once_with(spgl_service.to_spgl(sample_kg))


def test_knowledge_graph_spgl_methods(spgl_service, sample_kg):
    sample_kg._spgl_service = spgl_service
    assert sample_kg.to_spgl() == spgl_service.to_spgl(sample_kg)
    with pytest.raises(RuntimeError):
        KnowledgeGraph().to_spgl()
