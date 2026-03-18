from typing import TYPE_CHECKING
from rdflib import Graph, URIRef, Literal
from rdflib.namespace import RDF

if TYPE_CHECKING:
    from perseus_client.models import KnowledgeGraph


class RDFLibService:
    def to_rdflib(self, kg: "KnowledgeGraph") -> Graph:
        """Converts the knowledge graph to an rdflib.Graph object."""
        g = Graph()

        # Bind namespaces
        for prefix, uri in kg.namespaces.items():
            g.bind(prefix, URIRef(uri))

        # Add entities and their properties
        for entity in kg.entities:
            subject = URIRef(entity.uri)
            for type_uri in entity.types:
                g.add((subject, RDF.type, URIRef(type_uri)))
            for pred_uri, literal_val in entity.properties.items():
                predicate = URIRef(pred_uri)
                obj = Literal(
                    literal_val.value,
                    datatype=URIRef(literal_val.datatype)
                    if literal_val.datatype
                    else None,
                )
                g.add((subject, predicate, obj))

        # Add relations
        for relation in kg.relations:
            subject = URIRef(relation.source_uri)
            predicate = URIRef(relation.predicate)
            obj = URIRef(relation.target_uri)
            g.add((subject, predicate, obj))

        # Add relation properties
        for relation in kg.relations:
            # Re-creating the relation triple to attach properties to it
            relation_subject = URIRef(relation.source_uri)
            # This is a simplification. RDF-star or reification would be needed for full support.
            # Here we just add properties to the subject of the relation, which might not be ideal.
            for prop_predicate, prop_value in relation.properties.items():
                g.add(
                    (
                        relation_subject,
                        URIRef(prop_predicate),
                        Literal(
                            prop_value.value,
                            datatype=URIRef(prop_value.datatype)
                            if prop_value.datatype
                            else None,
                        ),
                    )
                )
        return g
