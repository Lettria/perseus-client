from typing import Dict, Any, List, Optional, TYPE_CHECKING
import logging
import uuid

try:
    from rdflib import Graph, URIRef, Literal, BNode
    from rdflib.namespace import Namespace, RDF, RDFS

    RDFLIB_AVAILABLE = True
except ImportError:
    RDFLIB_AVAILABLE = False

from ..models import KnowledgeGraph, Entity, Relation, Document, LiteralValue

logger = logging.getLogger(__name__)


class TTLService:
    """
    A service for manipulating RDF Turtle (TTL) files.
    """

    def __init__(self):
        if not RDFLIB_AVAILABLE:
            logger.warning(
                "TTLService initialized but 'rdflib' library is missing. "
                "Please run `pip install perseus-client[rdf]` to use this feature."
            )

    def add_metadata_to_ttl(self, ttl_content: str, metadata: Dict[str, Any]) -> str:
        """
        Adds a dictionary of metadata to all subjects in a Turtle file's content.

        Args:
            ttl_content: The Turtle file content as a string.
            metadata: A dictionary where keys are metadata property names and values are their values.

        Returns:
            The modified Turtle content as a string.
        """
        if not RDFLIB_AVAILABLE:
            logger.error("rdflib is not installed, which is required for TTL manipulation.")
            raise ImportError(
                "The 'rdflib' library is not installed. "
                "Please run `pip install perseus-client[rdf]` to use this feature."
            )

        logger.debug("Parsing TTL content to add metadata.")
        g = Graph()
        try:
            g.parse(data=ttl_content, format="turtle")
        except Exception as e:
            logger.error(f"Failed to parse TTL content: {e}", exc_info=True)
            return ttl_content

        metadata_ns = Namespace("https://lettria.com/perseus/metadata#")
        g.bind("pmeta", metadata_ns)

        subjects = set(g.subjects())
        logger.debug(f"Adding metadata to {len(subjects)} subjects in the graph.")
        for subject in subjects:
            if isinstance(subject, URIRef):
                for key, value in metadata.items():
                    predicate = metadata_ns[key]
                    obj = Literal(value)
                    g.add((subject, predicate, obj))

        return g.serialize(format="turtle")

    def parse_ttl_to_knowledge_graph(
        self,
        ttl_content: str,
    ) -> "KnowledgeGraph":
        """
        Parses TTL content into a high-fidelity KnowledgeGraph object, correctly
        differentiating between individuals and classes.

        Args:
            ttl_content: The Turtle file content as a string.

        Returns:
            A KnowledgeGraph object populated with rich data representing only individuals.
        """
        if not RDFLIB_AVAILABLE:
            logger.error("rdflib is not installed, which is required for parsing TTL.")
            raise ImportError(
                "The 'rdflib' library is not installed. "
                "Please run `pip install perseus-client[rdf]` to use this feature."
            )

        logger.debug("Parsing TTL content into a KnowledgeGraph object.")
        g = Graph()
        try:
            g.parse(data=ttl_content, format="turtle")
            logger.debug(f"Successfully parsed {len(g)} triples from TTL content.")
        except Exception as e:
            logger.error(f"Failed to parse TTL content: {e}", exc_info=True)
            return KnowledgeGraph()

        entities: Dict[str, Entity] = {}
        relations: List[Relation] = []
        namespaces = {
            prefix: str(uri) for prefix, uri in g.namespace_manager.namespaces()
        }

        class_uris = {str(o) for s, p, o in g if p == RDF.type}
        individual_uris = {str(s) for s, p, o in g} - class_uris
        logger.debug(f"Identified {len(individual_uris)} individuals and {len(class_uris)} classes.")
        
        for uri in individual_uris:
            entities[uri] = Entity(uri=uri)

        for s, p, o in g:
            subject_uri = str(s)
            if subject_uri not in entities:
                continue

            predicate_uri = str(p)
            if predicate_uri == str(RDF.type):
                entities[subject_uri].types.append(str(o))
            elif isinstance(o, (URIRef, BNode)):
                object_uri = str(o)
                if object_uri in entities:
                    relations.append(
                        Relation(
                            source_uri=subject_uri,
                            target_uri=object_uri,
                            predicate=predicate_uri,
                        )
                    )
            elif isinstance(o, Literal):
                entities[subject_uri].properties[predicate_uri] = LiteralValue(
                    value=o.value,
                    datatype=str(o.datatype) if o.datatype else None,
                )

        document = Document(id=str(uuid.uuid4()), content=ttl_content, metadata={})
        
        kg = KnowledgeGraph(
            entities=list(entities.values()),
            relations=relations,
            documents=[document],
            namespaces=namespaces,
        )
        logger.info(f"Successfully created KnowledgeGraph with {len(kg.entities)} entities and {len(kg.relations)} relations.")
        return kg

    def to_ttl(self, kg: KnowledgeGraph) -> str:
        """
        Serializes the KnowledgeGraph to a high-fidelity Turtle (TTL) string.
        """
        logger.debug(f"Serializing KnowledgeGraph with {len(kg.entities)} entities to TTL format.")
        if not RDFLIB_AVAILABLE:
            logger.error("rdflib is not installed, which is required for TTL serialization.")
            raise ImportError(
                "rdflib is required for TTL serialization. Please run `pip install perseus-client[rdf]`."
            )

        g = Graph()
        for prefix, uri in kg.namespaces.items():
            g.bind(prefix, Namespace(uri))

        for entity in kg.entities:
            entity_uri = URIRef(entity.uri)
            for entity_type in entity.types:
                g.add((entity_uri, RDF.type, URIRef(entity_type)))

            for predicate_uri, literal_value in entity.properties.items():
                predicate = URIRef(predicate_uri)
                literal_args = {}
                if literal_value.datatype:
                    literal_args["datatype"] = URIRef(literal_value.datatype)

                if isinstance(literal_value.value, list):
                    for item in literal_value.value:
                        g.add(
                            (
                                entity_uri,
                                predicate,
                                Literal(item, **literal_args),
                            )
                        )
                else:
                    g.add(
                        (
                            entity_uri,
                            predicate,
                            Literal(literal_value.value, **literal_args),
                        )
                    )

        for relation in kg.relations:
            source = URIRef(relation.source_uri)
            predicate = URIRef(relation.predicate)
            target = URIRef(relation.target_uri)
            g.add((source, predicate, target))

        serialized_ttl = g.serialize(format="turtle")
        logger.debug(f"Successfully serialized KnowledgeGraph to TTL string ({len(serialized_ttl)} bytes).")
        return serialized_ttl

    def save_ttl(self, kg: KnowledgeGraph, file_path: str):
        """
        Saves the KnowledgeGraph to a Turtle (TTL) file using high-fidelity serialization.
        Args:
            kg: The KnowledgeGraph object to save.
            file_path: The path to save the TTL file to.
        """
        logger.debug(f"Saving KnowledgeGraph to TTL file at: {file_path}")
        try:
            ttl_content = self.to_ttl(kg)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(ttl_content)
            logger.info(f"Successfully saved TTL file to {file_path}.")
        except Exception as e:
            logger.error(f"Failed to save TTL to file {file_path}: {e}", exc_info=True)
            raise
