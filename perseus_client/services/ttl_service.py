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
            raise ImportError(
                "The 'rdflib' library is not installed. "
                "Please run `pip install perseus-client` to use this feature."
            )

        g = Graph()
        try:
            g.parse(data=ttl_content, format="turtle")
        except Exception as e:
            logger.error(f"Failed to parse TTL content: {e}")
            # Return original content if parsing fails
            return ttl_content

        # Create a namespace for our custom metadata properties
        metadata_ns = Namespace("https://lettria.com/perseus/metadata#")
        g.bind("pmeta", metadata_ns)

        # Find all unique subjects in the graph
        subjects = set(g.subjects())

        for subject in subjects:
            # We only want to add metadata to URI subjects, not blank nodes
            if isinstance(subject, URIRef):
                for key, value in metadata.items():
                    predicate = metadata_ns[key]
                    obj = Literal(value)
                    g.add((subject, predicate, obj))

        # Serialize the graph back to a Turtle string
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
            raise ImportError(
                "The 'rdflib' library is not installed. "
                "Please run `pip install perseus-client[rdf]` to use this feature."
            )

        g = Graph()
        try:
            g.parse(data=ttl_content, format="turtle")
        except Exception as e:
            logger.error(f"Failed to parse TTL content: {e}")
            return KnowledgeGraph()

        entities: Dict[str, Entity] = {}
        relations: List[Relation] = []
        namespaces = {
            prefix: str(uri) for prefix, uri in g.namespace_manager.namespaces()
        }

        # 1. Identify all URIs that are used as classes (i.e., appear as objects of rdf:type)
        class_uris = {str(o) for s, p, o in g if p == RDF.type}

        # 2. Identify individuals: any subject that is not itself a class URI
        individual_uris = {str(s) for s, p, o in g} - class_uris
        
        # 3. Create Entity objects for all identified individuals
        for uri in individual_uris:
            entities[uri] = Entity(uri=uri)

        # 4. Iterate through all triples to populate types, properties, and relations for individuals
        for s, p, o in g:
            subject_uri = str(s)

            # Only process triples where the subject is one of our identified individuals
            if subject_uri not in entities:
                continue

            predicate_uri = str(p)

            # Case A: The triple defines a type for the individual
            if predicate_uri == str(RDF.type):
                entities[subject_uri].types.append(str(o))
            
            # Case B: The triple defines a relation between two individuals
            elif isinstance(o, (URIRef, BNode)):
                object_uri = str(o)
                # IMPORTANT: Only create a relation if the object is also an individual
                if object_uri in entities:
                    relations.append(
                        Relation(
                            source_uri=subject_uri,
                            target_uri=object_uri,
                            predicate=predicate_uri,
                        )
                    )
            
            # Case C: The triple defines a literal property for the individual
            elif isinstance(o, Literal):
                literal_value = LiteralValue(
                    value=o.value,
                    datatype=str(o.datatype) if o.datatype else None,
                )
                entities[subject_uri].properties[predicate_uri] = literal_value

        # For now, we create a single generic document.
        document_id = str(uuid.uuid4())
        document = Document(id=document_id, content=ttl_content, metadata={})

        return KnowledgeGraph(
            entities=list(entities.values()),
            relations=relations,
            documents=[document],
            namespaces=namespaces,
        )

    def to_ttl(self, kg: KnowledgeGraph) -> str:
        """
        Serializes the KnowledgeGraph to a high-fidelity Turtle (TTL) string.
        """
        if not RDFLIB_AVAILABLE:
            raise ImportError(
                "rdflib is required for TTL serialization. Please run `pip install perseus-client[rdf]`."
            )

        g = Graph()

        # Bind namespaces
        for prefix, uri in kg.namespaces.items():
            g.bind(prefix, Namespace(uri))

        # Add triples from entities
        for entity in kg.entities:
            entity_uri = URIRef(entity.uri)
            for entity_type in entity.types:
                g.add((entity_uri, RDF.type, URIRef(entity_type)))

            for predicate_uri, literal_value in entity.properties.items():
                literal_args = {}
                if literal_value.datatype:
                    literal_args["datatype"] = URIRef(literal_value.datatype)

                g.add(
                    (
                        entity_uri,
                        URIRef(predicate_uri),
                        Literal(literal_value.value, **literal_args),
                    )
                )

        # Add triples from relations
        for relation in kg.relations:
            source = URIRef(relation.source_uri)
            predicate = URIRef(relation.predicate)
            target = URIRef(relation.target_uri)
            g.add((source, predicate, target))
            # Note: Properties on relations (reification) are not handled in this serialization
            # to keep it simpler. A full reification would create more complex structures.

        return g.serialize(format="turtle")

    def save_ttl(self, kg: KnowledgeGraph, file_path: str):
        """
        Saves the KnowledgeGraph to a Turtle (TTL) file using high-fidelity serialization.
        Args:
            kg: The KnowledgeGraph object to save.
            file_path: The path to save the TTL file to.
        """
        try:
            ttl_content = self.to_ttl(kg)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(ttl_content)
        except Exception as e:
            logging.error(f"Failed to save TTL to file {file_path}: {e}")
