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

if TYPE_CHECKING:
    from .neo4j_service import Neo4jService
    from .falkordb_service import FalkorDBService

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
        neo4j_service: Optional["Neo4jService"] = None,
        falkordb_service: Optional["FalkorDBService"] = None,
    ) -> "KnowledgeGraph":
        """
        Parses TTL content into a high-fidelity KnowledgeGraph object, correctly
        differentiating between individuals and classes.

        Args:
            ttl_content: The Turtle file content as a string.
            neo4j_service: An optional Neo4jService instance.
            falkordb_service: An optional FalkorDBService instance.

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
            return KnowledgeGraph(
                neo4j_service=neo4j_service, falkordb_service=falkordb_service
            )

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
            neo4j_service=neo4j_service,
            falkordb_service=falkordb_service,
        )
