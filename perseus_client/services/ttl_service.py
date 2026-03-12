from typing import Dict, Any, List, Optional, TYPE_CHECKING
import logging
import uuid

try:
    from rdflib import Graph, URIRef, Literal, BNode
    from rdflib.namespace import Namespace, RDF, RDFS

    RDFLIB_AVAILABLE = True
except ImportError:
    RDFLIB_AVAILABLE = False

from ..models import KnowledgeGraph, Entity, Relation, Document

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
        g.bind("perseus-meta", metadata_ns)

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
        falkordb_service: Optional["FalkorDBService"] = None
    ) -> KnowledgeGraph:
        """
        Parses TTL content into a KnowledgeGraph object.

        Args:
            ttl_content: The Turtle file content as a string.
            neo4j_service: An optional Neo4jService instance.
            falkordb_service: An optional FalkorDBService instance.

        Returns:
            A KnowledgeGraph object.
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
            return KnowledgeGraph(neo4j_service=neo4j_service, falkordb_service=falkordb_service)

        entities: Dict[str, Entity] = {}
        relations: List[Relation] = []

        # Helper to get or create an entity
        def get_or_create_entity(subject_node) -> Entity:
            entity_id = str(subject_node)
            if entity_id not in entities:
                label = None
                # Try to find rdfs:label
                for o in g.objects(subject_node, RDFS.label):
                    label = str(o)
                    break
                if label is None:
                    # Fallback to last part of URI or a generic label
                    if isinstance(subject_node, URIRef):
                        label = str(subject_node).split('/')[-1].split('#')[-1]
                    else:
                        label = f"Node {len(entities)}"

                entity = Entity(id=entity_id, label=label, properties={})
                entities[entity_id] = entity
            return entities[entity_id]

        for s, p, o in g:
            # Treat subjects as entities
            subject_entity = get_or_create_entity(s)

            # Treat objects as entities if they are URIRef or BNode
            if isinstance(o, (URIRef, BNode)):
                object_entity = get_or_create_entity(o)
                # Create a relation
                relations.append(
                    Relation(source=subject_entity.id, target=object_entity.id, type=str(p))
                )
            elif isinstance(o, Literal):
                # Add literal properties to the subject entity
                key = str(p).split('/')[-1].split('#')[-1]
                subject_entity.properties[key] = str(o)

        # Placeholder for document - in a real scenario, this would be extracted
        # from specific triples linking entities to documents.
        # For now, we create a single generic document.
        document_id = str(uuid.uuid4())
        document = Document(id=document_id, content=ttl_content, metadata={})

        return KnowledgeGraph(
            entities=list(entities.values()), 
            relations=relations, 
            documents=[document],
            neo4j_service=neo4j_service,
            falkordb_service=falkordb_service
        )
