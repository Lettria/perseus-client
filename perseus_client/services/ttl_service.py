from typing import Dict, Any
import logging

try:
    from rdflib import Graph, URIRef, Literal
    from rdflib.namespace import Namespace

    RDFLIB_AVAILABLE = True
except ImportError:
    RDFLIB_AVAILABLE = False

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
