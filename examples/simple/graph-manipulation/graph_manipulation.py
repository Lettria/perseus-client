import logging
import sys
import os
from perseus_client.client import PerseusClient
from perseus_client.models import Entity, Relation, LiteralValue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main(file_path: str):
    """
    Main function to build a graph, manipulate it, and serialize the results.
    """
    # Define output directory
    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)

    try:
        with PerseusClient() as client:
            # 1. Build the initial KnowledgeGraph from a file using the SDK
            logger.info(f"Building initial graph from file: {file_path}")
            knowledge_graphs = client.build.build_graph(
                file_paths=[file_path], metadata={"source": "graph_manipulation_example"}
            )
            if not knowledge_graphs:
                logger.error("Failed to build graph from the file.")
                return

            kg = knowledge_graphs[0]
            logger.info(
                f"Initial graph built with {len(kg.entities)} entities and {len(kg.relations)} relations."
            )

            # 2. Save the original, unmodified graph content from the API
            logger.info("--- Original Graph Serialization ---")
            if kg.ttl_content:
                original_ttl_path = os.path.join(
                    output_dir, "output_graph_original.ttl"
                )
                with open(original_ttl_path, "w", encoding="utf-8") as f:
                    f.write(kg.ttl_content)
                logger.info(
                    f"Original TTL content from API saved to {original_ttl_path}"
                )
            else:
                logger.warning("No original TTL content was returned from the API.")

            if kg.cql_content:
                original_cql_path = os.path.join(
                    output_dir, "output_graph_original.cql"
                )
                with open(original_cql_path, "w", encoding="utf-8") as f:
                    f.write(kg.cql_content)
                logger.info(
                    f"Original CQL content from API saved to {original_cql_path}"
                )
            else:
                logger.warning("No original CQL content was returned from the API.")

            # 3. Manipulate the KnowledgeGraph object
            logger.info("--- Manipulating the Graph ---")

            # Define some URIs for our new data
            award_uri = "http://example.com/award/NobelPrizeInPhysics"
            award_name_predicate = "http://www.w3.org/2000/01/rdf-schema#label"
            award_type = "http://example.com/ontology/Award"
            year_predicate = "http://example.com/ontology/year"
            won_award_predicate = "http://example.com/ontology/wonAward"
            xsd_integer = "http://www.w3.org/2001/XMLSchema#integer"

            # Add a new entity
            new_entity = Entity(
                uri=award_uri,
                types=[award_type],
                properties={
                    award_name_predicate: LiteralValue(value="Nobel Prize in Physics")
                },
            )
            kg.entities.append(new_entity)
            logger.info(f"Added new entity: {new_entity.uri}")

            # Find Marie Curie's entity to connect the new relation.
            # We search by property, looking for the literal value "Marie Curie".
            marie_curie_entity = None
            for e in kg.entities:
                for prop in e.properties.values():
                    if "Marie Curie" in str(prop.value):
                        marie_curie_entity = e
                        break
                if marie_curie_entity:
                    break

            if marie_curie_entity:
                # Add a new relation
                new_relation = Relation(
                    source_uri=marie_curie_entity.uri,
                    target_uri=new_entity.uri,
                    predicate=won_award_predicate,
                    properties={
                        year_predicate: LiteralValue(value=1903, datatype=xsd_integer)
                    },
                )
                kg.relations.append(new_relation)
                logger.info(
                    f"Added new relation: {marie_curie_entity.uri} -> {new_entity.uri}"
                )
            else:
                logger.warning(
                    "Could not find Marie Curie's entity to add the new relation."
                )

            logger.info(
                f"Graph now has {len(kg.entities)} entities and {len(kg.relations)} relations."
            )

            # 4. Serialize the modified graph to TTL and CQL using the new methods
            logger.info("--- Modified Graph Serialization ---")
            kg.save_ttl(os.path.join(output_dir, "output_graph_modified.ttl"))
            logger.info(
                f"Modified TTL content saved to {os.path.join(output_dir, 'output_graph_modified.ttl')}"
            )

            kg.save_cql(
                os.path.join(output_dir, "output_graph_modified.cql"),
                strip_prefixes=True,
            )
            logger.info(
                f"Modified CQL content saved to {os.path.join(output_dir, 'output_graph_modified.cql')}"
            )

            # Save the modified graph to Neo4j
            logger.info("Saving modified graph to Neo4j...")
            kg.save_to_neo4j(strip_prefixes=True)
            logger.info("Modified graph saved to Neo4j.")

    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asset_path = "assets/sample.txt"
    main(asset_path)
