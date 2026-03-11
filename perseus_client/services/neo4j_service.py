import logging
import asyncio
from perseus_client.config import settings
from perseus_client.exceptions import ConfigurationException

try:
    from neo4j import GraphDatabase
    NEO4J_AVAILABLE = True
except ImportError:
    GraphDatabase = None
    NEO4J_AVAILABLE = False

logging.basicConfig(level=settings.loglevel.upper())
logger = logging.getLogger(__name__)


class Neo4jService:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        if not NEO4J_AVAILABLE:
            logger.warning("Neo4jService initialized but 'neo4j' library is missing.")
        self._loop = loop

    def save_output_to_neo4j(self, file_path: str):
        return self._loop.run_until_complete(
            self.save_output_to_neo4j_async(file_path)
        )

    async def save_output_to_neo4j_async(self, file_path: str):
        """
        Reads a file containing Cypher queries and executes them against a Neo4j database.

        Args:
            file_path (str): The path to the file containing Cypher queries.
        """
        logger.info(f"Reading CQL file from {file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                cql_query = f.read()
            await self.execute_cql_string_async(cql_query)
        except FileNotFoundError:
            logger.error(f"The file at {file_path} was not found.")
            raise

    def execute_cql_string(self, cql_query: str):
        """
        Executes a string containing Cypher queries against a Neo4j database.

        Args:
            cql_query (str): The string containing Cypher queries.
        """
        return self._loop.run_until_complete(self.execute_cql_string_async(cql_query))

    @staticmethod
    async def execute_cql_string_async(cql_query: str):
        """
        Asynchronously executes a string containing Cypher queries against a Neo4j database.

        Args:
            cql_query (str): The string containing Cypher queries.
        """
        if not NEO4J_AVAILABLE:
            error_msg = (
                "The 'neo4j' library is not installed. "
                "Please run `pip install perseus-client[neo4j]` to use this feature."
            )
            logger.error(error_msg)
            raise ImportError(error_msg)
        
        driver = None
        try:
            if (
                not settings.neo4j_uri
                or not settings.neo4j_user
                or not settings.neo4j_password
            ):
                raise ConfigurationException(
                    "Neo4j configuration is incomplete. Please check your settings."
                )
            driver = GraphDatabase.driver(
                settings.neo4j_uri,
                auth=(settings.neo4j_user, settings.neo4j_password),
            )
            driver.verify_connectivity()
            logger.info("Successfully connected to Neo4j.")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            if driver:
                driver.close()
            raise

        try:
            with driver.session() as session:
                # Split the query into individual statements
                statements = [s.strip() for s in cql_query.split(';') if s.strip()]
                for statement in statements:
                    try:
                        session.run(statement)
                        logger.debug(
                            f"Successfully executed query:\n{statement}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error executing query chunk:\n{statement}\nError: {e}"
                        )
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
        finally:
            if driver:
                driver.close()
            logger.info("Neo4j connection closed.")
