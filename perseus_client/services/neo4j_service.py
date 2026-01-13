from neo4j import GraphDatabase
import logging
import asyncio
from perseus_client.config import settings
from perseus_client.exceptions import ConfigurationException


logging.basicConfig(level=settings.loglevel.upper())
logger = logging.getLogger(__name__)


class Neo4jService:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop

    def save_output_to_neo4j(self, file_path: str):
        return self._loop.run_until_complete(
            self.save_output_to_neo4j_async(file_path)
        )

    @staticmethod
    async def save_output_to_neo4j_async(file_path: str):
        """
        Reads a file containing Cypher queries and executes them against a Neo4j database.

        Args:
            file_path (str): The path to the file containing Cypher queries.
        """
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
            return

        try:
            with driver.session() as session:
                with open(file_path, "r", encoding="utf-8") as f:
                    query_buffer = []

                    for line in f:
                        stripped_line = line.strip()

                        if not stripped_line:
                            continue

                        query_buffer.append(line)

                        if stripped_line.endswith(";"):
                            full_query = "".join(query_buffer)

                            try:
                                session.run(full_query)
                                logger.debug(
                                    f"Successfully executed query:\n{full_query}"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Error executing query chunk:\n{full_query}\nError: {e}"
                                )

                            query_buffer = []

        except FileNotFoundError:
            logger.error(f"The file at {file_path} was not found.")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
        finally:
            driver.close()
            logger.info("Neo4j connection closed.")
