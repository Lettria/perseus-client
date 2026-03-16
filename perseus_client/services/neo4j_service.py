import logging
import asyncio
import time
from perseus_client.config import settings
from perseus_client.exceptions import ConfigurationException, PerseusException
from ..models import KnowledgeGraph
from .cql_service import CQLService

try:
    from neo4j import GraphDatabase

    NEO4J_AVAILABLE = True
except ImportError:
    GraphDatabase = None
    NEO4J_AVAILABLE = False

logger = logging.getLogger(__name__)


class Neo4jService:
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        timeout: int = 120,
    ):
        if not NEO4J_AVAILABLE:
            logger.warning("Neo4jService initialized but 'neo4j' library is missing.")
        self._loop = loop
        self.cql_service = CQLService()
        self.timeout = timeout
        self._neo4j_ready = False

    async def _wait_for_neo4j_async(self):
        """Waits for the Neo4j database to become available."""
        if self._neo4j_ready:
            return

        from rich.progress import Progress, SpinnerColumn, TextColumn

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(
                description="Waiting for Neo4j to become available...", total=None
            )

            start_time = time.time()
            while time.time() - start_time < self.timeout:
                try:
                    if (
                        not settings.neo4j_uri
                        or not settings.neo4j_user
                        or not settings.neo4j_password
                    ):
                        raise ConfigurationException(
                            "Neo4j configuration is incomplete. Please check your settings."
                        )
                    with GraphDatabase.driver(
                        settings.neo4j_uri,
                        auth=(settings.neo4j_user, settings.neo4j_password),
                    ) as driver:
                        driver.verify_connectivity()
                        progress.update(
                            task,
                            completed=True,
                            description="[green]✓[/green] Connected to Neo4j.",
                        )
                        self._neo4j_ready = True
                        return
                except ConfigurationException:
                    raise
                except Exception:
                    await asyncio.sleep(2)  # Wait before retrying

            # If the loop finishes, it's a timeout
            raise PerseusException(
                f"Timed out after {self.timeout} seconds waiting for Neo4j to become available."
            )

    def save_output_to_neo4j(self, file_path: str):
        return self._loop.run_until_complete(self.save_output_to_neo4j_async(file_path))

    async def save_output_to_neo4j_async(self, file_path: str):
        """
        Reads a file containing Cypher queries and executes them against a Neo4j database.

        Args:
            file_path (str): The path to the file containing Cypher queries.
        """
        logger.debug(f"Reading CQL file from {file_path}")
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

    async def execute_cql_string_async(self, cql_query: str):
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

        await self._wait_for_neo4j_async()

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
            logger.info(f"Successfully connected to Neo4j at {settings.neo4j_uri}")
        except Exception as e:
            logger.error(f"Failed to connect to Neo4j: {e}", exc_info=True)
            if driver:
                driver.close()
            raise

        try:
            with driver.session() as session:
                statements = [s.strip() for s in cql_query.split(";") if s.strip()]
                logger.debug(
                    f"Executing {len(statements)} CQL statements against Neo4j."
                )
                for statement in statements:
                    try:
                        logger.debug(f"Executing query:\n{statement}")
                        session.run(statement)
                    except Exception as e:
                        logger.error(
                            f"Error executing query chunk:\n{statement}\nError: {e}",
                            exc_info=True,
                        )
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during the Neo4j session: {e}",
                exc_info=True,
            )
        finally:
            if driver:
                driver.close()
            logger.info("Neo4j connection closed.")

    def save_to_neo4j(self, kg: KnowledgeGraph, strip_prefixes: bool = True):
        """
        Synchronously saves the CQL content of the KnowledgeGraph to Neo4j.

        Args:
            kg: The KnowledgeGraph to save.
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties for a cleaner, more "native" Neo4j schema (e.g., `Person`, `label`).
                            If False, preserves prefixed names for higher fidelity (e.g., `dbo_Person`, `rdfs_label`).
        """
        logger.debug("Attempting to save KnowledgeGraph to Neo4j.")
        try:
            # Note: asyncio.run() is used for synchronous context.
            # For fully async apps, call save_to_neo4j_async directly.
            self._loop.run_until_complete(
                self.save_to_neo4j_async(kg, strip_prefixes=strip_prefixes)
            )
            logger.info("Successfully saved KnowledgeGraph to Neo4j.")
        except Exception as e:
            logger.error(f"Failed to save to Neo4j: {e}", exc_info=True)

    async def save_to_neo4j_async(
        self, kg: KnowledgeGraph, strip_prefixes: bool = True
    ):
        """
        Asynchronously saves the CQL content of the KnowledgeGraph to Neo4j.

        Args:
            kg: The KnowledgeGraph to save.
            strip_prefixes: If True (default), strips namespace prefixes from labels and
                            properties for a cleaner, more "native" Neo4j schema (e.g., `Person`, `label`).
                            If False, preserves prefixed names for higher fidelity (e.g., `dbo_Person`, `rdfs_label`).
        """
        logger.debug("Generating CQL from KnowledgeGraph for Neo4j.")
        try:
            cql_content = self.cql_service.to_cql(kg, strip_prefixes=strip_prefixes)
            if cql_content and cql_content.strip() != ';':
                logger.debug("CQL content generated. Executing against Neo4j.")
                await self.execute_cql_string_async(cql_content)
            else:
                logger.warning(
                    "KnowledgeGraph has no entities or relations to save to Neo4j."
                )
        except Exception as e:
            logger.error(f"Failed to generate or save CQL to Neo4j: {e}", exc_info=True)
            raise
