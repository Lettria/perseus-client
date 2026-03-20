import logging
import asyncio
import time
from perseus_client.config import settings
from perseus_client.exceptions import ConfigurationException, PerseusException
from ..models import KnowledgeGraph
from .cql_service import CQLService

try:
    from falkordb import FalkorDB

    FALKORDB_AVAILABLE = True
except ImportError:
    FALKORDB = None
    FALKORDB_AVAILABLE = False

logger = logging.getLogger(__name__)


class FalkorDBService:
    def __init__(self, loop: asyncio.AbstractEventLoop, timeout: int = 120):
        if not FALKORDB_AVAILABLE:
            logger.info(
                "FalkorDBService initialized but 'falkordb' library is missing."
            )
        self._loop = loop
        self.cql_service = CQLService()
        self.timeout = timeout
        self._falkordb_ready = False

    async def _wait_for_falkordb_async(self):
        """Waits for the FalkorDB database to become available."""
        if self._falkordb_ready:
            return

        from rich.progress import Progress, SpinnerColumn, TextColumn

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(
                description="Waiting for FalkorDB to become available...", total=None
            )

            start_time = time.time()
            while time.time() - start_time < self.timeout:
                try:
                    if not getattr(settings, "falkordb_host", None) or not getattr(
                        settings, "falkordb_port", None
                    ):
                        raise ConfigurationException(
                            "FalkorDB configuration is incomplete (Host or Port missing). Please check your settings."
                        )

                    driver = FalkorDB(
                        host=settings.falkordb_host,
                        port=settings.falkordb_port,
                        password=getattr(settings, "falkordb_password", None),
                        username=getattr(settings, "falkordb_username", None),
                    )
                    await asyncio.to_thread(driver.connection.ping)
                    progress.update(
                        task,
                        completed=True,
                        description="[green]✓[/green] Connected to FalkorDB.",
                    )
                    self._falkordb_ready = True
                    return
                except Exception:
                    await asyncio.sleep(2)  # Wait before retrying

            # If the loop finishes, it's a timeout
            raise PerseusException(
                f"Timed out after {self.timeout} seconds waiting for FalkorDB to become available."
            )

    def save_output_to_falkordb(self, file_path: str):
        return self._loop.run_until_complete(
            self.save_output_to_falkordb_async(file_path)
        )

    async def save_output_to_falkordb_async(self, file_path: str):
        """
        Reads a file containing Cypher queries and executes them against a FalkorDB database.

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
        Executes a string containing Cypher queries against a FalkorDB database.

        Args:
            cql_query (str): The string containing Cypher queries.
        """
        return self._loop.run_until_complete(self.execute_cql_string_async(cql_query))

    async def execute_cql_string_async(self, cql_query: str):
        """
        Asynchronously executes a string containing Cypher queries against a FalkorDB database.

        Args:
            cql_query (str): The string containing Cypher queries.
        """
        if not FALKORDB_AVAILABLE:
            error_msg = (
                "The 'FalkorDB' library is not installed. "
                "Please run `pip install perseus-client[falkordb]` to use this feature."
            )
            logger.error(error_msg)
            raise ImportError(error_msg)

        await self._wait_for_falkordb_async()

        driver = None
        try:
            if (
                not getattr(settings, "falkordb_host", None)
                or not getattr(settings, "falkordb_port", None)
                or not getattr(settings, "falkordb_graph_name", None)
            ):
                raise ConfigurationException(
                    "FalkorDB configuration is incomplete (Host, Port, or Graph Name missing). Please check your settings."
                )

            driver = FalkorDB(
                host=settings.falkordb_host,
                port=settings.falkordb_port,
                password=getattr(settings, "falkordb_password", None),
                username=getattr(settings, "falkordb_username", None),
            )

            graph = driver.select_graph(settings.falkordb_graph_name)

            logger.info(
                f"Successfully connected to FalkorDB at {settings.falkordb_host}:{settings.falkordb_port}, Graph: {settings.falkordb_graph_name}"
            )

        except Exception as e:
            logger.error(f"Failed to connect to FalkorDB: {e}", exc_info=True)
            raise

        try:
            statements = [s.strip() for s in cql_query.split(";") if s.strip()]
            logger.debug(
                f"Executing {len(statements)} CQL statements against FalkorDB."
            )
            for statement in statements:
                try:
                    logger.debug(f"Executing query:\n{statement}")
                    await asyncio.to_thread(graph.query, statement)
                except Exception as e:
                    logger.error(
                        f"Error executing query chunk:\n{statement}\nError: {e}",
                        exc_info=True,
                    )
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during the FalkorDB session: {e}",
                exc_info=True,
            )
        finally:
            logger.info("FalkorDB operation finished.")

    def save_to_falkordb(self, kg: KnowledgeGraph, strip_prefixes: bool = True):
        """
        Synchronously saves the CQL content of the KnowledgeGraph to FalkorDB.
        """
        logger.debug("Attempting to save KnowledgeGraph to FalkorDB.")
        try:
            # Note: asyncio.run() is used for synchronous context.
            # For fully async apps, call save_to_falkordb_async directly.
            self._loop.run_until_complete(
                self.save_to_falkordb_async(kg, strip_prefixes)
            )
            logger.info("Successfully saved KnowledgeGraph to FalkorDB.")
        except Exception as e:
            logger.error(f"Failed to save to FalkorDB: {e}", exc_info=True)

    async def save_to_falkordb_async(
        self, kg: KnowledgeGraph, strip_prefixes: bool = True
    ):
        """
        Asynchronously saves the CQL content of the KnowledgeGraph to FalkorDB.
        """
        logger.debug("Generating CQL from KnowledgeGraph for FalkorDB.")
        try:
            cql_content = self.cql_service.to_cql(kg, strip_prefixes=strip_prefixes)
            if cql_content and cql_content.strip() != ";":
                logger.debug("CQL content generated. Executing against FalkorDB.")
                await self.execute_cql_string_async(cql_content)
            else:
                logger.warning(
                    "KnowledgeGraph has no entities or relations to save to FalkorDB."
                )
        except Exception as e:
            logger.error(
                f"Failed to generate or save CQL to FalkorDB: {e}", exc_info=True
            )
            raise
