import logging
import asyncio
from perseus_client.config import settings
from perseus_client.exceptions import ConfigurationException

try:
    from falkordb import FalkorDB
    FALKORDB_AVAILABLE = True
except ImportError:
    FALKORDB = None
    FALKORDB_AVAILABLE = False

logging.basicConfig(level=settings.loglevel.upper())
logger = logging.getLogger(__name__)


class FalkorDBService:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        if not FALKORDB_AVAILABLE:
             logger.warning("FalkorDBService initialized but FalkorDB library is missing.")
        self._loop = loop

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
        Executes a string containing Cypher queries against a FalkorDB database.

        Args:
            cql_query (str): The string containing Cypher queries.
        """
        return self._loop.run_until_complete(self.execute_cql_string_async(cql_query))

    @staticmethod
    async def execute_cql_string_async(cql_query: str):
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
            
            await asyncio.to_thread(driver.connection.ping)
            logger.info(f"Successfully connected to FalkorDB, Graph: {settings.falkordb_graph_name}")

        except Exception as e:
            logger.error(f"Failed to connect to FalkorDB: {e}")
            raise

        try:
            statements = [s.strip() for s in cql_query.split(';') if s.strip()]
            for statement in statements:
                try:
                    await asyncio.to_thread(graph.query, statement)
                except Exception as e:
                    logger.error(
                        f"Error executing query chunk:\n{statement}\nError: {e}"
                    )
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
        finally:
            logger.info("FalkorDB operation finished.")