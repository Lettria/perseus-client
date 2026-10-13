import logging
import asyncio
import os
import time
from perseus_client.config import settings
from perseus_client.exceptions import ConfigurationException, PerseusException
from ..models import KnowledgeGraph
from .spgl_service import SPGLService, split_spgl_statements

try:
    from google.cloud import spanner

    SPANNER_AVAILABLE = True
except ImportError:
    spanner = None
    SPANNER_AVAILABLE = False

logger = logging.getLogger(__name__)

# Statements per transaction. Spanner limits a commit to 80,000 mutations.
BATCH_SIZE = 500


class SpannerGraphService:
    def __init__(self, loop: asyncio.AbstractEventLoop, timeout: int = 120):
        if not SPANNER_AVAILABLE:
            logger.info(
                "SpannerGraphService initialized but 'google-cloud-spanner' library is missing."
            )
        self._loop = loop
        self.spgl_service = SPGLService()
        self.timeout = timeout
        self._client = None
        self._database = None

    def _connect(self):
        """
        Connects to the Spanner database and creates the graph schema if needed.

        On the emulator (`SPANNER_EMULATOR_HOST` set), the instance is created if it doesn't exist.
        On real Spanner, the instance must already exist.
        """
        if self._database is not None:
            return self._database

        if (
            not getattr(settings, "spanner_project_id", None)
            or not getattr(settings, "spanner_instance_id", None)
            or not getattr(settings, "spanner_database_id", None)
        ):
            raise ConfigurationException(
                "Spanner configuration is incomplete (Project, Instance or Database ID missing). Please check your settings."
            )

        # The Spanner client only reads the emulator host from the environment
        emulator_host = getattr(settings, "spanner_emulator_host", None)
        if emulator_host:
            os.environ["SPANNER_EMULATOR_HOST"] = emulator_host
        use_emulator = bool(os.environ.get("SPANNER_EMULATOR_HOST"))

        client = spanner.Client(project=settings.spanner_project_id)
        instance = client.instance(settings.spanner_instance_id)
        if use_emulator and not instance.exists():
            instance = client.instance(
                settings.spanner_instance_id,
                configuration_name=f"projects/{settings.spanner_project_id}/instanceConfigs/emulator-config",
                node_count=1,
            )
            instance.create().result(self.timeout)
            logger.info(f"Created Spanner emulator instance {settings.spanner_instance_id}")

        database = instance.database(settings.spanner_database_id)
        if getattr(settings, "spanner_auto_create_schema", True):
            if not database.exists():
                database = instance.database(
                    settings.spanner_database_id,
                    ddl_statements=self.spgl_service.schema_ddl(),
                )
                database.create().result(self.timeout)
                logger.info(
                    f"Created Spanner database {settings.spanner_database_id} with graph {self.spgl_service.graph_name}"
                )
            elif not self._graph_exists(database):
                database.update_ddl(self.spgl_service.schema_ddl()).result(self.timeout)
                logger.info(f"Created Spanner graph {self.spgl_service.graph_name}")

        self._client = client
        self._database = database
        return database

    def _graph_exists(self, database) -> bool:
        with database.snapshot() as snapshot:
            rows = snapshot.execute_sql(
                "SELECT 1 FROM INFORMATION_SCHEMA.PROPERTY_GRAPHS WHERE PROPERTY_GRAPH_NAME = @name",
                params={"name": self.spgl_service.graph_name},
                param_types={"name": spanner.param_types.STRING},
            )
            return any(True for _ in rows)

    def ensure_schema(self):
        """Creates the tables and the property graph if they don't exist."""
        return self._loop.run_until_complete(self.ensure_schema_async())

    async def ensure_schema_async(self):
        """Asynchronously creates the tables and the property graph if they don't exist."""
        database = await self._wait_for_spanner_async()
        await asyncio.to_thread(
            lambda: database.update_ddl(self.spgl_service.schema_ddl()).result(
                self.timeout
            )
        )

    async def _wait_for_spanner_async(self):
        """Waits for the Spanner database to become available."""
        if self._database is not None:
            return self._database

        from rich.progress import Progress, SpinnerColumn, TextColumn

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(
                description="Waiting for Spanner to become available...", total=None
            )

            start_time = time.time()
            last_error = None
            while time.time() - start_time < self.timeout:
                try:
                    database = await asyncio.to_thread(self._connect)
                    progress.update(
                        task,
                        completed=True,
                        description="[green]✓[/green] Connected to Spanner.",
                    )
                    return database
                except ConfigurationException:
                    raise
                except Exception as e:
                    last_error = e
                    await asyncio.sleep(2)  # Wait before retrying

            # If the loop finishes, it's a timeout
            raise PerseusException(
                f"Timed out after {self.timeout} seconds waiting for Spanner to become available. Last error: {last_error}"
            )

    def save_output_to_spanner_graph(self, file_path: str):
        return self._loop.run_until_complete(
            self.save_output_to_spanner_graph_async(file_path)
        )

    async def save_output_to_spanner_graph_async(self, file_path: str):
        """
        Reads a file containing Spanner Graph statements (SPGL) and executes them against Spanner.

        Args:
            file_path (str): The path to the file containing SPGL statements.
        """
        logger.debug(f"Reading SPGL file from {file_path}")
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                spgl_query = f.read()
            await self.execute_spgl_string_async(spgl_query)
        except FileNotFoundError:
            logger.error(f"The file at {file_path} was not found.")
            raise

    def execute_spgl_string(self, spgl_query: str):
        """
        Executes a string containing Spanner Graph statements (SPGL) against Spanner.

        Args:
            spgl_query (str): The string containing SPGL statements.
        """
        return self._loop.run_until_complete(self.execute_spgl_string_async(spgl_query))

    async def execute_spgl_string_async(self, spgl_query: str):
        """
        Asynchronously executes a string containing Spanner Graph statements (SPGL) against Spanner.

        Statements run in order, in transactions of BATCH_SIZE statements. Unlike FalkorDB,
        a failing statement is not skipped: its transaction is rolled back and the error is raised.

        Args:
            spgl_query (str): The string containing SPGL statements.
        """
        if not SPANNER_AVAILABLE:
            error_msg = (
                "The 'google-cloud-spanner' library is not installed. "
                "Please run `pip install perseus-client[spanner]` to use this feature."
            )
            logger.error(error_msg)
            raise ImportError(error_msg)

        try:
            database = await self._wait_for_spanner_async()
            logger.info(
                f"Successfully connected to Spanner database {settings.spanner_database_id}, Graph: {self.spgl_service.graph_name}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Spanner: {e}", exc_info=True)
            raise

        statements = split_spgl_statements(spgl_query)
        logger.debug(f"Executing {len(statements)} SPGL statements against Spanner.")
        try:
            for i in range(0, len(statements), BATCH_SIZE):
                await asyncio.to_thread(
                    database.run_in_transaction,
                    self._execute_batch,
                    statements[i : i + BATCH_SIZE],
                )
        finally:
            logger.info("Spanner Graph operation finished.")

    @staticmethod
    def _execute_batch(transaction, statements):
        status, row_counts = transaction.batch_update(statements)
        if status.code != 0:
            # Raising rolls back the transaction
            raise PerseusException(
                f"Error executing statement:\n{statements[len(row_counts)]}\nError: {status.message}"
            )

    def save_to_spanner_graph(self, kg: KnowledgeGraph, strip_prefixes: bool = True):
        """
        Synchronously saves the KnowledgeGraph to Spanner Graph.
        """
        logger.debug("Attempting to save KnowledgeGraph to Spanner Graph.")
        self._loop.run_until_complete(
            self.save_to_spanner_graph_async(kg, strip_prefixes)
        )
        logger.info("Successfully saved KnowledgeGraph to Spanner Graph.")

    async def save_to_spanner_graph_async(
        self, kg: KnowledgeGraph, strip_prefixes: bool = True
    ):
        """
        Asynchronously saves the KnowledgeGraph to Spanner Graph.
        """
        logger.debug("Generating SPGL from KnowledgeGraph for Spanner Graph.")
        try:
            spgl_content = self.spgl_service.to_spgl(kg, strip_prefixes=strip_prefixes)
            if spgl_content and spgl_content.strip() != ";":
                logger.debug("SPGL content generated. Executing against Spanner.")
                await self.execute_spgl_string_async(spgl_content)
            else:
                logger.warning(
                    "KnowledgeGraph has no entities or relations to save to Spanner Graph."
                )
        except Exception as e:
            logger.error(
                f"Failed to generate or save SPGL to Spanner Graph: {e}", exc_info=True
            )
            raise

    def close(self):
        """Closes the Spanner client and its sessions."""
        if self._client is not None:
            self._client.close()
        self._client = None
        self._database = None
