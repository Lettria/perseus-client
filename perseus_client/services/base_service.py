from typing import Any, Optional, Callable, Awaitable, List, Union, Coroutine
import aiohttp
from ..exceptions import APIException, ConfigurationException
import logging
from perseus_client.config import settings
import asyncio
from rich.progress import Progress, SpinnerColumn, TextColumn

logging.basicConfig(level=settings.loglevel.upper())
logger = logging.getLogger(__name__)


class BaseService:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_host: str,
        loop: asyncio.AbstractEventLoop,
    ):
        self._session = session
        self.api_host = api_host
        self._loop = loop

    async def _request(
        self,
        method: str,
        endpoint: str,
        **kwargs,
    ) -> Any:
        """
        Internal method to make asynchronous requests to the API.
        """
        if not self._session:
            raise ConfigurationException(
                "Client session not found. Please use the client as an async context manager, e.g., `async with PerseusClient() as client:`"
            )
        url = f"{self.api_host}{endpoint}"
        logger.debug("Making async API request: %s %s", method.upper(), url)
        try:
            async with self._session.request(method, url, **kwargs) as response:
                if response.status >= 400:
                    try:
                        error_body = await response.json()
                    except Exception:
                        error_body = await response.text()

                    if response.status >= 500:
                        logger.error(
                            "Async API request failed: %s %s -> %s",
                            method.upper(),
                            url,
                            error_body,
                        )
                    elif response.status != 409:
                        logger.debug(
                            "Async API request returned error: %s %s -> %s",
                            method.upper(),
                            url,
                            error_body,
                        )

                    raise APIException(
                        status_code=response.status,
                        message=(
                            str(error_body["message"])
                            if isinstance(error_body, dict) and "message" in error_body
                            else str(error_body)
                        ),
                    )
                if response.status == 204:
                    return None
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error("Async request failed: %s", e)
            raise APIException(status_code=500, message=str(e)) from e

    async def _wait_for_tasks(
        self,
        tasks: List[Coroutine],
        descriptions: List[str]
    ):
        """
        Waits for multiple asyncio tasks to complete, displaying a spinner for each.
        """
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            # Create a dictionary to map asyncio tasks to Rich progress task IDs
            rich_tasks = {
                progress.add_task(description, total=None): task
                for task, description in zip(tasks, descriptions)
            }
            
            # Asynchronously gather all tasks
            results = await asyncio.gather(*rich_tasks.values())
            
            # Mark all tasks as complete
            for task_id in rich_tasks.keys():
                progress.update(task_id, completed=True)
            
            return results

