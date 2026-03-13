from typing import Any, Optional, Callable, Awaitable, List, Union, Coroutine
import aiohttp
from ..exceptions import APIException, ConfigurationException
import logging
from perseus_client.config import settings
import asyncio

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
        logger.debug(f"Request: {method.upper()} {url}")
        try:
            async with self._session.request(method, url, **kwargs) as response:
                # Success (2xx) or No Content (204)
                if 200 <= response.status < 300:
                    logger.debug(f"Success: {method.upper()} {url} -> {response.status}")
                    if response.status == 204:
                        return None
                    return await response.json()

                # Handle errors (4xx or 5xx)
                try:
                    error_body = await response.json()
                    error_message = (
                        str(error_body.get("message"))
                        if isinstance(error_body, dict)
                        else str(error_body)
                    )
                except Exception:
                    error_body = await response.text()
                    error_message = error_body

                log_message = f"API Error: {method.upper()} {url} -> {response.status} {error_message}"

                if response.status >= 500:
                    logger.error(log_message)  # Critical server-side error
                elif response.status != 409:  # Don't spam warnings for expected conflicts
                    logger.warning(log_message)  # Client-side error

                raise APIException(
                    status_code=response.status,
                    message=error_message,
                )

        except aiohttp.ClientError as e:
            logger.error(f"Client request failed: {e}", exc_info=True)
            raise APIException(status_code=500, message=str(e)) from e


    async def _wait_for_tasks(
        self,
        tasks: List[Coroutine],
        descriptions: List[str]
    ):
        """
        Waits for multiple asyncio tasks to complete.
        """
        return await asyncio.gather(*tasks)

