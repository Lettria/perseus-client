from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional
import aiohttp
import certifi
import ssl
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
        transfer_session: Optional[aiohttp.ClientSession] = None,
    ):
        self._session = session
        self.api_host = api_host
        self._loop = loop
        # Session without the API headers, for presigned-URL uploads and downloads.
        self._transfer_session = transfer_session

    @asynccontextmanager
    async def _transfer(self) -> AsyncIterator[aiohttp.ClientSession]:
        """
        Yields the shared transfer session, or a short-lived one if the service was
        created without it.
        """
        if self._transfer_session is not None:
            yield self._transfer_session
            return
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(connector=connector) as session:
            yield session

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
                    if isinstance(error_body, dict):
                        # Try common error message keys
                        error_message = (
                            error_body.get("message") or
                            error_body.get("error") or
                            error_body.get("detail") or
                            str(error_body)
                        )
                    else:
                        error_message = str(error_body)
                except Exception:
                    error_body = await response.text()
                    error_message = error_body or f"HTTP {response.status}"

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
