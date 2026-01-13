from typing import Any, Optional, Callable, Awaitable, List, Union
import aiohttp
from ..exceptions import APIException, ConfigurationException
import logging
from perseus_client.config import settings
import asyncio
import itertools
import sys
from time import time

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

    async def _wait_with_spinner(
        self,
        wait_message: str,
        polling_fct: Callable[..., Awaitable[Any]],
        polling_fct_args: List[Any],
        status_attribute: str,
        end_statuses: List[Union[Any, str]],
        polling_interval: float = 0.5,
        timeout: int = 3600,
    ) -> Any:
        async def spinner():
            for c in itertools.cycle("|/-\\"):
                sys.stdout.write(f"\r{wait_message} {c}")
                sys.stdout.flush()
                await asyncio.sleep(0.1)

        start_time = time()
        polled_object = await polling_fct(*polling_fct_args)
        if not polled_object:
            raise APIException(500, f"Could not find object with {polling_fct_args}")

        logger.debug(
            f"Object {polled_object.id} status: {getattr(polled_object, status_attribute)}"
        )

        spin_task = asyncio.create_task(spinner())

        try:
            while getattr(polled_object, status_attribute) not in end_statuses:
                if time() - start_time > timeout:
                    raise APIException(
                        500, f"Timeout reached for object {polled_object.id}"
                    )
                await asyncio.sleep(polling_interval)
                polled_object = await polling_fct(*polling_fct_args)
                if not polled_object:
                    raise APIException(
                        500, f"Could not find object with {polling_fct_args}"
                    )
                logger.debug(
                    f"Object {polled_object.id} status: {getattr(polled_object, status_attribute)}"
                )
        finally:
            spin_task.cancel()
            sys.stdout.write("\r\033[K")
        return polled_object
