# SPDX-FileCopyrightText: 2023-present Your Name <you@example.com>
#
# SPDX-License-Identifier: MIT
"""
Configuration for the Perseus client.
"""
import sys
from typing import Optional
from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

from .exceptions import ConfigurationException

import logging


logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """
    Settings for the Perseus client.
    """

    perseus_api_host: str = "https://oath.perseus.lettria.net"
    lettria_api_key: str = ""
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    loglevel: str = "WARNING"

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="allow"
    )


def get_settings(**kwargs):
    """
    Get the settings for the Perseus client.
    """
    try:
        return Settings(**kwargs)
    except ValidationError as e:
        logger.error(e.json())
        sys.exit(1)


settings = get_settings()
