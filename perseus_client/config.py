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


logger = logging.getLogger(__name__)


class Settings(BaseSettings):
    """
    Settings for the Perseus client.
    """

    perseus_api_host: str = "https://oath.perseus.lettria.net"
    perseus_api_key: str = ""
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "password"
    falkordb_host: str = "localhost"
    falkordb_port: int = 6379
    falkordb_username: str = ""
    falkordb_password: str = ""
    falkordb_graph_name: str = "perseus_graph"
    spanner_project_id: str = ""
    spanner_instance_id: str = ""
    spanner_database_id: str = ""
    spanner_graph_name: str = "PerseusGraph"
    spanner_table_prefix: str = "Perseus"
    spanner_emulator_host: str = ""
    spanner_auto_create_schema: bool = True
    loglevel: str = "INFO"

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
