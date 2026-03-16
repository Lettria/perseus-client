import os
from unittest import mock
import pytest
from perseus_client.config import Settings

def test_settings_initialization_with_args():
    """
    Test that Settings initializes correctly with explicit arguments.
    """
    config = Settings(perseus_api_host="https://arg.test.com", perseus_api_key="arg_token")
    assert config.perseus_api_host == "https://arg.test.com"
    assert config.perseus_api_key == "arg_token"

@mock.patch.dict(os.environ, {"PERSEUS_API_HOST": "https://env.test.com", "PERSEUS_API_KEY": "env_token"})
def test_settings_initialization_from_env():
    """
    Test that Settings initializes correctly from environment variables.
    """
    config = Settings()
    assert config.perseus_api_host == "https://env.test.com"
    assert config.perseus_api_key == "env_token"

@mock.patch.dict(os.environ, {}, clear=True)
def test_settings_initialization_defaults():
    """
    Test that Settings initializes with default values when no env vars or args are provided.
    """
    config = Settings()
    assert config.perseus_api_host == "https://oath.perseus.lettria.net"
    assert config.perseus_api_key == ""
