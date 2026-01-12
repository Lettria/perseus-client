# SPDX-FileCopyrightText: 2023-present Your Name <you@example.com>
#
# SPDX-License-Identifier: MIT
"""
Custom exceptions for the Perseus client.
"""


class PerseusException(Exception):
    """Base exception for all Perseus client errors."""

    pass


class APIException(PerseusException):
    """Raised for errors returned by the Perseus API."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message
        super().__init__(f"API Error: {status_code}: {message}")


class ConfigurationException(PerseusException):
    """Raised for configuration errors, such as a missing API token."""

    pass
