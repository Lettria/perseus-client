# SPDX-FileCopyrightText: 2023-present Your Name <you@example.com>
#
# SPDX-License-Identifier: MIT
"""
Data models for the Perseus client.
"""
from pydantic import BaseModel
from datetime import datetime
from enum import Enum
from typing import Optional


class FileStatus(str, Enum):
    """Enumeration for file statuses."""

    PENDING = "pending"
    UPLOADED = "uploaded"
    FAILED = "failed"


class File(BaseModel):
    """Represents a file object returned by the API."""

    id: str
    name: str
    status: FileStatus
    created_at: datetime


class OntologyStatus(str, Enum):
    """Enumeration for file statuses."""

    PENDING = "pending"
    UPLOADED = "uploaded"
    FAILED = "failed"


class Ontology(BaseModel):
    """Represents a file object returned by the API."""

    id: str
    name: str
    status: OntologyStatus
    created_at: datetime


class JobStatus(str, Enum):
    """Enumeration for job statuses."""

    PENDING = "PENDING"
    RUNNABLE = "RUNNABLE"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    FAILED = "FAILED"
    SUCCEEDED = "SUCCEEDED"


class Job(BaseModel):
    """Represents a job object returned by the API."""

    id: str
    status: JobStatus
    stopped: bool = False
