# SPDX-FileCopyrightText: 2023-present Your Name <you@example.com>
#
# SPDX-License-Identifier: MIT
"""
Perseus client for Python
"""
import logging
from .config import settings, Settings
from typing import List, Optional, Dict, Any

logging.basicConfig(level=settings.loglevel)

from .client import PerseusClient
from .exceptions import PerseusException
from .models import (
    File,
    FileStatus,
    Job,
    JobStatus,
    Ontology,
    OntologyStatus,
    KnowledgeGraph,
)

import atexit

_client: Optional[PerseusClient] = None
_atexit_registered = False


def _get_client() -> PerseusClient:
    global _client, _atexit_registered
    if _client is None:
        _client = PerseusClient()
        if not _atexit_registered:
            atexit.register(close)
            _atexit_registered = True
    return _client


async def _get_client_async() -> PerseusClient:
    global _client
    if _client is None:
        _client = PerseusClient()
    return await _client.__aenter__()


def build_graph(
    file_paths: List[str],
    ontology_path: Optional[str] = None,
    refresh_graph: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[KnowledgeGraph]:
    return _get_client().build_graph(
        file_paths=file_paths,
        ontology_path=ontology_path,
        refresh_graph=refresh_graph,
        metadata=metadata,
    )


def interlink(
    kbs: List[KnowledgeGraph],
    interlinking_key_uris: List[str] = ["http://www.w3.org/2000/01/rdf-schema#label"],
    immutable_properties: Optional[List[str]] = None,
    merge_properties_on_conflict: bool = False,
) -> KnowledgeGraph:
    return _get_client().interlink(
        kbs=kbs,
        interlinking_key_uris=interlinking_key_uris,
        immutable_properties=immutable_properties,
        merge_properties_on_conflict=merge_properties_on_conflict,
    )


def upload_file(file_path: str) -> File:
    return _get_client().file.upload_file(file_path)


def find_files(
    ids: Optional[List[str]] = None,
    source_hashes: Optional[List[str]] = None,
) -> list[File]:
    return _get_client().file.find_files(ids, source_hashes)


def find_file(id: str) -> Optional[File]:
    return _get_client().file.find_file(id)


def delete_file(file_id: str) -> None:
    return _get_client().file.delete_file(file_id)


def wait_for_file_upload(
    file_id: str,
    polling_interval: float = 0.5,
    timeout: int = 3600,
) -> File:
    return _get_client().file.wait_for_file_upload(file_id, polling_interval, timeout)


def submit_job(file_id: str, ontology_id: Optional[str] = None) -> Job:
    return _get_client().job.submit_job(file_id, ontology_id)


def find_jobs(ids: List[str]) -> List[Job]:
    return _get_client().job.find_jobs(ids)


def find_job(id: str) -> Optional[Job]:
    return _get_client().job.find_job(id)


def find_latest_succeeded_job(
    file_id: str, ontology_id: Optional[str] = None
) -> Optional[Job]:
    return _get_client().job.find_latest_succeeded_job(file_id, ontology_id)


def find_latest_job(file_id: str, ontology_id: Optional[str] = None) -> Optional[Job]:
    return _get_client().job.find_latest_job(file_id, ontology_id)


def download_job_output(job_id: str, output_path: Optional[str] = None) -> str:
    return _get_client().job.download_job_output(job_id, output_path)


def run_job(
    job_id: str,
    polling_interval: int = 5,
    timeout: int = 3600,
) -> Job:
    return _get_client().job.run_job(job_id, polling_interval, timeout)


def upload_ontology(ontology_path: str) -> Ontology:
    return _get_client().ontology.upload_ontology(ontology_path)


def find_ontologies(
    ids: Optional[list[str]] = None, source_hashes: Optional[list[str]] = None
) -> list[Ontology]:
    return _get_client().ontology.find_ontologies(ids, source_hashes)


def find_ontology(id: str) -> Optional[Ontology]:
    return _get_client().ontology.find_ontology(id)


def delete_ontology(ontology_id: str) -> None:
    return _get_client().ontology.delete_ontology(ontology_id)


def wait_for_ontology_upload(
    ontology_id: str,
    polling_interval: float = 0.5,
    timeout: int = 3600,
) -> Ontology:
    return _get_client().ontology.wait_for_ontology_upload(
        ontology_id, polling_interval, timeout
    )


async def build_graph_async(
    file_paths: List[str],
    ontology_path: Optional[str] = None,
    refresh_graph: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
) -> List[KnowledgeGraph]:
    client = await _get_client_async()
    return await client.build_graph_async(
        file_paths=file_paths,
        ontology_path=ontology_path,
        refresh_graph=refresh_graph,
        metadata=metadata,
    )


async def interlink_async(
    kbs: List[KnowledgeGraph],
    interlinking_key_uris: List[str] = ["http://www.w3.org/2000/01/rdf-schema#label"],
    immutable_properties: Optional[List[str]] = None,
    merge_properties_on_conflict: bool = False,
) -> KnowledgeGraph:
    client = await _get_client_async()
    return await client.interlink_async(
        kbs=kbs,
        interlinking_key_uris=interlinking_key_uris,
        immutable_properties=immutable_properties,
        merge_properties_on_conflict=merge_properties_on_conflict,
    )


async def upload_file_async(file_path: str) -> File:
    client = await _get_client_async()
    return await client.file.upload_file_async(file_path)


async def find_files_async(
    ids: Optional[List[str]] = None,
    source_hashes: Optional[List[str]] = None,
) -> list[File]:
    client = await _get_client_async()
    return await client.file.find_files_async(ids, source_hashes)


async def find_file_async(id: str) -> Optional[File]:
    client = await _get_client_async()
    return await client.file.find_file_async(id)


async def delete_file_async(file_id: str) -> None:
    client = await _get_client_async()
    return await client.file.delete_file_async(file_id)


async def wait_for_file_upload_async(
    file_id: str,
    polling_interval: float = 0.5,
    timeout: int = 3600,
) -> File:
    client = await _get_client_async()
    return await client.file.wait_for_file_upload_async(
        file_id, polling_interval, timeout
    )


async def submit_job_async(file_id: str, ontology_id: Optional[str] = None) -> Job:
    client = await _get_client_async()
    return await client.job.submit_job_async(file_id, ontology_id)


async def find_jobs_async(ids: List[str]) -> List[Job]:
    client = await _get_client_async()
    return await client.job.find_jobs_async(ids)


async def find_job_async(id: str) -> Optional[Job]:
    client = await _get_client_async()
    return await client.job.find_job_async(id)


async def find_latest_succeeded_job_async(
    file_id: str, ontology_id: Optional[str] = None
) -> Optional[Job]:
    client = await _get_client_async()
    return await client.job.find_latest_succeeded_job_async(file_id, ontology_id)


async def find_latest_job_async(
    file_id: str, ontology_id: Optional[str] = None
) -> Optional[Job]:
    client = await _get_client_async()
    return await client.job.find_latest_job_async(file_id, ontology_id)


async def download_job_output_async(
    job_id: str, output_path: Optional[str] = None
) -> str:
    client = await _get_client_async()
    return await client.job.download_job_output_async(job_id, output_path)


async def run_job_async(
    job_id: str,
    polling_interval: int = 5,
    timeout: int = 3600,
) -> Job:
    client = await _get_client_async()
    return await client.job.run_job_async(job_id, polling_interval, timeout)


async def upload_ontology_async(ontology_path: str) -> Ontology:
    client = await _get_client_async()
    return await client.ontology.upload_ontology_async(ontology_path)


async def find_ontologies_async(
    ids: Optional[list[str]] = None, source_hashes: Optional[list[str]] = None
) -> list[Ontology]:
    client = await _get_client_async()
    return await client.ontology.find_ontologies_async(ids, source_hashes)


async def find_ontology_async(id: str) -> Optional[Ontology]:
    client = await _get_client_async()
    return await client.ontology.find_ontology_async(id)


async def delete_ontology_async(ontology_id: str) -> None:
    client = await _get_client_async()
    return await client.ontology.delete_ontology_async(ontology_id)


async def wait_for_ontology_upload_async(
    ontology_id: str,
    polling_interval: float = 0.5,
    timeout: int = 3600,
) -> Ontology:
    client = await _get_client_async()
    return await client.ontology.wait_for_ontology_upload_async(
        ontology_id, polling_interval, timeout
    )


def close():
    if _client is not None:
        _client.close()


async def close_async():
    if _client is not None:
        await _client.__aexit__(None, None, None)


__all__ = [
    "PerseusClient",
    "PerseusException",
    "File",
    "FileStatus",
    "Job",
    "JobStatus",
    "Ontology",
    "OntologyStatus",
    "KnowledgeGraph",
    "build_graph",
    "interlink",
    "upload_file",
    "find_files",
    "find_file",
    "delete_file",
    "wait_for_file_upload",
    "submit_job",
    "find_jobs",
    "find_job",
    "find_latest_succeeded_job",
    "find_latest_job",
    "download_job_output",
    "run_job",
    "upload_ontology",
    "find_ontologies",
    "find_ontology",
    "delete_ontology",
    "wait_for_ontology_upload",
    "close",
    "close_async",
    "build_graph_async",
    "interlink_async",
    "upload_file_async",
    "find_files_async",
    "find_file_async",
    "delete_file_async",
    "wait_for_file_upload_async",
    "submit_job_async",
    "find_jobs_async",
    "find_job_async",
    "find_latest_succeeded_job_async",
    "find_latest_job_async",
    "download_job_output_async",
    "run_job_async",
    "upload_ontology_async",
    "find_ontologies_async",
    "find_ontology_async",
    "delete_ontology_async",
    "wait_for_ontology_upload_async",
]
