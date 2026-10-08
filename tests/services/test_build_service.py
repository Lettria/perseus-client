import asyncio
import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from perseus_client.models import FileStatus, JobStatus, KnowledgeGraph
from perseus_client.services.build_service import (
    BuildService,
    DEFAULT_MAX_CONCURRENCY,
)
from perseus_client.services.cql_service import CQLService
from perseus_client.services.ttl_service import TTLService


def make_build_service(**overrides) -> BuildService:
    services = dict(
        file_service=MagicMock(),
        job_service=MagicMock(),
        ontology_service=MagicMock(),
        ttl_service=TTLService(),
        cql_service=CQLService(),
        neo4j_service=MagicMock(),
        falkordb_service=MagicMock(),
        graph_service=MagicMock(),
        interlink_service=MagicMock(),
        rdflib_service=MagicMock(),
    )
    services.update(overrides)
    return BuildService(**services)


class ConcurrencyTracker:
    """Stands in for `_build_single_graph_async` and records peak concurrency."""

    def __init__(self, fail_on=None, delay=0.01):
        self.running = 0
        self.peak = 0
        self.started = []
        self.completed = []
        self.cancelled = []
        self.fail_on = fail_on
        self.delay = delay

    async def __call__(self, file_path, **kwargs):
        self.started.append(file_path)
        self.running += 1
        self.peak = max(self.peak, self.running)
        try:
            if file_path == self.fail_on:
                raise RuntimeError(f"failed: {file_path}")
            await asyncio.sleep(self.delay)
            self.completed.append(file_path)
            return KnowledgeGraph(cql_content=file_path)
        except asyncio.CancelledError:
            self.cancelled.append(file_path)
            raise
        finally:
            self.running -= 1


@pytest.mark.asyncio
async def test_build_graph_async_respects_max_concurrency():
    service = make_build_service()
    tracker = ConcurrencyTracker()
    service._build_single_graph_async = tracker
    paths = [f"file_{i}.txt" for i in range(50)]

    results = await service.build_graph_async(paths, max_concurrency=5)

    assert tracker.peak == 5
    assert [kg.cql_content for kg in results] == paths


@pytest.mark.asyncio
async def test_build_graph_async_default_concurrency_is_bounded():
    service = make_build_service()
    tracker = ConcurrencyTracker()
    service._build_single_graph_async = tracker
    paths = [f"file_{i}.txt" for i in range(DEFAULT_MAX_CONCURRENCY * 3)]

    await service.build_graph_async(paths)

    assert tracker.peak == DEFAULT_MAX_CONCURRENCY


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0, -1])
async def test_build_graph_async_rejects_invalid_max_concurrency(value):
    service = make_build_service()
    service._build_single_graph_async = AsyncMock()

    with pytest.raises(ValueError):
        await service.build_graph_async(["a.txt"], max_concurrency=value)
    service._build_single_graph_async.assert_not_called()


@pytest.mark.asyncio
async def test_build_graph_async_fail_fast_cancels_remaining():
    service = make_build_service()
    tracker = ConcurrencyTracker(fail_on="file_1.txt", delay=1)
    service._build_single_graph_async = tracker
    paths = [f"file_{i}.txt" for i in range(6)]

    with pytest.raises(RuntimeError, match="file_1.txt"):
        await service.build_graph_async(paths, max_concurrency=3)

    # Everything that started (other than the failing file) was cancelled, nothing
    # completed, and files beyond the concurrency window never started.
    assert tracker.completed == []
    assert sorted(tracker.cancelled) == sorted(set(tracker.started) - {"file_1.txt"})
    assert len(tracker.started) < len(paths)
    assert tracker.running == 0
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert pending == []


@pytest.mark.asyncio
async def test_build_graph_async_return_exceptions_keeps_partial_results():
    service = make_build_service()
    tracker = ConcurrencyTracker(fail_on="file_2.txt")
    service._build_single_graph_async = tracker
    paths = [f"file_{i}.txt" for i in range(5)]

    results = await service.build_graph_async(
        paths, max_concurrency=2, return_exceptions=True
    )

    assert len(results) == 5
    assert isinstance(results[2], RuntimeError)
    for i in [0, 1, 3, 4]:
        assert isinstance(results[i], KnowledgeGraph)
        assert results[i].cql_content == paths[i]
    assert tracker.cancelled == []


def make_single_graph_service(download_side_effect):
    file_service = MagicMock()
    file_service.upload_file_async = AsyncMock(
        return_value=SimpleNamespace(id="file1", status=FileStatus.UPLOADED)
    )
    job = SimpleNamespace(id="job1", status=JobStatus.SUCCEEDED)
    job_service = MagicMock()
    job_service.find_latest_job_async = AsyncMock(return_value=None)
    job_service.submit_job_async = AsyncMock(return_value=job)
    job_service.run_job_async = AsyncMock(return_value=job)
    job_service.download_job_output_async = AsyncMock(
        side_effect=download_side_effect
    )
    return make_build_service(file_service=file_service, job_service=job_service)


@pytest.mark.asyncio
async def test_build_single_graph_removes_temp_dir_on_success():
    output_paths = []

    async def download(job_id, output_path):
        output_paths.append(output_path)
        with open(f"{output_path}.cql", "w", encoding="utf-8") as f:
            f.write("CREATE (n)")
        return output_path

    service = make_single_graph_service(download)

    kg = await service._build_single_graph_async("doc.txt")

    assert isinstance(kg, KnowledgeGraph)
    output_dir = os.path.dirname(output_paths[0])
    assert not os.path.exists(output_dir)


@pytest.mark.asyncio
async def test_build_single_graph_removes_temp_dir_on_error():
    output_paths = []

    async def download(job_id, output_path):
        output_paths.append(output_path)
        with open(f"{output_path}.ttl", "w", encoding="utf-8") as f:
            f.write("partial")
        raise RuntimeError("download failed")

    service = make_single_graph_service(download)

    with pytest.raises(RuntimeError, match="download failed"):
        await service._build_single_graph_async("doc.txt")

    output_dir = os.path.dirname(output_paths[0])
    assert not os.path.exists(output_dir)
