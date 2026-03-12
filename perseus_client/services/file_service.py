import logging
import hashlib
from typing import Dict, List, Optional
from datetime import datetime
import aiohttp
import os
import asyncio
import ssl
import certifi
import time
from rich.progress import Progress, SpinnerColumn, TextColumn

from .base_service import BaseService
from ..models import File, FileStatus
from ..exceptions import PerseusException, APIException


logger = logging.getLogger(__name__)


class FileService(BaseService):
    def __init__(self, session, api_host, loop):
        super().__init__(session, api_host, loop)

    def create_file(self, name: str, source_hash: str) -> Dict:
        return self._loop.run_until_complete(self.create_file_async(name, source_hash))

    def find_files(
        self,
        ids: Optional[List[str]] = None,
        source_hashes: Optional[List[str]] = None,
    ) -> list[File]:
        return self._loop.run_until_complete(self.find_files_async(ids, source_hashes))

    def find_file(self, id: str) -> Optional[File]:
        return self._loop.run_until_complete(self.find_file_async(id))

    def delete_file(self, file_id: str) -> None:
        return self._loop.run_until_complete(self.delete_file_async(file_id))

    def upload_file(self, file_path: str) -> File:
        return self._loop.run_until_complete(self.upload_file_async(file_path))

    def wait_for_file_upload(
        self,
        file_id: str,
        polling_interval: float = 0.5,
        timeout: int = 3600,
    ) -> File:
        return self._loop.run_until_complete(
            self.wait_for_file_upload_async(file_id, polling_interval, timeout)
        )

    async def create_file_async(self, name: str, source_hash: str) -> Dict:
        """
        Asynchronously creates a file and returns a presigned URL for uploading.
        """
        response = await self._request(
            "POST",
            "/api/v0/file",
            json={
                "name": name,
                "sourceHash": source_hash,
            },
        )
        file_data = response["file"]
        created_at_str = file_data["createdAt"].replace("Z", "+00:00")
        file = File(
            id=file_data["id"],
            name=file_data["name"],
            status=FileStatus(file_data["status"]),
            created_at=datetime.fromisoformat(created_at_str),
        )
        upload_url = response.get("uploadUrl", "")
        return {"file": file, "upload_url": upload_url}

    async def find_files_async(
        self,
        ids: Optional[List[str]] = None,
        source_hashes: Optional[List[str]] = None,
    ) -> list[File]:
        """
        Asynchronously finds one or more files by their IDs.
        """
        logger.debug(
            "Finding files with ids: %s or source_hashes: %s", ids, source_hashes
        )
        payload = {}
        if ids:
            payload["ids"] = ids
        if source_hashes:
            payload["sourceHashes"] = source_hashes
        response = await self._request("POST", "/api/v0/file/find", json=payload)
        files: list[File] = []
        for file_data in response["files"]:
            created_at_str = file_data["createdAt"].replace("Z", "+00:00")
            file = File(
                id=file_data["id"],
                name=file_data["name"],
                status=FileStatus(file_data["status"]),
                created_at=datetime.fromisoformat(created_at_str),
            )
            files.append(file)
        return files

    async def find_file_async(self, id: str) -> Optional[File]:
        """
        Asynchronously finds a file by its ID.
        """
        files = await self.find_files_async(ids=[id])
        if not files:
            return None
        return files[0]

    async def delete_file_async(self, file_id: str) -> None:
        """
        Asynchronously deletes a file by its ID.
        """
        logger.info(f"Deleting file with id: {file_id}")
        await self._request(
            "DELETE",
            f"/api/v0/file/{file_id}",
        )
        logger.info(f"Successfully deleted file with id: {file_id}")

    async def upload_file_async(self, file_path: str) -> File:
        """
        Asynchronously creates a file record and uploads the file content.
        If a file with the same content already exists, it will be returned.
        """
        file_name = os.path.basename(file_path)
        with open(file_path, "rb") as f:
            file_content = f.read()
            source_hash = hashlib.sha256(file_content).hexdigest()

        try:
            response = await self.create_file_async(file_name, source_hash)
            file_obj = response["file"]
            upload_url = response["upload_url"]
            if not upload_url:
                # If the file is newly created, an upload URL is expected.
                raise PerseusException("Failed to get upload URL for the new file.")
        except APIException as e:
            if e.status_code == 409:
                files = await self.find_files_async(source_hashes=[source_hash])
                if not files:
                    raise PerseusException(e) from e
                return files[0]
            else:
                raise

        try:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            async with aiohttp.ClientSession(connector=connector) as s3_session:
                async with s3_session.put(upload_url, data=file_content) as resp:
                    resp.raise_for_status()
        except FileNotFoundError:
            raise PerseusException(f"Local file not found at: {file_path}")
        except aiohttp.ClientResponseError as e:
            raise PerseusException(
                f"Failed to upload file to S3. Status: {e.status}, "
                f"Response: {e.message}"
            ) from e
        except Exception as e:
            raise PerseusException(
                f"An unexpected error occurred during file upload: {e}"
            ) from e
        return file_obj

    async def wait_for_file_upload_async(
        self,
        file_id: str,
        polling_interval: float = 0.5,
        timeout: int = 3600,
    ) -> File:
        """
        Asynchronously waits for a file to be uploaded and processed, displaying a spinner.
        """
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task_id = progress.add_task(description=f"Processing file {file_id}...", total=None)
            
            start_time = time.time()
            while time.time() - start_time < timeout:
                file = await self.find_file_async(file_id)
                if not file:
                    raise PerseusException(f"Could not find file {file_id} during polling.")

                if file.status in [FileStatus.UPLOADED, FileStatus.FAILED]:
                    if file.status == FileStatus.FAILED:
                        raise PerseusException(f"File {file.id} failed to upload.")
                    
                    progress.update(task_id, completed=True)
                    return file
                
                await asyncio.sleep(polling_interval)

            raise PerseusException(f"Timeout reached waiting for file {file_id}")
