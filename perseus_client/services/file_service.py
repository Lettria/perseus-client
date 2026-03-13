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
        logger.info(f"Attempting to delete file with id: {file_id}")
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
        logger.info(f"Starting upload process for file: {file_path}")
        file_name = os.path.basename(file_path)
        try:
            with open(file_path, "rb") as f:
                file_content = f.read()
                source_hash = hashlib.sha256(file_content).hexdigest()
                logger.debug(f"File '{file_name}' has hash: {source_hash}")
        except FileNotFoundError:
            logger.error(f"Local file not found at: {file_path}")
            raise PerseusException(f"Local file not found at: {file_path}")

        try:
            logger.debug(f"Requesting to create file '{file_name}' in API.")
            response = await self.create_file_async(file_name, source_hash)
            file_obj = response["file"]
            upload_url = response["upload_url"]
            
            # If there is an upload_url, it means the file is new and needs to be uploaded.
            if upload_url:
                logger.info(f"File created with ID: {file_obj.id}. Now uploading content to pre-signed URL.")
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                connector = aiohttp.TCPConnector(ssl=ssl_context)
                async with aiohttp.ClientSession(connector=connector) as s3_session:
                    async with s3_session.put(upload_url, data=file_content) as resp:
                        resp.raise_for_status()
                logger.info(f"Successfully uploaded content for file: {file_obj.id}")
            else:
                # This case is logged by the 409 handling below.
                pass

        except APIException as e:
            if e.status_code == 409:
                logger.info(
                    f"File with hash {source_hash} already exists. Fetching existing file."
                )
                files = await self.find_files_async(source_hashes=[source_hash])
                if not files:
                    logger.error(
                        f"API reported conflict for hash {source_hash}, but no file was found."
                    )
                    raise PerseusException(
                        f"Could not find existing file with hash {source_hash} after a 409 conflict."
                    ) from e
                file_obj = files[0]
                logger.info(f"Found existing file with ID: {file_obj.id}")
            else:
                logger.error(f"API error during file creation or upload: {e}", exc_info=True)
                raise
        except aiohttp.ClientResponseError as e:
            logger.error(f"Failed to upload file content: {e}", exc_info=True)
            raise PerseusException(
                f"Failed to upload file to S3. Status: {e.status}, "
                f"Response: {e.message}"
            ) from e
        except Exception as e:
            logger.error(f"An unexpected error occurred during file upload: {e}", exc_info=True)
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
        Asynchronously waits for a file to be uploaded and processed.
        """
        logger.info(f"Waiting for file {file_id} to be processed by the API...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            file = await self.find_file_async(file_id)
            if not file:
                raise PerseusException(f"Could not find file {file_id} during polling.")

            if file.status in [FileStatus.UPLOADED, FileStatus.FAILED]:
                if file.status == FileStatus.FAILED:
                    logger.error(f"File {file.id} processing failed.")
                    raise PerseusException(f"File {file.id} failed to upload.")
                
                logger.info(f"File {file.id} processing complete. Status: {file.status.value}")
                return file
            
            logger.debug(f"File {file_id} status is '{file.status.value}', continuing to wait.")
            await asyncio.sleep(polling_interval)

        raise PerseusException(f"Timeout reached waiting for file {file_id}")
