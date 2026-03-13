import logging
import os
import hashlib
from typing import Dict, List, Optional
from datetime import datetime
import aiohttp
import asyncio
import ssl
import certifi
import time


from .base_service import BaseService
from ..models import Ontology, OntologyStatus
from ..exceptions import PerseusException, APIException


logger = logging.getLogger(__name__)


class OntologyService(BaseService):
    def __init__(self, session, api_host, loop):
        super().__init__(session, api_host, loop)

    def create_ontology(self, name: str, source_hash: str) -> Dict:
        return self._loop.run_until_complete(
            self.create_ontology_async(name, source_hash)
        )

    def find_ontologies(
        self, ids: Optional[list[str]] = None, source_hashes: Optional[list[str]] = None
    ) -> list[Ontology]:
        return self._loop.run_until_complete(
            self.find_ontologies_async(ids, source_hashes)
        )

    def find_ontology(self, id: str) -> Optional[Ontology]:
        return self._loop.run_until_complete(self.find_ontology_async(id))

    def delete_ontology(self, ontology_id: str) -> None:
        return self._loop.run_until_complete(self.delete_ontology_async(ontology_id))

    def upload_ontology(self, ontology_path: str) -> Ontology:
        return self._loop.run_until_complete(self.upload_ontology_async(ontology_path))

    def wait_for_ontology_upload(
        self,
        ontology_id: str,
        polling_interval: float = 0.5,
        timeout: int = 3600,
    ) -> Ontology:
        return self._loop.run_until_complete(
            self.wait_for_ontology_upload_async(ontology_id, polling_interval, timeout)
        )

    async def create_ontology_async(self, name: str, source_hash: str) -> Dict:
        """
        Asynchronously creates a ontology and returns a presigned URL for uploading.
        """
        response = await self._request(
            "POST",
            "/api/v0/ontology",
            json={
                "name": name,
                "sourceHash": source_hash,
            },
        )
        ontology_data = response["ontology"]
        created_at_str = ontology_data["createdAt"].replace("Z", "+00:00")
        ontology = Ontology(
            id=ontology_data["id"],
            name=ontology_data["name"],
            status=OntologyStatus(ontology_data["status"]),
            created_at=datetime.fromisoformat(created_at_str),
        )
        upload_url = response.get("uploadUrl", "")
        return {"ontology": ontology, "upload_url": upload_url}

    async def find_ontologies_async(
        self, ids: Optional[list[str]] = None, source_hashes: Optional[list[str]] = None
    ) -> list[Ontology]:
        """
        Asynchronously finds one or more ontologies by their IDs.
        """
        logger.debug(
            "Finding ontologies with ids: %s or source_hashes: %s", ids, source_hashes
        )
        payload = {}
        if ids:
            payload["ids"] = ids
        if source_hashes:
            payload["sourceHashes"] = source_hashes
        response = await self._request("POST", "/api/v0/ontology/find", json=payload)
        ontologies: list[Ontology] = []
        for ontology_data in response["ontologies"]:
            created_at_str = ontology_data["createdAt"].replace("Z", "+00:00")
            ontology = Ontology(
                id=ontology_data["id"],
                name=ontology_data["name"],
                status=OntologyStatus(ontology_data["status"]),
                created_at=datetime.fromisoformat(created_at_str),
            )
            ontologies.append(ontology)
        return ontologies

    async def find_ontology_async(self, id: str) -> Optional[Ontology]:
        """
        Asynchronously finds an ontology by its ID.
        """
        ontologies = await self.find_ontologies_async(ids=[id])
        if not ontologies:
            return None
        return ontologies[0]

    async def delete_ontology_async(self, ontology_id: str) -> None:
        """
        Asynchronously deletes an ontology by its ID.
        """
        logger.debug(f"Attempting to delete ontology with id: {ontology_id}")
        await self._request(
            "DELETE",
            f"/api/v0/ontology/{ontology_id}",
        )
        logger.info(f"Successfully deleted ontology with id: {ontology_id}")

    async def upload_ontology_async(self, ontology_path: str) -> Ontology:
        """
        Asynchronously creates a ontology record and uploads the ontology content.
        If a ontology with the same content already exists, it will be returned.
        """
        logger.debug(f"Starting upload process for ontology: {ontology_path}")
        ontology_name = os.path.basename(ontology_path)
        try:
            with open(ontology_path, "rb") as f:
                ontology_content = f.read()
                source_hash = hashlib.sha256(ontology_content).hexdigest()
                logger.debug(f"Ontology '{ontology_name}' has hash: {source_hash}")
        except FileNotFoundError:
            logger.error(f"Local ontology file not found at: {ontology_path}")
            raise PerseusException(f"Local ontology file not found at: {ontology_path}")

        try:
            logger.debug(f"Requesting to create ontology '{ontology_name}' in API.")
            response = await self.create_ontology_async(ontology_name, source_hash)
            ontology_obj = response["ontology"]
            upload_url = response["upload_url"]

            if upload_url:
                logger.debug(f"Ontology created with ID: {ontology_obj.id}. Now uploading content to pre-signed URL.")
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                connector = aiohttp.TCPConnector(ssl=ssl_context)
                async with aiohttp.ClientSession(connector=connector) as s3_session:
                    async with s3_session.put(
                        upload_url, data=ontology_content
                    ) as resp:
                        resp.raise_for_status()
                logger.debug(f"Successfully uploaded content for ontology: {ontology_obj.id}")

        except APIException as e:
            if e.status_code == 409:
                logger.debug(
                    f"Ontology with hash {source_hash} already exists. Fetching existing ontology."
                )
                ontologies = await self.find_ontologies_async(
                    source_hashes=[source_hash]
                )
                if not ontologies:
                    logger.error(
                        f"API reported conflict for hash {source_hash}, but no ontology was found."
                    )
                    raise PerseusException(
                        f"Could not find existing ontology with hash {source_hash} after a 409 conflict."
                    ) from e
                ontology_obj = ontologies[0]
                logger.debug(f"Found existing ontology with ID: {ontology_obj.id}")
            else:
                logger.error(
                    f"API error during ontology creation or upload: {e}", exc_info=True
                )
                raise
        except aiohttp.ClientResponseError as e:
            logger.error(f"Failed to upload ontology content: {e}", exc_info=True)
            raise PerseusException(
                f"Failed to upload ontology to S3. Status: {e.status}, "
                f"Response: {e.message}"
            ) from e
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during ontology upload: {e}",
                exc_info=True,
            )
            raise PerseusException(
                f"An unexpected error occurred during ontology upload: {e}"
            ) from e
        
        return ontology_obj

    async def wait_for_ontology_upload_async(
        self,
        ontology_id: str,
        polling_interval: float = 0.5,
        timeout: int = 3600,
    ) -> Ontology:
        """
        Asynchronously waits for an ontology to be uploaded and processed.
        """
        logger.info(f"Waiting for ontology {ontology_id} to be processed by the API...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            ontology = await self.find_ontology_async(ontology_id)
            if not ontology:
                raise PerseusException(
                    f"Could not find ontology {ontology_id} during polling."
                )

            if ontology.status in [OntologyStatus.UPLOADED, OntologyStatus.FAILED]:
                if ontology.status == OntologyStatus.FAILED:
                    logger.error(f"Ontology {ontology.id} processing failed.")
                    raise PerseusException(
                        f"Ontology {ontology.id} failed to upload."
                    )
                
                logger.info(f"Ontology {ontology.id} processing complete. Status: {ontology.status.value}")
                return ontology

            logger.debug(f"Ontology {ontology_id} status is '{ontology.status.value}', continuing to wait.")
            await asyncio.sleep(polling_interval)

        raise PerseusException(f"Timeout reached waiting for ontology {ontology_id}")
