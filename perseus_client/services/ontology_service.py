import logging
import os
import hashlib
from typing import Dict, List, Optional
from datetime import datetime
import aiohttp
import asyncio


from .base_service import BaseService
from ..models import Ontology, OntologyStatus
from ..exceptions import PerseusException, APIException

logging.basicConfig(level=os.getenv("LOGLEVEL", "INFO"))
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
        logger.info(f"Deleting ontology with id: {ontology_id}")
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
        ontology_name = os.path.basename(ontology_path)
        with open(ontology_path, "rb") as f:
            ontology_content = f.read()
            source_hash = hashlib.sha256(ontology_content).hexdigest()

        try:
            response = await self.create_ontology_async(ontology_name, source_hash)
            ontology_obj = response["ontology"]
            upload_url = response["upload_url"]
            if not upload_url:
                # If the ontology is newly created, an upload URL is expected.
                raise PerseusException("Failed to get upload URL for the new ontology.")
        except APIException as e:
            if e.status_code == 409:
                ontologies = await self.find_ontologies_async(
                    source_hashes=[source_hash]
                )
                if not ontologies:
                    raise PerseusException(e) from e
                return ontologies[0]
            else:
                raise

        try:
            async with aiohttp.ClientSession() as s3_session:
                async with s3_session.put(upload_url, data=ontology_content) as resp:
                    resp.raise_for_status()
        except FileNotFoundError:
            raise PerseusException(f"Local ontology not found at: {ontology_path}")
        except aiohttp.ClientResponseError as e:
            raise PerseusException(
                f"Failed to upload ontology to S3. Status: {e.status}, "
                f"Response: {e.message}"
            ) from e
        except Exception as e:
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
        ontology = await self._wait_with_spinner(
            wait_message=f"Waiting for ontology upload {ontology_id}...",
            polling_fct=self.find_ontology_async,
            polling_fct_args=[ontology_id],
            status_attribute="status",
            end_statuses=[OntologyStatus.UPLOADED, OntologyStatus.FAILED],
            polling_interval=polling_interval,
            timeout=timeout,
        )
        if ontology.status == OntologyStatus.FAILED:
            raise PerseusException(f"Ontology {ontology.id} failed to upload.")
        return ontology
