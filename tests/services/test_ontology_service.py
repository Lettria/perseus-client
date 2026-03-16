from perseus_client.exceptions import PerseusException
from perseus_client.client import PerseusClient
import pytest
import os
from faker import Faker
from unittest.mock import Mock, AsyncMock, patch
import aiohttp # Added aiohttp import

fake = Faker()

@pytest.mark.asyncio
async def test_upload_ontology(client: PerseusClient, tmp_path, mock_aiohttp_session_instance):
    """
    Test uploading an ontology.
    """
    ontology_path = tmp_path / "ontology.ttl"
    ontology_content = b"<#test> a <#ontology>."
    ontology_path.write_bytes(ontology_content)

    # Mock the initial API call to create the ontology record and get an upload URL
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "ontology": {"id": "ontology1", "name": "ontology.ttl", "status": "pending", "createdAt": "2023-01-01T00:00:00Z"},
        "uploadUrl": "http://s3.test/upload"
    }
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 200

    # Mock the S3 upload by patching aiohttp.ClientSession directly
    with patch('aiohttp.ClientSession') as MockS3ClientSession:
        mock_s3_client_session_instance = AsyncMock()
        mock_s3_client_session_instance.__aenter__.return_value = mock_s3_client_session_instance
        mock_s3_client_session_instance.__aexit__.return_value = None

        mock_response = AsyncMock(spec=aiohttp.ClientResponse)
        mock_response.raise_for_status.return_value = None
        mock_response.status = 200

        mock_put_cm = AsyncMock()
        mock_put_cm.__aenter__.return_value = mock_response
        mock_put_cm.__aexit__.return_value = None

        mock_s3_client_session_instance.put = Mock(return_value=mock_put_cm)

        MockS3ClientSession.return_value = mock_s3_client_session_instance

        result = await client.ontology.upload_ontology_async(str(ontology_path))

        assert result.id == "ontology1"
        assert result.name == "ontology.ttl"
        mock_aiohttp_session_instance.request.assert_called_once_with(
            "POST",
            f"{client.api_host}/api/v0/ontology",
            json={'name': 'ontology.ttl', 'sourceHash': 'c3454c88236b369ca42712f9aee0d4c4130c313d5ba8ac97903953dd535cdd36'}
        )
        mock_s3_client_session_instance.put.assert_called_once_with("http://s3.test/upload", data=ontology_content)

@pytest.mark.asyncio
async def test_get_ontology(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test getting an ontology.
    """
    ontology_id = "ontology_id_123"
    
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {"ontologies": [{"id": ontology_id, "name": "test.ttl", "status": "uploaded", "createdAt": "2023-01-01T00:00:00Z"}]}
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 200

    result = await client.ontology.find_ontology_async(ontology_id)
    
    assert result.id == ontology_id
    assert result.name == "test.ttl"

@pytest.mark.asyncio
async def test_list_ontologies(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test listing ontologies.
    """
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {"ontologies": [
        {"id": "ontology1", "name": "test1.ttl", "status": "uploaded", "createdAt": "2023-01-01T00:00:00Z"},
        {"id": "ontology2", "name": "test2.ttl", "status": "uploaded", "createdAt": "2023-01-01T00:00:00Z"}
    ]}
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 200

    result = await client.ontology.find_ontologies_async()
    
    assert len(result) == 2
    assert result[0].id == "ontology1"
    assert result[1].name == "test2.ttl"

@pytest.mark.asyncio
async def test_delete_ontology(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test deleting an ontology.
    """
    ontology_id = "ontology_id_123"
    
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 204

    await client.ontology.delete_ontology_async(ontology_id)

    mock_aiohttp_session_instance.request.assert_called_once_with(
        "DELETE",
        f"{client.api_host}/api/v0/ontology/{ontology_id}"
    )
    
@pytest.mark.asyncio
async def test_upload_ontology_not_found(client: PerseusClient):
    """
    Test uploading an ontology that does not exist.
    """
    with pytest.raises(PerseusException):
        await client.ontology.upload_ontology_async("non_existent_ontology.ttl")
