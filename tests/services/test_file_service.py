from perseus_client.exceptions import PerseusException
import requests_mock
from perseus_client.client import PerseusClient
import pytest
import os
from faker import Faker
from unittest.mock import Mock, AsyncMock, patch
import aiohttp # Added aiohttp import

fake = Faker()

@pytest.mark.asyncio
async def test_upload_file(client: PerseusClient, tmp_path, mock_aiohttp_session_instance):
    """
    Test uploading a file.
    """
    file_path = tmp_path / "test.txt"
    file_content = b"test content"
    file_path.write_bytes(file_content)

    # Mock the initial API call to create the file record and get an upload URL
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {
        "file": {"id": "file1", "name": "test.txt", "status": "pending", "createdAt": "2023-01-01T00:00:00Z"},
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

        # IMPORTANT: put is NOT async
        mock_s3_client_session_instance.put = Mock(return_value=mock_put_cm)

        MockS3ClientSession.return_value = mock_s3_client_session_instance

        result = await client.file.upload_file_async(str(file_path))

        assert result.id == "file1"
        assert result.name == "test.txt"
        mock_aiohttp_session_instance.request.assert_called_once_with(
            "POST",
            f"{client.api_host}/api/v0/file",
            json={'name': 'test.txt', 'sourceHash': '6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72'}
        )
        mock_s3_client_session_instance.put.assert_called_once_with("http://s3.test/upload", data=file_content)
@pytest.mark.asyncio
async def test_get_file(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test getting a file.
    """
    file_id = "file_id_123"
    
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {"files": [{"id": file_id, "name": "test.txt", "status": "uploaded", "createdAt": "2023-01-01T00:00:00Z"}]}
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 200

    result = await client.file.find_file_async(file_id)
    
    assert result.id == file_id
    assert result.name == "test.txt"

@pytest.mark.asyncio
async def test_list_files(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test listing files.
    """
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.json.return_value = {"files": [
        {"id": "file1", "name": "test1.txt", "status": "uploaded", "createdAt": "2023-01-01T00:00:00Z"},
        {"id": "file2", "name": "test2.txt", "status": "uploaded", "createdAt": "2023-01-01T00:00:00Z"}
    ]}
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 200

    result = await client.file.find_files_async()
    
    assert len(result) == 2
    assert result[0].id == "file1"
    assert result[1].name == "test2.txt"

@pytest.mark.asyncio
async def test_delete_file(client: PerseusClient, mock_aiohttp_session_instance):
    """
    Test deleting a file.
    """
    file_id = "file_id_123"
    
    mock_aiohttp_session_instance.request.return_value.__aenter__.return_value.status = 204

    await client.file.delete_file_async(file_id)

    mock_aiohttp_session_instance.request.assert_called_once_with(
        "DELETE",
        f"{client.api_host}/api/v0/file/{file_id}"
    )
    
@pytest.mark.asyncio
async def test_upload_file_not_found(client: PerseusClient):
    """
    Test uploading a file that does not exist.
    """
    with pytest.raises(PerseusException):
        await client.file.upload_file_async("non_existent_file.txt")
