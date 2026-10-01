import pytest
import aiohttp
from unittest.mock import AsyncMock, MagicMock, patch
from perseus_client.services.base_service import BaseService
from perseus_client.exceptions import APIException
import asyncio


@pytest.fixture
def mock_session():
    return MagicMock(spec=aiohttp.ClientSession)


@pytest.fixture
def base_service(mock_session):
    loop = asyncio.new_event_loop()
    return BaseService(
        session=mock_session,
        api_host="https://api.example.com",
        loop=loop
    )


def create_mock_response(status, json_data=None, json_error=None, text_data=""):
    """Helper to create a properly mocked response."""
    mock_response = MagicMock()
    mock_response.status = status

    if json_error:
        mock_response.json = AsyncMock(side_effect=json_error)
    else:
        mock_response.json = AsyncMock(return_value=json_data)

    mock_response.text = AsyncMock(return_value=text_data)
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    return mock_response


@pytest.mark.asyncio
async def test_request_error_with_message_key(base_service, mock_session):
    """Test that error responses with 'message' key are properly extracted."""
    mock_response = create_mock_response(401, json_data={"message": "Invalid API key"})
    mock_session.request = MagicMock(return_value=mock_response)

    with pytest.raises(APIException) as exc_info:
        await base_service._request("POST", "/api/v0/file")

    assert exc_info.value.status_code == 401
    assert exc_info.value.message == "Invalid API key"
    assert "Invalid API key" in str(exc_info.value)


@pytest.mark.asyncio
async def test_request_error_with_error_key(base_service, mock_session):
    """Test that error responses with 'error' key are properly extracted."""
    mock_response = create_mock_response(400, json_data={"error": "Bad request format"})
    mock_session.request = MagicMock(return_value=mock_response)

    with pytest.raises(APIException) as exc_info:
        await base_service._request("POST", "/api/v0/file")

    assert exc_info.value.status_code == 400
    assert exc_info.value.message == "Bad request format"


@pytest.mark.asyncio
async def test_request_error_with_detail_key(base_service, mock_session):
    """Test that error responses with 'detail' key are properly extracted."""
    mock_response = create_mock_response(422, json_data={"detail": "Validation failed"})
    mock_session.request = MagicMock(return_value=mock_response)

    with pytest.raises(APIException) as exc_info:
        await base_service._request("POST", "/api/v0/file")

    assert exc_info.value.status_code == 422
    assert exc_info.value.message == "Validation failed"


@pytest.mark.asyncio
async def test_request_error_with_no_standard_keys(base_service, mock_session):
    """Test that error responses without standard keys use full body."""
    mock_response = create_mock_response(500, json_data={"status": "error", "code": 500})
    mock_session.request = MagicMock(return_value=mock_response)

    with pytest.raises(APIException) as exc_info:
        await base_service._request("POST", "/api/v0/file")

    assert exc_info.value.status_code == 500
    assert "status" in exc_info.value.message
    assert "error" in exc_info.value.message


@pytest.mark.asyncio
async def test_request_error_with_text_response(base_service, mock_session):
    """Test that non-JSON error responses are handled."""
    mock_response = create_mock_response(
        500,
        json_error=Exception("Not JSON"),
        text_data="Internal Server Error"
    )
    mock_session.request = MagicMock(return_value=mock_response)

    with pytest.raises(APIException) as exc_info:
        await base_service._request("POST", "/api/v0/file")

    assert exc_info.value.status_code == 500
    assert exc_info.value.message == "Internal Server Error"


@pytest.mark.asyncio
async def test_request_success(base_service, mock_session):
    """Test successful request returns response data."""
    mock_response = create_mock_response(200, json_data={"data": "success"})
    mock_session.request = MagicMock(return_value=mock_response)

    result = await base_service._request("GET", "/api/v0/status")

    assert result == {"data": "success"}
