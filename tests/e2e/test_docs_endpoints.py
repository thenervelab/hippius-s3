
import pytest
import requests


BASE_URL = "http://localhost:8080"


@pytest.mark.e2e
def test_docs_endpoint_returns_swagger_ui():
    response = requests.get(f"{BASE_URL}/docs")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "Hippius S3 API Documentation" in response.text
    assert "swagger-ui" in response.text


@pytest.mark.e2e
def test_redoc_endpoint_returns_redoc_ui():
    response = requests.get(f"{BASE_URL}/redoc")
    assert response.status_code == 200
    assert "text/html" in response.headers.get("content-type", "")
    assert "redoc" in response.text


@pytest.mark.e2e
def test_openapi_json_returns_valid_schema():
    response = requests.get(f"{BASE_URL}/openapi.json")
    assert response.status_code == 200
    assert response.headers.get("content-type") == "application/json"

    schema = response.json()
    assert "openapi" in schema
    assert "info" in schema
    assert schema["info"]["title"] == "Hippius S3"
    assert "paths" in schema
    assert "components" in schema


@pytest.mark.e2e
def test_docs_endpoints_no_auth_required():
    response = requests.get(f"{BASE_URL}/docs")
    assert response.status_code == 200

    response = requests.get(f"{BASE_URL}/openapi.json")
    assert response.status_code == 200


@pytest.mark.e2e
def test_bearer_token_auth_rejected():
    """Bearer token auth is not supported — all bearer requests should get 403."""
    response = requests.get(f"{BASE_URL}/test-bucket", headers={"Authorization": "Bearer hip_some_token"})

    assert response.status_code == 403
    assert b"InvalidAccessKeyId" in response.content
