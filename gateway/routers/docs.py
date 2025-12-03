import logging
from pathlib import Path

from fastapi import APIRouter
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse

from gateway.services.docs_proxy_service import DocsProxyService


logger = logging.getLogger(__name__)

router = APIRouter(tags=["documentation"])

SWAGGER_UI_HTML = (Path(__file__).parent.parent / "templates" / "swagger_ui.html").read_text()
REDOC_HTML = (Path(__file__).parent.parent / "templates" / "redoc.html").read_text()


@router.get("/openapi.json", include_in_schema=False)
async def get_openapi_json(request: Request) -> JSONResponse:
    docs_service: DocsProxyService = request.app.state.docs_proxy_service
    try:
        schema = await docs_service.get_openapi_schema()
        return JSONResponse(schema)
    except Exception as e:
        logger.error(f"Failed to fetch OpenAPI schema: {e}")
        return JSONResponse({"error": "Failed to fetch API documentation"}, status_code=500)


@router.get("/docs", response_class=HTMLResponse, include_in_schema=False)
async def get_swagger_ui() -> HTMLResponse:
    return HTMLResponse(content=SWAGGER_UI_HTML)


@router.get("/redoc", response_class=HTMLResponse, include_in_schema=False)
async def get_redoc() -> HTMLResponse:
    return HTMLResponse(content=REDOC_HTML)


@router.delete("/docs/cache", include_in_schema=False)
async def clear_docs_cache(request: Request) -> dict[str, str]:
    docs_service: DocsProxyService = request.app.state.docs_proxy_service
    await docs_service.clear_cache()
    return {"status": "cache cleared"}
