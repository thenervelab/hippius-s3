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


@router.get("/openapi.json", include_in_schema=False)
async def get_openapi_json(request: Request) -> JSONResponse:
    docs_service: DocsProxyService = request.app.state.docs_proxy_service
    try:
        schema = await docs_service.get_openapi_schema()
        return JSONResponse(schema)
    except Exception as e:
        logger.error(f"Failed to fetch OpenAPI schema: {e}")
        return JSONResponse(
            {"error": "Failed to fetch API documentation"},
            status_code=500
        )


@router.get("/docs", response_class=HTMLResponse, include_in_schema=False)
async def get_swagger_ui() -> HTMLResponse:
    return HTMLResponse(content=SWAGGER_UI_HTML)


@router.get("/redoc", response_class=HTMLResponse, include_in_schema=False)
async def get_redoc() -> HTMLResponse:
    redoc_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Hippius S3 API Documentation</title>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <link href="https://fonts.googleapis.com/css?family=Montserrat:300,400,700|Roboto:300,400,700" rel="stylesheet">
        <style>
            body {
                margin: 0;
                padding: 0;
            }
        </style>
    </head>
    <body>
        <redoc spec-url='/openapi.json'></redoc>
        <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script>
    </body>
    </html>
    """
    return HTMLResponse(content=redoc_html)


@router.delete("/docs/cache", include_in_schema=False)
async def clear_docs_cache(request: Request):
    docs_service: DocsProxyService = request.app.state.docs_proxy_service
    await docs_service.clear_cache()
    return {"status": "cache cleared"}
