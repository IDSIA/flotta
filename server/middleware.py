from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp
from starlette.requests import Request
from starlette.responses import Response

from time import time

import os


class SecurityMiddleware(BaseHTTPMiddleware):
    
    def __init__(self, app: ASGIApp, skip_paths: list[str]=list()) -> None:
        super().__init__(app)

        self.skip_paths = skip_paths
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path
        headers = request.headers

        if path in self.skip_paths:
            try:
                return await call_next(request)
            except Exception as e:
                raise e

        return await call_next(request)
