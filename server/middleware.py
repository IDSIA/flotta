from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp
from starlette.requests import Request
from starlette.responses import Response

from time import time

import os


class AccessMiddleware(BaseHTTPMiddleware):
    
    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)
    
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        return await call_next(request)
