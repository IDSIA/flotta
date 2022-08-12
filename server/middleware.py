from http.client import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.types import ASGIApp
from starlette.requests import Request
from starlette.responses import Response

from .security import check_token
from database import SessionLocal

import logging


class SecurityMiddleware(BaseHTTPMiddleware):

    def __init__(self, app: ASGIApp, skip_paths: list[str]=list()) -> None:
        super().__init__(app)

        self.skip_paths = skip_paths

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        path = request.url.path

        if path in self.skip_paths:
            try:
                return await call_next(request)
            except Exception as e:
                raise e

        headers = request.headers

        if 'Token' not in headers:
            logging.warning('Token not in header')
            raise HTTPException(403)

        db = SessionLocal()

        if not check_token(db, headers['Token']):
            logging.warning('Invalid token received')
            raise HTTPException(403)

        return await call_next(request)
