from typing import Callable, Coroutine, Any
from dataclasses import dataclass

from ferdelance.config import config_manager
from ferdelance.database import DataBase, AsyncSession
from ferdelance.database.repositories import ComponentRepository
from ferdelance.logging import get_logger
from ferdelance.schemas.components import Component
from ferdelance.security.algorithms import Algorithm
from ferdelance.security.exchange import Exchange
from ferdelance.security.headers import SignedHeaders

from fastapi import HTTPException, Request, Response
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.routing import APIRoute

from starlette.requests import empty_receive, empty_send
from starlette.types import Receive, Scope, Send

from cryptography.exceptions import InvalidSignature

from pathlib import Path

import asyncio


LOGGER = get_logger(__name__)


@dataclass(kw_only=True)
class SessionArgs:
    session: AsyncSession
    exc: Exchange

    self_component: Component

    ip_address: str

    lock: asyncio.Lock


@dataclass(kw_only=True)
class ValidSessionArgs(SessionArgs):
    checksum: str
    source: Component
    target: Component

    extra_headers: dict[str, str]


class SignedRequest(Request):
    def __init__(
        self,
        db_session: AsyncSession,
        self_component: Component,
        lock: asyncio.Lock,
        scope: Scope,
        receive: Receive = empty_receive,
        send: Send = empty_send,
    ):
        super().__init__(scope, receive, send)

        self.db_session: AsyncSession = db_session

        self.lock: asyncio.Lock = lock

        private_key_path: Path = config_manager.get().private_key_location()
        self.exc: Exchange = Exchange(self_component.id, private_key_path=private_key_path)

        self.ip_address: str = self.client.host if self.client else ""

        self.source_checksum: str = ""
        self.checksum: str = ""

        self.signed_in: bool = False
        self.encrypted: bool = False
        self.encryption: str = ""

        self.self_component: Component = self_component
        self.source: Component | None = None
        self.target: Component | None = None

        self.extra_headers: dict[str, str] = dict()

    def args(self) -> SessionArgs:
        return SessionArgs(
            session=self.db_session,
            exc=self.exc,
            self_component=self.self_component,
            ip_address=self.ip_address,
            lock=self.lock,
        )

    def valid_args(self) -> ValidSessionArgs:
        if self.source is None:
            LOGGER.warning(f"component=UNKNOWN: no source id defined!")
            raise HTTPException(403, "Access Denied")

        if self.target is None:
            self.target = self.self_component

        return ValidSessionArgs(
            session=self.db_session,
            exc=self.exc,
            self_component=self.self_component,
            ip_address=self.ip_address,
            checksum=self.checksum,
            source=self.source,
            target=self.target,
            extra_headers=self.extra_headers,
            lock=self.lock,
        )

    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            body: bytes = await super().body()

            try:
                if self.target is not None and self.target.id != self.self_component.id:
                    # do nothing
                    LOGGER.debug(f"Received proxy request with encrypted data")
                    body = body

                elif self.signed_in and self.source_checksum and self.source:
                    # decrypt body
                    LOGGER.debug(f"component={self.source.id}: Received signed request with encrypted data")

                    self.checksum, payload = self.exc.get_payload(body)

                    if self.source_checksum != self.checksum:
                        LOGGER.warning(f"component={self.source.id}: Checksum failed")
                        raise HTTPException(403, "Invalid Data")

                    body = payload

                elif self.encrypted:
                    # decrypt body
                    LOGGER.debug(f"Received unknown request with encrypted data")

                    self.checksum, payload = self.exc.get_payload(body)

                    if self.source_checksum != self.checksum:
                        LOGGER.warning(f"Unknown request: Checksum failed")
                        raise HTTPException(403, "Invalid Data")

                    body = payload

            except Exception as e:
                LOGGER.warning(f"Secure checks failed: {e}")
                LOGGER.exception(e)
                raise HTTPException(403, "Access Denied")

            self._body = body
        return self._body


async def check_signature(db_session: AsyncSession, request: Request, lock: asyncio.Lock) -> SignedRequest:
    cr: ComponentRepository = ComponentRepository(db_session)
    self_component = await cr.get_self_component()

    request = SignedRequest(db_session, self_component, lock, request.scope, request.receive)

    given_signature = request.headers.get("Signature", "")

    if given_signature:
        LOGGER.debug("checking authentication header")

        try:
            # decrypt header
            headers: SignedHeaders = request.exc.get_headers(given_signature)

            if headers.target_id == "JOIN":
                # this is for decrypting a new join request
                request.encrypted = True
                request.extra_headers = headers.extra
                request.source_checksum = headers.checksum
                return request

            # get request's component
            source = await cr.get_by_id(headers.source_id)

            if not source.active:
                LOGGER.warning(f"component={source.id}: request denied to inactive component")
                raise HTTPException(403, "Inactive Component")

            if source.blacklisted:
                LOGGER.warning(f"component={source.id}: request denied to blacklisted component")
                raise HTTPException(403, "Access Denied")

            # verify signature data
            request.exc.set_remote_key(source.id, source.public_key)
            request.exc.verify(f"{headers.source_id}:{headers.checksum}", headers.signature)

            request.source = source

            # check target id
            if headers.target_id == self_component.id:
                request.target = self_component
                request.exc.algorithm = Algorithm[headers.encryption]

            else:
                request.exc.algorithm = Algorithm.NO_ENCRYPTION
                request.target = await cr.get_by_id(headers.target_id)

            request.encryption = headers.encryption
            request.source_checksum = headers.checksum
            request.extra_headers = headers.extra

            request.signed_in = True
            request.encrypted = True

        except InvalidSignature as _:
            LOGGER.warning(f"component=UNKNOWN: invalid signature")
            raise HTTPException(403, "Access Denied")

        except ValueError as e:
            LOGGER.warning(f"component=UNKNOWN: access denied")
            LOGGER.exception(e)
            raise HTTPException(403, "Access Denied")

        except HTTPException as e:
            LOGGER.warning(f"component=UNKNOWN: HTTPException {e}")
            LOGGER.exception(e)
            raise e

        except Exception as e:
            LOGGER.exception(e)
            raise HTTPException(500, "Internal server error")

    return request


async def encrypt_response(request: SignedRequest, response: Response) -> Response:
    encrypted_for = response.headers.get("Encrypted_for", "")

    if encrypted_for:
        request.exc.algorithm = Algorithm.NO_ENCRYPTION
    elif request.encryption:
        request.exc.algorithm = Algorithm[request.encryption]

    if isinstance(response, FileResponse) and request.signed_in:
        path = Path(response.path)
        checksum, it = request.exc.encrypt_file_to_stream(path)

        headers = request.exc.create_signed_headers(checksum)

        response = StreamingResponse(
            it,
            headers=headers,
            media_type="application/octet-stream",
        )

    elif request.signed_in or request.encrypted:
        checksum, payload = request.exc.create_payload(response.body)

        response.headers["Content-Length"] = f"{len(payload)}"
        response.body = payload

        headers = request.exc.create_signed_headers(checksum)

    else:
        headers = {}

    for k, v in headers.items():
        response.headers[k] = v

    return response


class SignedAPIRoute(APIRoute):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lock = asyncio.Lock()

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            async with DataBase().session() as db_session:
                request = await check_signature(db_session, request, self.lock)

                response = await original_route_handler(request)

                return await encrypt_response(request, response)

        return custom_route_handler


async def session_args(request: SignedRequest) -> SessionArgs:
    return request.args()


async def valid_session_args(request: SignedRequest) -> ValidSessionArgs:
    return request.valid_args()
