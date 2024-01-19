from typing import Callable, Coroutine, Any
from dataclasses import dataclass

from ferdelance.config import config_manager
from ferdelance.database import DataBase, AsyncSession
from ferdelance.database.repositories import ComponentRepository
from ferdelance.logging import get_logger
from ferdelance.schemas.components import Component
from ferdelance.security.algorithms import Algorithm
from ferdelance.security.exchange import Exchange

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

    content_encrypted: bool
    content_algorithm: Algorithm
    accept_encrypted: bool
    accept_algorithm: Algorithm

    identity: bool = False


@dataclass(kw_only=True)
class ValidSessionArgs(SessionArgs):
    checksum: str
    component: Component
    target: Component

    extra_headers: dict[str, str]


class SignableRequest(Request):
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

        content_encryption = self.headers.get("Content-Encryption", "").split("/")
        if "encrypted" in content_encryption:
            self.content_encrypted: bool = True
            self.content_algorithm: Algorithm = Algorithm[content_encryption[1]]
        else:
            self.content_encrypted: bool = False
            self.content_algorithm: Algorithm = Algorithm.NO_ENCRYPTION

        accept_encryption = self.headers.get("Accept-Encryption", "").split("/")
        if "encrypted" in accept_encryption:
            self.accept_encrypted: bool = True
            self.accept_algorithm: Algorithm = Algorithm[accept_encryption[1]]
        else:
            self.accept_encrypted: bool = False
            self.accept_algorithm: Algorithm = Algorithm.NO_ENCRYPTION

        private_key_path: Path = config_manager.get().private_key_location()
        self.exc: Exchange = Exchange(private_key_path, self.content_algorithm)

        self.ip_address: str = self.client.host if self.client else ""

        self.signed_checksum: str = ""
        self.checksum: str = ""

        self.self_component: Component = self_component
        self.component: Component | None = None
        self.target: Component | None = None

        if (
            self.component is not None
            and self.target is not None
            and self.component.id != self.target.id
            and self.content_encrypted
        ):
            self.content_algorithm = Algorithm.NO_ENCRYPTION

        self.extra_headers: dict[str, str] = dict()

    def args(self) -> SessionArgs:
        return SessionArgs(
            session=self.db_session,
            exc=self.exc,
            self_component=self.self_component,
            content_encrypted=self.content_encrypted,
            content_algorithm=self.content_algorithm,
            accept_encrypted=self.accept_encrypted,
            accept_algorithm=self.accept_algorithm,
            ip_address=self.ip_address,
            identity=self.component is not None,
            lock=self.lock,
        )

    def valid_args(self) -> ValidSessionArgs:
        if self.component is None:
            raise HTTPException(403, "Access Denied")

        if self.target is None:
            self.target = self.self_component

        return ValidSessionArgs(
            session=self.db_session,
            exc=self.exc,
            self_component=self.self_component,
            content_encrypted=self.content_encrypted,
            content_algorithm=self.content_algorithm,
            accept_encrypted=self.accept_encrypted,
            accept_algorithm=self.accept_algorithm,
            ip_address=self.ip_address,
            identity=self.component is not None,
            checksum=self.checksum,
            component=self.component,
            target=self.target,
            extra_headers=self.extra_headers,
            lock=self.lock,
        )

    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            body: bytes = await super().body()

            try:
                if self.content_encrypted and self.signed_checksum and self.component:
                    # decrypt body
                    LOGGER.debug(f"component={self.component.id}: Received signed request with encrypted data")

                    self.checksum, payload = self.exc.get_payload(body)

                    if self.signed_checksum != self.checksum:
                        LOGGER.warning(f"component={self.component.id}: Checksum failed")
                        raise HTTPException(403)

                    body = payload

                else:
                    LOGGER.debug("not signed request")

                    if self.content_encrypted:
                        # decrypt body
                        LOGGER.debug("received not signed request with encrypted data")

                        self.checksum, body = self.exc.get_payload(body)

            except Exception as e:
                LOGGER.warning(f"Secure checks failed: {e}")
                LOGGER.exception(e)
                raise HTTPException(403)

            self._body = body
        return self._body


async def check_signature(db_session: AsyncSession, request: Request, lock: asyncio.Lock) -> SignableRequest:
    cr: ComponentRepository = ComponentRepository(db_session)
    self_component = await cr.get_self_component()

    request = SignableRequest(db_session, self_component, lock, request.scope, request.receive)

    given_signature = request.headers.get("Signature", "")

    if given_signature:
        LOGGER.debug("checking authentication header")

        try:
            # decrypt header
            headers = request.exc.get_headers(given_signature)

            # get request's component
            component = await cr.get_by_id(headers.source_id)

            if not component.active:
                LOGGER.warning(f"component={component.id}: request denied to inactive component")
                raise HTTPException(403, "Inactive component")

            if component.blacklisted:
                LOGGER.warning(f"component={component.id}: request denied to blacklisted component")
                raise HTTPException(403, "Access Denied")

            request.exc.set_remote_key(component.public_key)

            request.component = component

            # verify signature data
            request.exc.verify(f"{headers.source_id}:{request.signed_checksum}", headers.signature)

            # check target id
            if headers.target_id == self_component.id:
                request.target = self_component
            else:
                request.target = await cr.get_by_id(headers.target_id)

            request.extra_headers = headers.extra

        except InvalidSignature | ValueError as _:
            LOGGER.warning(f"component=UNKNOWN: invalid signature")
            raise HTTPException(403, "Access Denied")

        except Exception as e:
            LOGGER.exception(e)
            raise HTTPException(500, "Internal server error")

    return request


async def encrypt_response(request: SignableRequest, response: Response) -> Response:
    args: SessionArgs = request.args()

    target_id = "" if request.component is None else request.component.id

    if isinstance(response, FileResponse) and args.accept_encrypted:
        path = Path(response.path)
        checksum, it = args.exc.encrypt_file_to_stream(path)
        stream_response = StreamingResponse(it, media_type="application/octet-stream")

        headers = args.exc.create_signed_headers(
            args.self_component.id,
            checksum,
            target_id,
        )

        response = stream_response

    elif args.accept_encrypted:
        checksum, payload = args.exc.create_payload(response.body)

        response.headers["Content-Length"] = f"{len(payload)}"
        response.body = payload

        headers = args.exc.create_signed_headers(
            args.self_component.id,
            checksum,
            target_id,
        )
    else:
        checksum = ""  # TODO: maybe set this to something and use it...
        args.exc.algorithm = Algorithm.NO_ENCRYPTION
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


async def session_args(request: SignableRequest) -> SessionArgs:
    return request.args()


async def valid_session_args(request: SignableRequest) -> ValidSessionArgs:
    return request.valid_args()
