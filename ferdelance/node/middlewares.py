from typing import Callable, Coroutine, Any
from dataclasses import dataclass


from ferdelance.database import DataBase, AsyncSession
from ferdelance.database.repositories import ComponentRepository
from ferdelance.logging import get_logger
from ferdelance.node.services import SecurityService
from ferdelance.schemas.components import Component

from fastapi import HTTPException, Request, Response
from fastapi.responses import FileResponse
from fastapi.routing import APIRoute

from starlette.types import Receive, Scope, Send

LOGGER = get_logger(__name__)


@dataclass(kw_only=True)
class SessionArgs:
    session: AsyncSession
    security_service: SecurityService

    self_component: Component

    ip_address: str

    content_encrypted: bool = False
    accept_encrypted: bool = False

    identity: bool = False


@dataclass(kw_only=True)
class ValidSessionArgs(SessionArgs):
    checksum: str
    component: Component


class SignableRequest(Request):
    def __init__(
        self,
        db_session: AsyncSession,
        self_component: Component,
        scope: Scope,
        receive: Receive = ...,
        send: Send = ...,
    ):
        super().__init__(scope, receive, send)

        self.db_session: AsyncSession = db_session
        self.security: SecurityService = SecurityService(db_session)

        self.content_encrypted: bool = "encrypted" in self.headers.get("Content-Encoding", "").split("/")
        self.accept_encrypted: bool = "encrypted" in self.headers.get("Accept-Encoding", "").split("/")

        self.ip_address: str = self.client.host if self.client else ""

        self.signed_checksum: str = ""
        self.checksum: str = ""

        self.self_component: Component = self_component
        self.component: Component | None = None

    def args(self) -> SessionArgs:
        return SessionArgs(
            session=self.db_session,
            security_service=self.security,
            self_component=self.self_component,
            content_encrypted=self.content_encrypted,
            accept_encrypted=self.accept_encrypted,
            ip_address=self.ip_address,
            identity=self.component is not None,
        )

    def valid_args(self) -> ValidSessionArgs:
        if self.component is None:
            raise HTTPException(403, "Access Denied")

        return ValidSessionArgs(
            session=self.db_session,
            security_service=self.security,
            self_component=self.self_component,
            content_encrypted=self.content_encrypted,
            accept_encrypted=self.accept_encrypted,
            ip_address=self.ip_address,
            identity=self.component is not None,
            checksum=self.checksum,
            component=self.component,
        )

    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            body: bytes = await super().body()

            try:
                if self.content_encrypted and self.signed_checksum and self.component:
                    # decrypt body
                    LOGGER.debug(f"component_id={self.component.id}: Received signed request with encrypted data")

                    self.checksum, payload = self.security.exc.get_payload(body)

                    if self.signed_checksum != self.checksum:
                        LOGGER.warning(f"component_id={self.component.id}: Checksum failed")
                        raise HTTPException(403)

                    body = payload

                else:
                    LOGGER.debug("not signed request")

                    if self.content_encrypted:
                        # decrypt body
                        LOGGER.debug("Received not signed request with encrypted data")

                        self.checksum, body = self.security.exc.get_payload(body)

            except Exception as e:
                LOGGER.warning(f"Secure checks failed: {e}")
                LOGGER.exception(e)
                raise HTTPException(403)

            self._body = body
        return self._body


async def check_signature(db_session: AsyncSession, request: Request) -> SignableRequest:
    cr: ComponentRepository = ComponentRepository(db_session)
    self_component = await cr.get_self_component()

    request = SignableRequest(db_session, self_component, request.scope, request.receive)
    await request.security.setup()

    given_signature = request.headers.get("Signature", "")
    if given_signature:
        LOGGER.debug("checking authentication header")

        # decrypt header
        component_id, request.signed_checksum, signature = request.security.get_headers(given_signature)

        # get request's component
        component = await cr.get_by_id(component_id)

        if not component.active:
            LOGGER.warning(f"component_id={component.id}: request denied to inactive component")
            raise HTTPException(403, "Inactive component")

        if component.blacklisted:
            LOGGER.warning(f"component_id={component.id}: request denied to blacklisted component")
            raise HTTPException(403, "Access Denied")

        await request.security.setup(component.public_key)

        request.component = component

        # verify signature data
        request.security.verify_signature_data(component_id, request.signed_checksum, signature)

    return request


async def encrypt_response(request: SignableRequest, response: Response) -> Response:
    args: SessionArgs = request.args()

    if isinstance(response, FileResponse) and args.accept_encrypted:
        checksum, stream_response = args.security_service.encrypt_file(response.path)

        headers = args.security_service.exc.create_signed_header(
            args.self_component.id,
            checksum,
            args.accept_encrypted,
        )

        response = stream_response

    elif args.accept_encrypted:
        checksum, payload = args.security_service.exc.create_payload(response.body)

        response.headers["Content-Length"] = f"{len(payload)}"
        response.body = payload

        headers = args.security_service.exc.create_signed_header(
            args.self_component.id,
            checksum,
            args.accept_encrypted,
        )
    else:
        checksum = ""  # TODO: maybe set this to something and use it...
        headers = args.security_service.exc.create_header(False)

    for k, v in headers.items():
        response.headers[k] = v

    return response


class SignedAPIRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            async with DataBase().session() as db_session:
                request = await check_signature(db_session, request)

                response = await original_route_handler(request)

                return await encrypt_response(request, response)

        return custom_route_handler


async def session_args(request: SignableRequest) -> SessionArgs:
    return request.args()


async def valid_session_args(request: SignableRequest) -> ValidSessionArgs:
    return request.valid_args()
