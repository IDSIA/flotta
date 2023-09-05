from typing import Callable, Coroutine, Any
from dataclasses import dataclass

from ferdelance.database import DataBase, AsyncSession
from ferdelance.database.repositories import ComponentRepository
from ferdelance.logging import get_logger
from ferdelance.node.services import SecurityService
from ferdelance.schemas.components import Component

from fastapi import HTTPException, Request, Response
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


class EncodedRequest(Request):
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

            cr: ComponentRepository = ComponentRepository(self.db_session)

            try:
                if self.headers.get("Authentication", ""):
                    LOGGER.debug("checking authentication header")

                    # verify header
                    component_id, self.given_checksum = self.security.verify_headers(self)

                    if self.content_encrypted:
                        # decrypt body
                        LOGGER.debug(f"component_id={component_id}: Received encrypted data")

                        self.checksum, payload = self.security.exc.get_payload(body)

                        if self.given_checksum != self.checksum:
                            LOGGER.warning(f"component_id={component_id}: Checksum failed")
                            raise HTTPException(403)

                        body = payload

                        # get component
                        self.component = await cr.get_by_id(component_id)
                        await self.security.setup(self.component.public_key)

                else:
                    LOGGER.debug("not authenticated header")

                    if self.content_encrypted:
                        # decrypt body
                        LOGGER.debug("component_id=UNKNOWN: Received encrypted data")

                        self.checksum, payload = self.security.exc.get_payload(body)

                        body = payload

            except Exception as e:
                LOGGER.warning(f"Secure checks failed: {e}")
                LOGGER.exception(e)
                raise HTTPException(403)

            self._body = body
        return self._body


class EncodedAPIRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            async with DataBase().session() as db_session:
                cr: ComponentRepository = ComponentRepository(db_session)
                self_component = await cr.get_self_component()

                request = EncodedRequest(db_session, self_component, request.scope, request.receive)
                await request.security.setup()

                response = await original_route_handler(request)

                args = request.args()

                if args.accept_encrypted:
                    checksum, payload = args.security_service.exc.create_payload(response.body)

                    response.headers["Content-Length"] = f"{len(payload)}"
                    response.body = payload

                    headers = args.security_service.exc.create_signed_header(
                        args.self_component.id,
                        checksum,
                        args.accept_encrypted,
                    )
                else:
                    checksum = ""  # TODO: maybe set this to something...
                    headers = args.security_service.exc.create_header(False)

                for k, v in headers.items():
                    response.headers[k] = v

                return response

        return custom_route_handler


async def session_args(request: EncodedRequest) -> SessionArgs:
    return request.args()


async def valid_session_args(request: EncodedRequest) -> ValidSessionArgs:
    return request.valid_args()
