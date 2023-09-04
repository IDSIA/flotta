from typing import Callable, Coroutine, Any

from ferdelance.database import DataBase, AsyncSession
from ferdelance.database.repositories import ComponentRepository
from ferdelance.logging import get_logger
from ferdelance.node.services import SecurityService
from ferdelance.schemas.components import Component

from fastapi import HTTPException, Request, Response
from fastapi.routing import APIRoute

from starlette.types import Receive, Scope, Send


LOGGER = get_logger(__name__)


class SessionArgs:
    def __init__(
        self,
        session: AsyncSession,
        security_service: SecurityService,
        checksum: str,
        component: Component,
        self_component: Component,
        ip_address: str,
    ) -> None:
        self.session: AsyncSession = session
        self.security_service: SecurityService = security_service

        self.checksum: str = checksum

        self.component: Component = component
        self.self_component: Component = self_component

        self.ip_address: str = ip_address


class EncodedRequest(Request):
    def __init__(self, db_session: AsyncSession, scope: Scope, receive: Receive = ..., send: Send = ...):
        super().__init__(scope, receive, send)

        self.db_session: AsyncSession = db_session
        self.security: SecurityService = SecurityService(db_session)

        self.content_encrypted: bool = "encrypted" in self.headers.get("Content-Encoding", "").split("/")
        self.accept_encrypted: bool = "encrypted" in self.headers.get("Accept-Encoding", "").split("/")
        self.checksum: str

        self.component: Component
        self.self_component: Component

    def args(self) -> SessionArgs:
        return SessionArgs(
            self.db_session,
            self.security,
            self.checksum,
            self.component,
            self.self_component,
            self.client.host if self.client else "",
        )

    async def body(self) -> bytes:
        if not hasattr(self, "_body"):
            body: bytes = await super().body()

            cr: ComponentRepository = ComponentRepository(self.db_session)
            self.self_component = await cr.get_self_component()

            try:
                if self.headers.get("Authentication", ""):
                    LOGGER.debug("checking authentication header")

                    # verify header
                    component_id, given_checksum = self.security.verify_headers(self)

                    if self.content_encrypted:
                        # decrypt body
                        LOGGER.debug(f"component_id={component_id}: Received encrypted data")

                        self.checksum, payload = self.security.exc.get_payload(body)

                        if given_checksum != self.checksum:
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


class EncodedRoute(APIRoute):
    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: Request) -> Response:
            db = DataBase()
            async with db.session() as db_session:
                request = EncodedRequest(db_session, request.scope, request.receive)

                await request.security.setup()

                response = await original_route_handler(request)

                if request.accept_encrypted:
                    checksum, payload = request.security.exc.create_payload(response.body)

                    response.headers["Content-Length"] = f"{len(payload)}"
                    response.body = payload
                else:
                    checksum = ""  # TODO: maybe set this to something...

                headers = request.security.exc.sign_header(
                    request.self_component.id,
                    checksum,
                    request.accept_encrypted,
                )
                for k, v in headers.items():
                    response.headers[k] = v

                return response

        return custom_route_handler


async def session_args(request: EncodedRequest) -> SessionArgs:
    return request.args()
