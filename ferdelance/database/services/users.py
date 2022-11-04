from .core import DBSessionService, AsyncSession

from ..tables import User, UserToken

from sqlalchemy import select

import logging

LOGGER = logging.getLogger(__name__)


class UserService(DBSessionService):

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session)

    async def create_user(self, user: User) -> User:
        LOGGER.info(f'user_id={user.user_id}: creating new user')

        res = await self.session.execute(
            select(User.user_id)
            .where(User.public_key == user.public_key)
            .limit(1)
        )
        existing_user_id = res.scalar_one_or_none()

        if existing_user_id is not None:
            LOGGER.warning(f'user_id={existing_user_id}: user already exists')
            raise ValueError('User already exists')

        self.session.add(user)
        await self.session.commit()
        await self.session.refresh(user)

        return user

    async def get_user_by_id(self, user_id: str) -> User | None:
        res = await self.session.execute(select(User).where(User.user_id == user_id))
        return res.scalar_one_or_none()

    async def get_user_by_key(self, user_public_key: str) -> User | None:
        res = await self.session.execute(select(User).where(User.public_key == user_public_key))
        return res.scalar_one_or_none()

    async def get_user_list(self) -> list[User]:
        res = await self.session.scalars(select(User))
        return res.all()

    async def get_user_by_token(self, token: str) -> User:
        res = await self.session.execute(
            select(User)
            .join(UserToken, User.user_id == UserToken.token_id)
            .where(UserToken.token == token)
        )
        return res.scalar_one()

    async def create_user_token(self, token: UserToken) -> UserToken:
        LOGGER.info(f'user_id={token.user_id}: creating new token')

        res = await self.session.execute(select(UserToken).where(UserToken.token == token.token))

        existing_user_token: UserToken | None = res.scalar_one_or_none()

        if existing_user_token is not None:
            LOGGER.warning(f'user_id={existing_user_token.user_id}: a valid token already exists')
            # TODO: check if we have more strong condition for this
            return existing_user_token

        self.session.add(token)
        await self.session.commit()
        await self.session.refresh(token)

        return token

    async def invalidate_all_tokens(self, user_id: str) -> None:
        res = await self.session.scalars(select(UserToken).where(UserToken.user_id == user_id))
        tokens: list[UserToken] = res .all()

        for token in tokens:
            token.valid = False

        await self.session.commit()

    async def get_user_id_by_token(self, token: str) -> str | None:
        res = await self.session.scalars(select(UserToken).where(UserToken.token == token))
        user_token: UserToken | None = res.one_or_none()

        if user_token is None:
            return None

        return str(user_token.user_id)

    async def get_user_token_by_token(self, token: str) -> UserToken | None:
        res = await self.session.execute(select(UserToken).where(UserToken.token == token))
        return res.scalar_one_or_none()

    async def get_user_token_by_user_id(self, user_id: str) -> UserToken | None:
        res = await self.session.execute(select(UserToken).where(UserToken.user_id == user_id, UserToken.valid == True))
        return res.scalar_one_or_none()
