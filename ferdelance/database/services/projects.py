
    async def get_by_name(self, name: str) -> Project:
        res = await self.session.execute(select(ProjectDB).where(ProjectDB.name == name))
        return view(res.scalar_one())

    async def update_token(self, project: Project, new_token: str) -> Project:
        res = await self.session.scalars(select(ProjectDB).filter(ProjectDB.project_id == project.project_id))
        p: ProjectDB = res.one()

        p.token = new_token
        self.session.add(new_token)
        await self.session.commit()
        await self.session.refresh(p)

        return view(p)
