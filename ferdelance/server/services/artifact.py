from . import DBSessionService, Session

from ...database.tables import Artifact, Model


class ArtifactService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def create_artifact(self, artifact_id: str, path: str) -> Artifact:
        db_artifact = Artifact(artifact_id=artifact_id, path=path)

        existing = self.db.query(Artifact).filter(Artifact.artifact_id == artifact_id).first()

        if existing is not None:
            raise ValueError('artifact already exists!')

        self.db.add(db_artifact)
        self.db.commit()
        self.db.refresh(db_artifact)

        return db_artifact

    def get_artifact(self, artifact_id: str) -> Artifact:
        return self.db.query(Artifact).filter(Artifact.artifact_id == artifact_id).first()

    def get_model_by_artifact(self, artifact: Artifact) -> list[Model]:
        return self.db.query(Model).filter(Model.artifact_id == artifact.artifact_id).first()