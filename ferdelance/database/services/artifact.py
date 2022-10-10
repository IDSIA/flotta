from .core import DBSessionService, Session

from ..tables import Artifact, Model

from ferdelance_shared.status import ArtifactJobStatus


class ArtifactService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def create_artifact(self, artifact_id: str, path: str, status: str) -> Artifact:
        db_artifact = Artifact(artifact_id=artifact_id, path=path, status=status)

        existing = self.db.query(Artifact).filter(Artifact.artifact_id == artifact_id).first()

        if existing is not None:
            raise ValueError('artifact already exists!')

        self.db.add(db_artifact)
        self.db.commit()
        self.db.refresh(db_artifact)

        return db_artifact

    def get_artifact(self, artifact_id: str) -> Artifact | None:
        return self.db.query(Artifact).filter(Artifact.artifact_id == artifact_id).first()

    def get_models_by_artifact_id(self, artifact_id: str) -> list[Model]:
        return self.db.query(Model).filter(Model.artifact_id == artifact_id, Model.aggregated).all()

    def update_status(self, artifact_id: str, new_status: ArtifactJobStatus) -> None:
        self.db.query(Artifact).filter(Artifact.artifact_id == artifact_id).update({
            'status': new_status.name,
        })
