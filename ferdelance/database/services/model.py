from .core import DBSessionService, Session
from ..tables import Model
from ...config import STORAGE_MODELS

from uuid import uuid4

import os


class ModelService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def storage_dir(self, artifact_id) -> str:
        out_dir = os.path.join(STORAGE_MODELS, artifact_id)
        os.makedirs(out_dir, exist_ok=True)
        return out_dir

    def create_model(self, artifact_id: str, client_id: str, aggregated: bool = False) -> Model:
        model_id: str = str(uuid4())

        filename = f'{model_id}.AGGREGATED.model' if aggregated else f'{model_id}.model'
        out_path = os.path.join(self.storage_dir(artifact_id), filename)

        model_db = Model(
            model_id=model_id,
            path=out_path,
            artifact_id=artifact_id,
            client_id=client_id,
            aggregated=aggregated,
        )

        self.db.add(model_db)
        self.db.commit()
        self.db.refresh(model_db)

        return model_db

    def get_model_by_id(self, model_id: str) -> Model | None:
        return self.db.query(Model).filter(Model.model_id == model_id).first()

    def get_models_by_artifact_id(self, artifact_id: str) -> list[Model]:
        return self.db.query(Model).filter(Model.artifact_id == artifact_id).all()

    def get_models_by_job_id(self, job_id: str) -> list[Model]:
        return self.db.query(Model).filter(Model.job_id == job_id).all()
