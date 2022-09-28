import importlib
import sys
import logging
from celery import Task

from .celery import worker
from ferdelance.database.tables import ClientTaskEvent
from ferdelance.database import SessionLocal


class PredictTask(Task):
    """
    Abstraction of Celery's Task class to support loading ML model.
    """

    abstract = True

    def __init__(self):
        super().__init__()
        self.model = None

    def __call__(self, *args, **kwargs):
        """
        Load model on first call (i.e. first task processed)
        Avoids the need to load model on each task request
        """
        if not self.model:
            logging.info("Loading Model...")
            sys.path.append("..")
            module_import = importlib.import_module(self.path[0])
            model_obj = getattr(module_import, self.path[1])
            self.model = model_obj()
            logging.info("Model loaded")
        return self.run(*args, **kwargs)



@worker.task(
    ignore_result=False,
    bind=True,
    base=PredictTask,
    path=("ferdelance.logic.fake", "FakeModel"),
    name="{}.{}".format(__name__, "Fake"),
)
def predict(self, artifact_id: str, x: float):

    # celery_task_id: str = str(self.request.id)

    # with SessionLocal() as session:
    #     logging.info(f"Task {celery_task_id} STARTED")
    #     session.add(ClientTaskEvent(
    #         status = "START",
    #         celery_task_id = celery_task_id,
    #         client_id = "SERVER",
    #         artifact_id = artifact_id,
    #     ))
    #     session.commit()

    prediction = self.model.predict(x)

    # with SessionLocal() as session:
    #     logging.info(f"Task {celery_task_id} ENDED")
    #     session.add(ClientTaskEvent(
    #     status = "END",
    #     celery_task_id = celery_task_id,
    #     client_id = "SERVER",
    #     artifact_id = artifact_id,
    # ))
    #     session.commit()

    return prediction
