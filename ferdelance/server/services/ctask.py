from . import DBSessionService, Session

from ...database.tables import Artifact, ClientTask, Client, Task

from uuid import uuid4


class ClientTaskService(DBSessionService):

    def __init__(self, db: Session) -> None:
        super().__init__(db)

    def create_task(self, artifact: Artifact) -> Task:
        task_id: str = str(uuid4())
        task = Task(task_id=task_id, status='CREATED', artifact_id=artifact.artifact_id)

        self.db.add(task)
        self.db.commit()
        self.db.refresh(task)

        return task

    def create_client_task(self, artifact_id: str, client_id: str, task_id: str) -> ClientTask:
        client_task_id: str = str(uuid4())

        client_task = ClientTask(
            client_task_id=client_task_id,
            status='SCHEDULED',
            task_id=task_id,
            client_id=client_id,
            artifact_id=artifact_id,
        )

        self.db.add(client_task)
        self.db.commit()
        self.db.refresh(client_task)

        return client_task

    def get_task_for_client(self, client_task_id: str) -> ClientTask | None:
        return self.db.query(ClientTask).filter(ClientTask.client_task_id == client_task_id).first()

    def get_next_task_for_client(self, client: Client) -> ClientTask | None:
        return self.db.query(ClientTask).filter(ClientTask.status == 'SCHEDULED', ClientTask.client_id == client.client_id).order_by(ClientTask.creation_time.asc()).first()
