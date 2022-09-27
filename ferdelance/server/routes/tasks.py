from fastapi import APIRouter, Depends, Request, HTTPException, UploadFile
from fastapi.responses import JSONResponse
from celery.result import AsyncResult

import logging
import json

from ferdelance.logic.fake.models import ModelInput, ModelPrediction, TaskTicket
from ferdelance.worker.tasks import predict

LOGGER = logging.getLogger(__name__)


tasks_router = APIRouter()

@tasks_router.post('/tasks/fakemodel/predict', response_model=TaskTicket, status_code=202)
async def schedule_prediction(model_input: ModelInput):
    """Create celery prediction task. Return task_id to client in order to retrieve result"""
    task_id = predict.delay(**dict(model_input))
    return {'task_id': str(task_id), 'status': 'Processing'}


@tasks_router.get('/tasks/fakemodel/result/{task_id}', response_model=ModelPrediction, status_code=200,
         responses={202: {'model': TaskTicket, 'description': 'Accepted: Not Ready'}})
async def get_prediction_result(task_id):
    """Fetch result for given task_id"""
    task = AsyncResult(task_id)
    if not task.ready():
        print(app.url_path_for('schedule_prediction'))
        return JSONResponse(status_code=202, content={'task_id': str(task_id), 'status': 'Processing'})
    result = task.get()
    return {'task_id': task_id, 'status': 'Success', 'result': str(result)}