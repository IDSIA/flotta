import os

result_expires = 3600

# Ignore other content
accept_content = ["json"]
task_serializer = "json"
result_serializer = "json"

result_backend = os.getenv("CELERY_BACKEND_URL", None)
broker_url = os.getenv("CELERY_BROKER_URL", None)

timezone = "Europe/Zurich"
enable_utc = True

imports = [
    "ferdelance.config",
    "ferdelance.schemas.artifacts",
    "ferdelance.schemas.estimators",
    "ferdelance.schemas.models",
    "ferdelance.worker.tasks.aggregation",
]
