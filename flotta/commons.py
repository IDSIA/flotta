from pathlib import Path

import os


def storage_job(artifact_id: str, job_id: str, iteration: int = 0, work_directory: Path | None = None) -> Path:
    d = Path(".") / artifact_id / f"{iteration}" / job_id

    if work_directory is not None:
        d = work_directory / d

    os.makedirs(d, exist_ok=True)
    return d
