from ferdelance.core.artifacts import Artifact
from ferdelance.core.distributions import Collect, Distribute
from ferdelance.core.estimators.counters import CountEstimator
from ferdelance.core.models import FederatedRandomForestClassifier
from ferdelance.core.operations import SubtractMatrix, SumMatrix, UniformMatrix
from ferdelance.core.steps import Finalize, Initialize, Parallel, SchedulerContext
from ferdelance.schemas.components import Component

import json


def get_scheduler_context(n_workers: int = 2) -> SchedulerContext:
    s = Component(id="S", type_name="NODE", public_key="")

    workers = [Component(id=f"W{w}", type_name="NODE", public_key="") for w in range(n_workers)]

    return SchedulerContext(
        artifact_id="artifact",
        initiator=s,
        workers=workers,
    )


def test_simple_artifact():
    artifact = Artifact(
        id="artifact_id",
        project_id="project_id",
        steps=[
            Initialize(
                UniformMatrix(size=(2, 2), persist=True),
                Distribute(),
            ),
            Parallel(
                SumMatrix(),
                Collect(),
            ),
            Finalize(
                SubtractMatrix(),
            ),
        ],
    )

    artifact_str = artifact.json()
    artifact_json = json.loads(artifact_str)

    assert "steps" in artifact_json

    rebuilt = Artifact(**artifact_json)

    assert rebuilt == artifact
    assert len(rebuilt.steps) == 3

    sc = get_scheduler_context()
    jobs = rebuilt.jobs(sc)

    assert len(jobs) == 4

    assert jobs[0].worker == sc.initiator
    assert jobs[0].locks == [1, 2]

    assert jobs[1].worker in sc.workers
    assert jobs[1].locks == [3]

    assert jobs[2].worker in sc.workers
    assert jobs[2].locks == [3]

    assert jobs[3].worker == sc.initiator
    assert jobs[3].locks == []


def test_aggregate_model_rf_artifact():
    sc = get_scheduler_context()

    model = FederatedRandomForestClassifier()

    artifact = Artifact(
        id="artifact_id",
        project_id="project_id",
        steps=model.get_steps(),
    )

    artifact_str = artifact.json()
    artifact_json = json.loads(artifact_str)

    assert "steps" in artifact_json

    rebuilt = Artifact(**artifact_json)

    assert rebuilt == artifact
    assert len(rebuilt.steps) == 2

    sc = get_scheduler_context()
    jobs = rebuilt.jobs(sc)

    assert len(jobs) == 3

    assert jobs[0].worker in sc.workers
    assert jobs[0].locks == [2]

    assert jobs[1].worker in sc.workers
    assert jobs[1].locks == [2]

    assert jobs[2].worker == sc.initiator
    assert jobs[2].locks == []


def test_plan_sequence():
    count = CountEstimator()

    artifact = Artifact(
        id="artifact_id",
        project_id="project_id",
        steps=count.get_steps(),
    )

    artifact_str = artifact.json()
    artifact_json = json.loads(artifact_str)

    assert "steps" in artifact_json

    rebuilt = Artifact(**artifact_json)

    assert rebuilt == artifact
    assert len(rebuilt.steps) == 1

    sc = get_scheduler_context()
    jobs = rebuilt.jobs(sc)

    assert len(jobs) == 4

    assert jobs[0].worker == sc.initiator
    assert jobs[0].locks == [1]

    assert jobs[1].worker in sc.workers
    assert jobs[1].locks == [2]

    assert jobs[2].worker in sc.workers
    assert jobs[2].locks == [3]

    assert jobs[3].worker == sc.initiator
    assert jobs[3].locks == []
