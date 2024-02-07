from ferdelance.core.artifacts import Artifact
from ferdelance.core.environment import EnvResource, Environment
from ferdelance.core.estimators import CountEstimator

from pathlib import Path

import pandas as pd
import os

from ferdelance.core.steps import Sequential
from tests.utils import get_scheduler_context

PATH_DIR = Path(os.path.abspath(os.path.dirname(__file__)))
PATH_CALIFORNIA = PATH_DIR / ".." / "data" / "california.csv"


def test_count_estimator():
    data = pd.read_csv(PATH_CALIFORNIA)

    dfs = [
        data.iloc[:7000, :],
        data.iloc[7000:14000, :],
        data.iloc[14000:, :],
    ]

    ce = CountEstimator()

    steps = ce.get_steps()
    assert len(steps) == 1

    seq = steps[0]

    assert isinstance(seq, Sequential)

    op_init = seq.init_operation
    op_work = seq.operation
    op_final = seq.final_operation

    env = Environment("", "", "", Path("."))

    env = op_init.exec(env)

    assert env[".init_value"] != 0
    assert env[".init_value"] == env["count"]

    for df in dfs:
        env.df = df
        env.resources = {"1": EnvResource("1", data=env.products)}

        env = op_work.exec(env)

    env.resources = {"1": EnvResource("1", data=env.products)}

    env = op_final.exec(env)

    # checking results
    fed_counts = env["count"]

    df_counts = data.shape[0]

    assert fed_counts == df_counts


def test_count_estimator_in_simulated_env():
    # data setup
    data = pd.read_csv(PATH_CALIFORNIA)

    dfs = [
        None,
        data.iloc[:7000, :],
        data.iloc[7000:14000, :],
        data.iloc[14000:, :],
        None,
    ]

    # estimator setup
    ce = CountEstimator()

    # artifact setup
    artifact = Artifact(
        id="artifact_id",
        project_id="project_id",
        steps=ce.get_steps(),
    )

    # context setup
    sc = get_scheduler_context(3)
    jobs = artifact.jobs(sc)

    assert len(jobs) == 5

    print()

    # work environment setup
    env = Environment("", "", "", Path("."))

    for i, (job, df) in enumerate(zip(jobs, dfs)):
        if i > 0:
            env.df = df
            env.resources = {"1": EnvResource("1", data=env.products)}

        job.step.step(env)

    # checking results
    fed_counts = env["count"]

    df_counts = data.shape[0]

    assert fed_counts == df_counts
