from ferdelance.core.artifacts import Artifact
from ferdelance.core.environment import EnvResource, Environment
from ferdelance.core.estimators import MeanEstimator

from pathlib import Path

import pandas as pd
import os

from ferdelance.core.steps import Sequential
from tests.utils import get_scheduler_context

PATH_DIR = Path(os.path.abspath(os.path.dirname(__file__)))
PATH_CALIFORNIA = PATH_DIR / ".." / "data" / "california.csv"


def test_mean_estimator():
    data = pd.read_csv(PATH_CALIFORNIA)

    dfs = [
        data.iloc[:7000, :],
        data.iloc[7000:14000, :],
        data.iloc[14000:, :],
    ]

    ce = MeanEstimator()

    steps = ce.get_steps()
    assert len(steps) == 1

    seq = steps[0]

    assert isinstance(seq, Sequential)

    op_init = seq.init_operation
    op_work = seq.operation
    op_final = seq.final_operation

    env = Environment("", "", "", Path("."))

    env = op_init.exec(env)

    assert env[".init_sum"] != 0
    assert env["sum"] == env[".init_sum"]

    assert env[".init_count"] != 0
    assert env["count"] == env[".init_count"]

    for df in dfs:
        env.df = df
        env.resources = {"1": EnvResource("1", data=env.products)}

        env = op_work.exec(env)

    env.resources = {"1": EnvResource("1", data=env.products)}

    env = op_final.exec(env)

    # checking results
    fed_means = env["mean"]

    df_means = data.mean()

    assert isinstance(fed_means, pd.Series)
    assert fed_means.shape == df_means.shape

    fed_means_dict = fed_means.to_dict()
    df_means_dict = df_means.to_dict()

    for k in df_means_dict.keys():
        assert k in fed_means_dict
        assert abs(fed_means_dict[k] - df_means_dict[k]) < 1e6

    assert abs(fed_means.sum() - df_means.sum()) < 1e6


def test_mean_estimator_in_simulated_env():
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
    ce = MeanEstimator()

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
    fed_means = env["mean"]

    df_means = data.mean()

    assert isinstance(fed_means, pd.Series)
    assert fed_means.shape == df_means.shape

    fed_means_dict = fed_means.to_dict()
    df_means_dict = df_means.to_dict()

    for k in df_means_dict.keys():
        assert k in fed_means_dict
        assert abs(fed_means_dict[k] - df_means_dict[k]) < 1e6

    assert abs(fed_means.sum() - df_means.sum()) < 1e6
