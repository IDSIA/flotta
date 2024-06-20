from flotta.core.environment import EnvResource, Environment
from flotta.core.estimators import GroupCountEstimator, GroupMeanEstimator
from flotta.core.steps import Sequential

from pathlib import Path

import pandas as pd
import os


PATH_DIR = Path(os.path.abspath(os.path.dirname(__file__)))
PATH_CALIFORNIA = PATH_DIR / ".." / "data" / "california.csv"


def test_group_count_estimator():
    data = pd.read_csv(PATH_CALIFORNIA)

    dfs = [
        data.iloc[:7000, :],
        data.iloc[7000:14000, :],
        data.iloc[14000:, :],
    ]

    gce = GroupCountEstimator(by=["HouseAge"], features=["AveBedrms"], random_state=42)

    steps = gce.get_steps()

    assert len(steps) == 1

    seq = steps[0]

    assert isinstance(seq, Sequential)

    op_init = seq.init_operation
    op_work = seq.operation
    op_final = seq.final_operation

    env = Environment("", "", "", Path("."))

    env = op_init.exec(env)

    assert env["noise"] != 0

    for df in dfs:
        env.df = df
        env.resources = {"1": EnvResource("1", data=env.products)}

        env = op_work.exec(env)

        assert env["noise"] == 0

    env.resources = {"1": EnvResource("1", data=env.products)}

    env = op_final.exec(env)

    # checking results
    fed_counts = env["counts"]["AveBedrms"]

    df_counts = data.groupby("HouseAge").count()[["AveBedrms"]].to_dict()["AveBedrms"]

    assert len(fed_counts) == len(df_counts)
    assert sum(fed_counts.values()) == sum(df_counts.values())

    for k in df_counts.keys():
        assert k in fed_counts
        assert fed_counts[k] == df_counts[k]


def test_group_mean_estimator():
    data = pd.read_csv(PATH_CALIFORNIA)

    print(data.columns)

    dfs = [
        data.iloc[:7000, :],
        data.iloc[7000:14000, :],
        data.iloc[14000:, :],
    ]

    gce = GroupMeanEstimator(by=["HouseAge"], features=["AveBedrms"], random_state=42)

    steps = gce.get_steps()

    assert len(steps) == 1

    seq = steps[0]

    assert isinstance(seq, Sequential)

    op_init = seq.init_operation
    op_work = seq.operation
    op_final = seq.final_operation

    env = Environment("", "", "", Path("."))

    env = op_init.exec(env)

    assert env["noise_sum"] != 0

    for df in dfs:
        env.df = df
        env.resources = {"1": EnvResource("1", data=env.products)}

        env = op_work.exec(env)

        assert env["noise_num"] == 0

    env.resources = {"1": EnvResource("1", data=env.products)}

    env = op_final.exec(env)

    # checking results
    fed_means = env["means"]["AveBedrms"]

    df_means = data.groupby("HouseAge").mean()[["AveBedrms"]].to_dict()["AveBedrms"]

    assert len(fed_means) == len(df_means)

    for k in df_means.keys():
        assert k in fed_means
        assert abs(fed_means[k] - df_means[k]) < 1e6

    assert abs(sum(fed_means.values()) - sum(df_means.values())) < 1e6
