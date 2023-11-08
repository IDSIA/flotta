from ferdelance.core.environment import Environment
from ferdelance.core.transformers import QueryTransformer

import pandas as pd


def run(df: pd.DataFrame, t: QueryTransformer) -> pd.DataFrame | None:
    env = Environment()
    env.X_tr = df

    env, _ = t.transform(env)
    return env.X_tr
