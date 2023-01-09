from pathlib import Path
from ferdelance.shared.artifacts import MetaDataSource, MetaFeature

from .datasource import DataSource

import pandas as pd


class DataSourceFile(DataSource):
    def __init__(self, name: str, kind: str, path: str) -> None:
        super().__init__(name, kind)
        self.path: Path = Path(path)

    def get(self) -> pd.DataFrame:
        extension = self.path.suffix  # CSV, TSV, XLSX, ...

        if extension == ".csv":
            return pd.read_csv(self.path)
        elif extension == ".tsv":
            return pd.read_csv(self.path, sep="\t")

        raise NotImplemented(f"Don't know how to load {extension} format")

    def metadata(self) -> MetaDataSource:
        sep = '\t' if self.kind == 'tsv' else ','

        df = pd.read_csv(self.path, sep=sep)
        df_desc = df.describe()

        n_records, n_features = df.shape

        features: list[MetaFeature] = []
        for feature in df.columns:
            dtype = str(df[feature].dtype)

            if feature in df_desc:
                f = MetaFeature(
                    name=str(feature),
                    dtype=dtype,
                    v_mean=df_desc[feature]['mean'],
                    v_std=df_desc[feature]['std'],
                    v_min=df_desc[feature]['min'],
                    v_p25=df_desc[feature]['25%'],
                    v_p50=df_desc[feature]['50%'],
                    v_p75=df_desc[feature]['75%'],
                    v_max=df_desc[feature]['max'],
                    v_miss=df[feature].isna().sum(),
                )
            else:
                f = MetaFeature(
                    name=str(feature),
                    dtype=dtype,
                    v_mean=None,
                    v_std=None,
                    v_min=None,
                    v_p25=None,
                    v_p50=None,
                    v_p75=None,
                    v_max=None,
                    v_miss=None,
                )
            features.append(f)

        return MetaDataSource(
            name=self.name,
            removed=False,
            n_records=n_records,
            n_features=n_features,
            features=features,
        )
