from flotta.datasources.datasource import DataSource
from flotta.schemas.metadata import MetaDataSource, MetaFeature

from pathlib import Path

import pandas as pd


class DataSourceFile(DataSource):
    def __init__(
        self,
        name: str,
        type: str,
        path: Path | str,
        tokens: list[str] = list(),
        encoding: str = "utf8",
    ) -> None:
        super().__init__(name, type, str(path), tokens, encoding)

        if isinstance(path, str):
            path = Path(path)
        self.path: Path = path

    def get(self) -> pd.DataFrame:
        extension = self.path.suffix  # CSV, TSV, XLSX, ...

        if extension == ".csv":
            return pd.read_csv(self.path)
        elif extension == ".tsv":
            return pd.read_csv(self.path, sep="\t")

        raise ValueError(f"Don't know how to load {extension} format")

    def dump(self) -> dict[str, str]:
        return super().dump() | {
            "conn": str(self.path),
        }

    def metadata(self) -> MetaDataSource:
        sep = "\t" if self.type == "tsv" else ","

        df = pd.read_csv(self.path, sep=sep)
        df_desc = df.describe()

        n_records, n_features = df.shape

        features: list[MetaFeature] = []
        for feature in df.columns:
            dtype = str(df[feature].dtype)

            if feature in df_desc:
                f = MetaFeature(
                    datasource_hash=self.hash,
                    name=str(feature),
                    dtype=dtype,
                    v_mean=df_desc[feature]["mean"],
                    v_std=df_desc[feature]["std"],
                    v_min=df_desc[feature]["min"],
                    v_p25=df_desc[feature]["25%"],
                    v_p50=df_desc[feature]["50%"],
                    v_p75=df_desc[feature]["75%"],
                    v_max=df_desc[feature]["max"],
                    v_miss=df[feature].isna().sum(),
                    n_cats=0,  # TODO
                )
            else:
                f = MetaFeature(
                    datasource_hash=self.hash,
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
                    n_cats=0,  # TODO
                )
            features.append(f)

        return MetaDataSource(
            id=None,
            hash=self.hash,
            name=self.name,
            removed=False,
            n_records=n_records,
            n_features=n_features,
            features=features,
            tokens=self.tokens,
        )
