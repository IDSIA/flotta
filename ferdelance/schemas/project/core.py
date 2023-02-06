from __future__ import annotations

from typing import List

from .project import ProjectBase
from .datasource import DataSourceBase
from .feature import FeatureBase


class Feature(FeatureBase):

    datasource: DataSource


class DataSource(DataSourceBase):
    projects: List[Project] = []
    features: List[Feature] = []


class Project(ProjectBase):
    valid: bool
    active: bool
    datasources: List[DataSource] = []
