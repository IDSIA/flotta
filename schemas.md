# ferdelance.database.schemas <---------------- to move under schemas

BaseComponent
    Component(BaseComponent)
    Client(BaseComponent)
Token
Job
Artifact <------------------------------------- to remove in favor of ferdelance.schemas.artifacts.Artifact
Model <---------------------------------------- to remove in favor of ferdelance.schemas.models.core.Model
DataSource <----------------------------------- to remove in favor of ferdelance.schemas.project.DataSource
Project <-------------------------------------- to remove in favor of ferdelance.schemas.project.Project
Event

# ferdelance.schemas.client

                                               remove unused
ClientJoinRequest
ClientJoinData
ClientDetails
ClientUpdate
    ClientUpdateTaskCompleted(ClientUpdate)
DataSourceConfig
ArgumentsConfig

# ferdelance.schemas.project

                                               consider split in multiple files
ProjectBase
    Project(ProjectBase)
    ProjectCreate(ProjectBase) <--------------- consider if remove
DataSourceBase
    DataSource(DataSourceBase)
    AggregatedDataSource(DataSourceBase)
    DataSourceCreate(DataSourceBase) <--------- consider if remove
FeatureBase
    Feature(FeatureBase)
    AggregatedFeature(FeatureBase)
    FeatureCreate(FeatureBase) <--------------- consider if remove

# ferdelance.schemas.updates

UpdateData
    UpdateToken(UpdateData) <------------------ consider if remove from hierarchy
    UpdateClientApp(UpdateData) <-------------- consider if remove from hierarchy
    UpdateExecute(UpdateData) <---------------- consider if remove from hierarchy
    UpdateNothing(UpdateData) <---------------- consider if remove from hierarchy
DownloadApp

# ferdelance.schemas.workbench
                                               
                                               remove unused
WorkbenchJoinRequest
WorkbenchJoinData
WorkbenchProjectToken
WorkbenchClientList
WorkbenchDataSourceIdList
WorkbenchFeature
WorkbenchDataSource
WorkbenchProject
WorkbenchProjectDescription
AggregatedDataSource

# ferdelance.schemas.artifacts.artifacts

BaseArtifact
    Artifact(BaseArtifact)
    ArtifactStatus(BaseArtifact)

# ferdelance.schemas.artifacts.datasets

Dataset <-------------------------------------- remove in favor of ferdelance.schemas.project.AggregatedDataSource

# ferdelance.schemas.artifacts.datasources

BaseDataSource <------------------------------- remove in favor of ferdelance.schemas.project.DataSource (move code)
    DataSource(BaseDataSource) <--------------- remove in favor of ferdelance.schemas.project.DataSource (move code)
    MetaDataSource(BaseDataSource) <----------- move with ferdelance.schemas.project.DataSource (move code)
Metadata <------------------------------------- move with ferdelance.schemas.project (move code)

# ferdelance.schemas.artifacts.queries

QueryFeature <--------------------------------- consider new in ferdelance.schemas.project.Feature 
QueryFilter
QueryTransformer
BaseFeature <---------------------------------- remove in favor of ferdelance.schemas.project.Feature (move code)
    Feature(BaseFeature) <--------------------- remove in favor of ferdelance.schemas.project.Feature (move code)
    MetaFeature(BaseFeature) <----------------- remove in favor of ferdelance.schemas.project.Feature (move code)
Query
