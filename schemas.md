# ferdelance.schemas.database

ServerArtifact                                  (internal representation)
ServerModel                                     (internal representation)

# ferdelance.schemas.components

BaseComponent
    Component(BaseComponent)
    Client(BaseComponent) <-------------------- consider if remove
Token
Event

# ferdelance.schemas.jobs

Job

# ferdelance.schemas.client

                                                remove unused
ClientJoinRequest
ClientJoinData
ClientDetails
ClientUpdate
    ClientUpdateTaskCompleted(ClientUpdate)
DataSourceConfig
ArgumentsConfig

# ferdelance.schemas.projects.datasources

BaseDataSource
DataSource(BaseDataSource)
Feature

# ferdelance.schemas.projects.metadata

Metadata
MetaDataSource(BaseDataSource)
MetaFeature(Feature)

# ferdelance.schemas.projects.projects

BaseProject
Project(BaseProject)
AggregatedDataSource(BaseDataSource)
AggregatedFeature(Feature)

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

# ferdelance.schemas.artifacts.queries

QueryFeature
QueryFilter
QueryTransformer
Query
