from ferdelance.workbench import (
    Context,
    Project,
    Artifact,
)
from ferdelance.schemas.models import (
    FederatedRandomForestClassifier,
    ParametersRandomForestClassifier,
    StrategyRandomForestClassifier,
)
from ferdelance.schemas.transformers import FederatedKBinsDiscretizer
from ferdelance.schemas.plans import TrainTestSplit


ctx = Context("http://localhost:1456")

project_token = "58981bcbab77ef4b8e01207134c38873e0936a9ab88cd76b243a2e2c85390b94"

project: Project = ctx.load(project_token)

q = project.data.extract()
q.add(q["variety"] < 2)
q.add(FederatedKBinsDiscretizer(q["variety"], "variety_discr"))

m = FederatedRandomForestClassifier(
    strategy=StrategyRandomForestClassifier.MERGE,
    parameters=ParametersRandomForestClassifier(n_estimators=10),
)

a: Artifact = Artifact(
    project_id=project.project_id,
    model=m.build(),
    transform=q,
    load=TrainTestSplit(
        label="variety",
        test_percentage=0.5,
    ).build(),
)

a = ctx.submit(a)
