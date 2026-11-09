"""TEST STUB ONLY - stands in for cloudera-airflow-provider (pre-installed on
Workflow Orchestrator, not on PyPI) so CI can parse the CDE labs."""
from airflow.sdk import BaseOperator


class CdeRunJobOperator(BaseOperator):
    def __init__(self, *, job_name, connection_id="cde_runtime_api", **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.connection_id = connection_id

    def execute(self, context):
        self.log.info("[stub] would run CDE job %s via %s", self.job_name, self.connection_id)
