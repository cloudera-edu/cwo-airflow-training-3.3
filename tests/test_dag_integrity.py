"""Checks that every DAG in the repo parses and serializes on Airflow 3.3.2, and follows the platform rules.

Run locally:  pytest tests/  (see the CI workflow for the install command)
"""
from pathlib import Path

import pytest
from airflow.dag_processing.dagbag import DagBag
from airflow.serialization.serialized_objects import DagSerialization

ROOT = Path(__file__).resolve().parents[1]
FOLDERS = ["dags", "solutions"]
DISABLED = {  # Workflow Orchestrator's default disabled-operator list
    "KubernetesPodOperator", "KubernetesJobOperator", "KubernetesDeleteJobOperator",
    "KubernetesPatchJobOperator", "KubernetesInstallKueueOperator", "KubernetesStartKueueJobOperator",
    "PythonVirtualenvOperator", "BranchPythonVirtualenvOperator",
}
# Lab files that use a disabled operator on purpose
EXPECTED_DISABLED = {"debug_disabled_operator_broken.py"}


def _bag(folder: str) -> DagBag:
    return DagBag(dag_folder=str(ROOT / folder))


@pytest.mark.parametrize("folder", FOLDERS)
def test_no_import_errors(folder):
    bag = _bag(folder)
    assert not bag.import_errors, "\n".join(f"{k}: {v}" for k, v in bag.import_errors.items())
    assert bag.dags, f"no DAGs found in {folder}"


def test_every_dag_serializes():
    """The DAG processor serializes each DAG; partition mappers and retry policies must survive it."""
    for dag_id, dag in _bag("dags").dags.items():
        DagSerialization.from_dict(DagSerialization.to_dict(dag))


def test_disabled_operators_only_where_intended():
    for dag in _bag("dags").dags.values():
        for t in dag.tasks:
            if type(t).__name__ in DISABLED:
                assert Path(dag.fileloc).name in EXPECTED_DISABLED, (
                    f"{dag.dag_id}.{t.task_id} uses disabled operator {type(t).__name__}"
                )


def test_dagrun_timeout_for_waiting_dags():
    """DAGs that defer or wait for people must set dagrun_timeout (platform default is 1 hour)."""
    waiting = ("hitl_backfill_example", "time_sensor_example", "deferrable_s3_file_sensor_example")
    for dag_id, dag in _bag("dags").dags.items():
        if dag_id.startswith(waiting):
            assert dag.dagrun_timeout is not None, f"{dag_id} needs dagrun_timeout"
