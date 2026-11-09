# Test results

## Airflow 3.3.2 (local), 9 November 2026

Run against **Apache Airflow 3.3.2** (Task SDK 1.3.2) with Standard provider 1.19.0 and Amazon provider 9.36.0, using a local SQLite metadata database. The CDE provider is a stub (`tests/stubs`).

| Check | Result |
|---|---|
| `pytest tests/` (parse, serialize, disabled operators, `dagrun_timeout`) | 5 passed |
| DAGs loaded from `dags/instructor/` | 46, no import errors |
| Every DAG serialized and deserialized (`DagSerialization`) | 46 of 46 |

Labs 23–25 were also run with `airflow dags test` and the scheduler:

| Lab | What was run | Result |
|---|---|---|
| 23, Part A | `resumable_batches` | Try 1 failed at batch 3; try 2 logged "resuming after batch 2 (read from task_state_store)" and finished batches 3–5 |
| 23, Part B | `incremental_orders`, twice | Run 1 loaded orders 1–5 (watermark 0 → 5); run 2 loaded 6–10 (5 → 10) |
| 24 | `retry_policy_example`, `failure_type=transient` | "Retry policy decision action=retry reason='Transient error: the source may recover'", task up_for_retry |
| 24 | `retry_policy_example`, `failure_type=permanent` | "Retry policy decision action=fail reason='Bad input: retrying won't help'", failed on try 1 |
| 25 | `regional_sales` for north, south, east, scheduler between each | After north: no consumer runs. After south: `early_regions_report` (MinimumCount(2)) ran once. After east: `all_regions_report` (WaitForAll) ran once; `early_regions_report` did not run again |

Not yet tested:

- **Everything on a Workflow Orchestrator 2.0 instance.** The platform may enable or restrict 3.3 features (state store, retry policies, rollups) differently from open-source Airflow.
- **Lab 26** (`rerun_with_latest_version`, bulk actions, Gantt, log search): needs the UI.
- **Labs 05 and 10** (CDE).

## Airflow 3.2.1

Labs 01–22 were tested end to end on Workflow Orchestrator 1.6.0 (Airflow 3.2.1) in November 2026, except Labs 05 and 10 (CDE). See the `cwo-airflow-training` repository.
