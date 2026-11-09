# Airflow on Cloudera Workflow Orchestrator (Airflow 3.3.2): lab repository

[![Validate DAGs](https://github.com/cloudera-edu/cwo-airflow-training-3.3/actions/workflows/validate-dags.yml/badge.svg)](https://github.com/cloudera-edu/cwo-airflow-training-3.3/actions/workflows/validate-dags.yml)

Repository: https://github.com/cloudera-edu/cwo-airflow-training-3.3

Lab DAGs for the course *Building and Operating Airflow Pipelines on Cloudera Anywhere Cloud*, on **Cloudera Workflow Orchestrator in Anywhere Cloud 2.0** (Apache Airflow 3.3.2, Python 3.12). Workflow Orchestrator deploys DAGs by syncing them from this repository.

Labs 01–22 are the same as in the Airflow 3.2.1 repository ([cwo-airflow-training](https://github.com/cloudera-edu/cwo-airflow-training)); only version strings and the Lab 22 discussion note changed. Labs 23–26 cover features new in Airflow 3.3:

| Lab | Folder | Airflow 3.3 feature |
|---|---|---|
| 23 | `23-task-and-asset-state-store` | Task and asset state store (AIP-103): resume after a retry, incremental-load watermark |
| 24 | `24-retry-policies` | Retry policies (AIP-105): `ExceptionRetryPolicy`, `RetryRule` |
| 25 | `25-partition-rollups` | `PartitionedAtRuntime`, `RollupMapper` with `FixedKeyMapper`, `SegmentWindow`, `WaitForAll` and `MinimumCount` |
| 26 | `26-operating-pipelines` | `rerun_with_latest_version`, clear with a note, bulk actions, log search, Gantt chart |

## Repository layout

```
.
├── dags/                    <- Git SubPath synced by Workflow Orchestrator
│   ├── instructor/          <- trainer's copy of every lab
│   ├── student01/           <- one folder per student (created with scripts/new_student.sh)
│   └── student02/
├── student-template/        <- master copy of the labs; never synced
├── solutions/               <- solutions for labs 04 and 11 (kept out of dags/ so DAG IDs don't clash)
├── spark/                   <- PySpark scripts to upload as CDE resources (labs 05 and 10)
├── scripts/new_student.sh   <- creates dags/<student_name>/
├── tests/                   <- DAG checks run by CI
├── .github/workflows/       <- CI: parses and serializes every DAG on Airflow 3.3.2 on each push and pull request
└── docs/TEST_RESULTS.md
```

Each DAG takes the student name from its folder (`dags/<student_name>/<lab>/file.py`). That name goes into every DAG ID and asset URI, so students on one shared instance never trigger each other's DAGs, and nobody edits the DAG files to set a name.

## 1. Connect the repository to Workflow Orchestrator

When deploying the instance (Marketplace > Cloudera Workflow Orchestrator > Deploy), set:

| Field | Value |
|---|---|
| Git Repository URL | `https://github.com/cloudera-edu/cwo-airflow-training-3.3.git` |
| Git Branch | `main` |
| Git SubPath | `dags` |
| SSH Private Key | Leave empty. The repository is public, so no key is needed |

The compute cluster needs network access to `github.com`.

> **The repository is public.** Anyone can read it, so never commit credentials. Connections, keys and secrets live only in Workflow Orchestrator. If a customer needs a private copy, make the repository private, add a read-only [deploy key](https://github.com/cloudera-edu/cwo-airflow-training-3.3/settings/keys), and use the SSH URL `git@github.com:cloudera-edu/cwo-airflow-training-3.3.git` with the Base64-encoded private key (`base64 -w0 id_ed25519`).

## 2. Instructor checklist: before the training

### One week before

**Platform**
- [ ] Deploy the Workflow Orchestrator instance with the Git settings in section 1. For 10 students, plan about 6 worker nodes (8 CPU / 16 GB). The idle platform alone uses about 14 CPU and 32 GB.
- [ ] Set **remote logging** to the class Object Store bucket, so task logs survive after worker pods are removed.
- [ ] Leave the **Disabled Operators** field at its default. Lab 11 relies on `PythonVirtualenvOperator` being blocked.
- [ ] Confirm that new DAGs start **paused**. Otherwise every student's scheduled DAGs start running as soon as they're pushed.
- [ ] Confirm with the platform team that the **state store** (labs 23, 25), **retry policies** (lab 24) and **partition rollups** (lab 25) are enabled on the instance.

**Accounts and access**
- [ ] Create an Anywhere Cloud account for each student. Check that their role can trigger, clear and pause DAGs and respond to human-in-the-loop tasks (lab 20).
- [ ] Give each student **write** access to the repository: add them to a `cloudera-edu` team with write permission, or as collaborators under [Settings > Collaborators and teams](https://github.com/cloudera-edu/cwo-airflow-training-3.3/settings/access). Remove them after the class.
- [ ] Protect `main` under [Settings > Branches](https://github.com/cloudera-edu/cwo-airflow-training-3.3/settings/branches): block force pushes and deletion, and require the **Validate DAGs** check to pass.
- [ ] Check that [Actions](https://github.com/cloudera-edu/cwo-airflow-training-3.3/actions) are enabled for the repository, so the check runs on every push.
- [ ] Students push over HTTPS with a personal access token (or with SSH keys on their GitHub account). Tell them which one to use in the hand-out.
- [ ] Create AWC OAuth credentials (client ID and secret) for the CDE connection (labs 05 and 10). No other lab needs API credentials.

**Connections** (Admin > Connections in the Workflow Orchestrator UI). These are shared by the class, so only the instructor creates or changes them.

| Connection ID | Type | Used by |
|---|---|---|
| `cde_runtime_api` | Cloudera Data Engineering. Host: virtual cluster Jobs API URL; Schema: `default`; Access Key / Private Key: AWC OAuth client ID / secret; Extra: `{"awc_console_url": "<console URL>", "auth_mode": "awc", "cache_dir": "/tmp"}` | 05, 10 |
| `object_store` | Amazon Web Services. AWS Access Key ID / Secret Access Key: keys of an IAM user limited to the class bucket; Extra: `{"region_name": "<bucket region>"}` | 09, 12, 13 |

**Data and CDE**
- [ ] Check the class bucket `cloudera-edu-cwo-airflow-training` exists and the `object_store` connection passes its connection test. The bucket is kept between classes; empty the student prefixes after each class.
- [ ] In CDE on Anywhere Cloud, upload `spark/` as a resource and create `word_count_job`, `ingest_job`, `validate_job`, `transform_job` and `publish_job`. Give the lab 10 jobs the arguments `<base_path> <load_date>`, and give them S3A access to Object Store in their Spark configuration. The jobs are shared by the class; they write the same sample data each run, so students rerunning them don't break each other's labs.

**Pools**
- [ ] Size `default_pool` for the cluster: (total worker RAM in GB − 32) ÷ 1. This stops a class-wide catchup or fan-out from overloading the workers.

### Two or three days before

- [ ] Create the student folders and push them. `student01` and `student02` already exist; add one per extra student:

```bash
git clone https://github.com/cloudera-edu/cwo-airflow-training-3.3.git
cd cwo-airflow-training-3.3
for s in student03 student04 student05; do scripts/new_student.sh $s; done
git add dags && git commit -m "Lab folders for <batch name>" && git push
```

- [ ] After the next Git sync, check there are **no DAG import errors** in the UI, other than `debug_disabled_operator_*` (lab 11, broken on purpose).
- [ ] Dry run from `dags/instructor/`, at least labs 01, 05 (CDE), 09 (Object Store) and 12 (triggerer). This checks every connection.
- [ ] Prepare a hand-out for each student: Anywhere Cloud Console URL, their login, the repository URL (`https://github.com/cloudera-edu/cwo-airflow-training-3.3`), their folder name (for example `student01`), and how to push (token or SSH).

### On the day

- [ ] Check the instance is **Deployed** in Deployed Experiences and the UI opens.
- [ ] Check the latest commit on `main` has synced: the DAG list should show every student's DAGs. Check that the latest **Validate DAGs** run on GitHub is green.
- [ ] Go through the student rules in section 3 at the start of the first lab.

### After the training

- [ ] Remove the student folders: `git rm -r dags/student*`, then commit and push. Keep `dags/instructor/`.
- [ ] Remove the students' repository access and Anywhere Cloud accounts.
- [ ] Rotate the OAuth secrets and Object Store keys used in the connections.
- [ ] Empty the student prefixes in the bucket: `s3://<bucket>/<student_name>/`.

## 3. Student guide: working on a shared instance

Everyone in the class uses **one Workflow Orchestrator instance and one Git repository**. Your folder under `dags/` is yours alone. Your DAG IDs and your data paths in Object Store all end with your folder name, so your work stays separate from everyone else's as long as you follow these rules.

### Getting started

```bash
git clone https://github.com/cloudera-edu/cwo-airflow-training-3.3.git
cd cwo-airflow-training-3.3
git config user.name "<your name>"
git config user.email "<your email>"
ls dags/          # find your folder, for example dags/student01/
```

When Git asks for a password on your first push, use the personal access token from your hand-out, not your GitHub password.

Your instructor has already created your folder. If not, run `scripts/new_student.sh <your_name>`, then commit and push it.

### Daily workflow

```bash
git pull --rebase                       # always start with the latest version
# edit files under dags/<your_name>/ only
git add dags/<your_name>
git commit -m "<your_name>: lab 04 branching"
git pull --rebase && git push
```

Wait for the next Git sync, then find your DAGs in the UI by searching for your name, for example `xcom_exercise_student01`.

### Rules

**In Git**
1. Only change files inside `dags/<your_name>/`. Don't touch `dags/instructor/`, other students' folders, `student-template/`, or anything outside `dags/`.
2. Use `git add dags/<your_name>`, never `git add .` or `git add -A`.
3. Never use `git push --force`, and don't rename or delete your folder.
4. If `git pull --rebase` shows a conflict, you have changed a file outside your folder. Ask your instructor before going on.
5. Never commit passwords, keys or tokens. Credentials live in Airflow connections, not in DAG files.

**In the DAG files**
6. Don't edit the `STUDENT = ...` line or remove `{STUDENT}` from DAG IDs and asset names. That's what keeps your DAGs separate from everyone else's.
7. Write data only under your own prefix, `s3://<bucket>/<your_name>/`. The labs already do this.

**In the Workflow Orchestrator UI**
8. Only unpause, trigger, clear or delete runs of DAGs that end with your name.
9. Pause your DAGs when you finish a lab, especially scheduled ones (labs 07, 21 and 22 run every 5 minutes).
10. Don't create, edit or delete **Connections**, **Variables** or **Pools** unless a lab tells you to. If it does, use a name that ends with your name. The `cde_runtime_api` and `object_store` connections are shared by the class.
11. Respond only to human-in-the-loop requests (lab 20) from your own DAGs.

### If something goes wrong

- **Your DAG doesn't appear:** check that your push reached `main` and the Git sync has run. Then look under **DAG import errors** for a file in your folder.
- **The "Validate DAGs" check fails on GitHub:** open the failed run under [Actions](https://github.com/cloudera-edu/cwo-airflow-training-3.3/actions), fix the file named in its log, and push again.
- **`git push` is rejected:** someone pushed before you. Run `git pull --rebase`, then push again.
- **A task is stuck in queued:** the shared pool may be full. Wait, or ask your instructor; don't change the pool.

## 4. Before you push: check locally (optional)

```bash
python3.12 -m venv .venv && source .venv/bin/activate
pip install "apache-airflow==3.3.2" apache-airflow-providers-standard apache-airflow-providers-amazon \
    aiobotocore pytest \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
export AIRFLOW_HOME=$PWD/airflow-home PYTHONPATH=$PWD/tests/stubs
airflow db migrate && pytest -q tests/
```

## Notes

- **Lab 11, part 2:** `debug_disabled_operator_broken.py` uses a disabled operator on purpose, so the DAG processor reports it as an import error on Workflow Orchestrator. CI allows it.
- **Labs 21 and 22** run their producer every 5 minutes so the lab fits in class time; in production these would be hourly.
- **Lab 25** uses a categorical rollup (regions) instead of `DayWindow`, which expects all 24 hourly partitions of a day.
- **Tests:** CI uses a stub for the Cloudera CDE provider, which is pre-installed on Workflow Orchestrator but not on PyPI. See `docs/TEST_RESULTS.md` for what has been tested.
