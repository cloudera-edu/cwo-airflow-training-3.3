# dags/

This folder is the **Git SubPath** that Workflow Orchestrator syncs.

- `instructor/` - the trainer's working copy of every lab
- `<your_name>/` - one folder per student, created with `scripts/new_student.sh <your_name>`

Only commit DAG files here. Solutions, Spark jobs and the custom image live elsewhere in the repo so they aren't parsed.
