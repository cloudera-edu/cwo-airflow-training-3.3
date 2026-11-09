#!/usr/bin/env bash
# Creates your lab folder: dags/<your_name>/ with every lab DAG.
# Usage: scripts/new_student.sh <your_name>     (lowercase letters, digits, underscores)
set -euo pipefail
name="${1:-}"
if [[ ! "$name" =~ ^[a-z][a-z0-9_]{1,30}$ ]]; then
  echo "Usage: $0 <your_name>   (lowercase letters, digits, underscores; start with a letter)" >&2
  exit 1
fi
root="$(cd "$(dirname "$0")/.." && pwd)"
dest="$root/dags/$name"
if [[ -e "$dest" ]]; then
  echo "dags/$name already exists" >&2
  exit 1
fi
cp -r "$root/student-template" "$dest"
echo "Created dags/$name. Next:"
echo "  git add dags/$name && git commit -m \"Add lab folder for $name\" && git push"
