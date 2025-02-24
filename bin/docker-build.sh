#!/bin/bash
set -euxo pipefail

script_dir="$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
project_dir="$(realpath ${script_dir}/..)"

short_commit="$(git rev-parse --short HEAD)"
pipeline_id="${GITHUB_RUN_ID:-0}"
tag="v${pipeline_id}-${short_commit}"

docker build \
  --tag "mc144/finance-toolkit:${tag}" \
  --tag "mc144/finance-toolkit:latest" \
  "$project_dir"
