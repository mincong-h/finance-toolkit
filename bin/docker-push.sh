#!/bin/bash
set -euxo pipefail

short_commit="$(git rev-parse --short HEAD)"
pipeline_id="${GITHUB_RUN_ID:-0}"
tag="v${pipeline_id}-${short_commit}"

docker push "mc144/finance-toolkit:${tag}"
docker push "mc144/finance-toolkit:latest"
