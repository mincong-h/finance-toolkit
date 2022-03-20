#!/bin/bash
set -euxo pipefail

REGISTRY_NAME="registry-intl.cn-hongkong.aliyuncs.com"
IMAGE_NAMESPACE="jimidata-prod"
IMAGE_NAME="finance-toolkit"

script_dir="$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
project_dir="$(realpath ${script_dir}/..)"

short_commit="$(git rev-parse --short HEAD)"
pipeline_id="${GITHUB_RUN_ID:-0}"
tag="v${pipeline_id}-${short_commit}"

docker build \
  --tag "${IMAGE_NAME}:${tag}" \
  --tag "${IMAGE_NAME}:latest" \
  --tag "${REGISTRY_NAME}/${IMAGE_NAMESPACE}/${IMAGE_NAME}:${tag}" \
  --tag "${REGISTRY_NAME}/${IMAGE_NAMESPACE}/${IMAGE_NAME}:latest" \
  "$project_dir"
