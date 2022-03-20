#!/bin/bash
set -euxo pipefail

REGISTRY_NAME="registry-intl.cn-hongkong.aliyuncs.com"
IMAGE_NAMESPACE="jimidata-prod"
IMAGE_NAME="finance-toolkit"

short_commit="$(git rev-parse --short HEAD)"
pipeline_id="${GITHUB_RUN_ID:-0}"
tag="v${pipeline_id}-${short_commit}"

docker push "${REGISTRY_NAME}/${IMAGE_NAMESPACE}/${IMAGE_NAME}:${tag}"
docker push "${REGISTRY_NAME}/${IMAGE_NAMESPACE}/${IMAGE_NAME}:latest"
