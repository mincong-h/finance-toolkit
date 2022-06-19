#!/bin/bash

REGISTRY_NAME="registry-intl.cn-hongkong.aliyuncs.com"
IMAGE_NAMESPACE="jimidata-prod"
image="${REGISTRY_NAME}/${IMAGE_NAMESPACE}/finance-toolkit:latest"

docker pull "$image"
docker run \
    --rm \
    --volume "${HOME}/Downloads:/data/source" \
    --volume "${HOME}/gitty/finance-data:/data/target" \
    "$image" $@
