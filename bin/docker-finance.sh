#!/bin/bash

REGISTRY_NAME="registry-intl.cn-hongkong.aliyuncs.com"
IMAGE_NAMESPACE="jimidata-prod"

docker run \
    --rm \
    --volume "${HOME}/Downloads:/data/source" \
    --volume "${HOME}/gitty/finance-data:/data/target" \
    "${REGISTRY_NAME}/${IMAGE_NAMESPACE}/finance-toolkit:latest" $@
