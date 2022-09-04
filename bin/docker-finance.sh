#!/bin/bash
#
# Wrapper script to run Finance Toolkit in Docker
#
# Arguments (environment variables):
#
#   FTK_DOCKER_MODE:
#       Optional. The Docker mode to be used: "remote" or "local". Default value
#       is "remote", meaning that the Docker image will be pulled from the
#       remote registry. "LOCAL" means using the Docker image built locally,
#       useful for testing.
#
REGISTRY_NAME="registry-intl.cn-hongkong.aliyuncs.com"
IMAGE_NAMESPACE="jimidata-prod"
image="${REGISTRY_NAME}/${IMAGE_NAMESPACE}/finance-toolkit:latest"

docker_mode=${FTK_DOCKER_MODE:-remote}

echo "FTK_DOCKER_MODE: ${FTK_DOCKER_MODE}"
echo "---"

if [ "$docker_mode" == "remote" ]
then
  docker pull "$image"
fi

docker run \
    --rm \
    --volume "${HOME}/Downloads:/data/source" \
    --volume "${FINANCE_ROOT}:/data/target" \
    "$image" $@
