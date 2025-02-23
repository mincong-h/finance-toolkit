#!/bin/bash
#
# Wrapper script to run Finance Toolkit in Docker
#
# Arguments (environment variables):
#
#   FTK_DOCKER_MODE:
#       Optional. The Docker mode to be used: "remote" or "local". Default value
#       is "remote", meaning that the Docker image will be pulled from the
#       remote registry. "local" means using the Docker image built locally,
#       useful for testing.
#
image="mc144/finance-toolkit:latest"

docker_mode=${FTK_DOCKER_MODE:-remote}

echo "FTK_DOCKER_MODE: ${docker_mode}"
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
