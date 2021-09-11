#!/bin/bash

IMAGE_NAME='finance-toolkit'
short_commit=$(git rev-parse --short HEAD)

script_dir="$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
docker_dir="${script_dir}/.."

docker build \
  --tag "${IMAGE_NAME}:${tag}" \
  "$docker_dir"
