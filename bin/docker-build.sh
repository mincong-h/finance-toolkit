#!/bin/bash

IMAGE_NAME='finance-toolkit'

tag="$(git rev-parse --short HEAD)"
script_dir="$(cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd)"
docker_dir="${script_dir}/.."

docker build \
  --tag "${IMAGE_NAME}:${tag}" \
  --tag "${IMAGE_NAME}:latest" \
  "$docker_dir"
