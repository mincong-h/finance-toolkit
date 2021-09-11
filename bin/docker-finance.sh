#!/bin/bash
docker run \
    --rm \
    --volume "${HOME}/Downloads:/data/source" \
    --volume "${HOME}/gitty/finance-data:/data/target" \
    finance-toolkit $@
