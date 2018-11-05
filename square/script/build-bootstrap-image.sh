#!/usr/bin/env bash
# Produces the docker images that most other scripts use
# If run on Kuchiko, will also upload the image file to a shared cache so the build can be distributed
set -euxo pipefail

source ./square/script/bootstrap-image-vars.sh

if [[ -z ${KOCHIKU_ENV+x} ]]; then
  echo "running in local mode"
  GIT_COMMIT=$(git rev-parse HEAD)
fi

# builds the square vitess docker image from the pwd and pushes a runnable image to ECR
# assumes GIT_COMMIT and GIT_BRANCH are set (which is true if run on kochiku)

# Rebuild the bootstrap images in case something has changed
docker/bootstrap/build.sh common
docker/bootstrap/build.sh mysql57

if [[ -z ${KOCHIKU_ENV+x} ]]; then
    echo "local mode. not saving docker files"
else
    # Save the docker image so it can be loaded by later kochiku steps
    mkdir -p $VITESS_BOOTSTRAP_DOCKER_IMAGE_DIR
    docker save -o $VITESS_BOOTSTRAP_DOCKER_IMAGE_PATH vitess/bootstrap:0-common vitess/bootstrap:0-mysql57
fi