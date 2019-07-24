#!/usr/bin/env bash
# Produces the docker images that most other scripts use
# If run on Kuchiko, will also upload the image file to a shared cache so the build can be distributed
set -euxo pipefail

if [[ -z ${KOCHIKU_ENV+x} ]]; then
  echo "running in local mode"
  GIT_COMMIT=$(git rev-parse HEAD)
fi

# builds the square vitess docker image from the pwd and pushes a runnable image to ECR
# assumes GIT_COMMIT and GIT_BRANCH are set (which is true if run on kochiku)

# Rebuild the bootstrap images in case something has changed
docker/bootstrap/build.sh common
docker/bootstrap/build.sh mysql57

# Now we build using the regular vitess Dockerfile
BUILD_DOCKER_TAG=square-vitess-build-${GIT_COMMIT}
docker build -t "$BUILD_DOCKER_TAG" --build-arg CGO_ENABLED=0 .

# TODO: antaylor - running a single test to exercise the binary production
# tests are failing at the moment
docker run "$BUILD_DOCKER_TAG" go test vitess.io/vitess/go/vt/vttablet/heartbeat

REPO=square-vitess

# Now we build the vitess image we intend on running
# The underlying Dockerfile should be using an image tagged with $BUILD_DOCKER_TAG
# This is done so we can continue to use the normal vitess Dockerfile in the above build.
docker build -t "$REPO:$GIT_COMMIT" --build-arg GIT_COMMIT="${GIT_COMMIT}" --file square/Dockerfile .

if [[ -z ${KOCHIKU_ENV+x} ]]; then
    echo "local mode. not pushing docker files"
else
    # Save the docker image so it can be loaded by later kochiku steps
    mkdir -p docker-cache
    docker save -o "docker-cache/${BUILD_DOCKER_TAG}.tar" "$BUILD_DOCKER_TAG"
    CASH_CI_DIR=/tmp/cash-ci
    rm -rf ${CASH_CI_DIR}
    git clone ssh://git@git.sqcorp.co/cash/cash-ci.git ${CASH_CI_DIR}
    ${CASH_CI_DIR}/cash-docker-push -r ${REPO} -t "${GIT_COMMIT}" -p
fi
