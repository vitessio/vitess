#!/bin/bash
# Produces runnable binaries packaged in tar.gz
# If run inside of Kochiku, will also upload said binaries so that downstream builds can use them
set -euxo pipefail

buildpack_root_dir=/data/app/kochiku-worker/build-tools/buildpack
artifact_dir=/data/app/kochiku-worker/tmp/artifacts
app="vitess-linux-build"

function upload_artifacts {
  logging_args='$stderr, $stderr'
  gem install aws-sdk-s3
  ruby -I "${buildpack_root_dir}/lib" \
    -r build_and_upload_deployable \
    -e "BuildAndUploadDeployable.new(%w[$app el6 --upload-destination partial-artifact], RealOperations.new, $logging_args).sign_and_upload_deployable_artifacts(%q($app), %q(el6), %q($artifact_dir))"
}

if [[ -z ${KOCHIKU_ENV+x} ]]; then
  echo "running in local mode"
  GIT_COMMIT=$(git rev-parse HEAD)
  GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
fi

export REPO=vitess
BUILD_DOCKER_TAG=square-vitess-build-${GIT_COMMIT}

## Load the docker image file
if [[ -n ${KOCHIKU_ENV+x} ]]; then
  # if we are running inside of kochiku, first load the docker container
  docker load -i docker-cache/${BUILD_DOCKER_TAG}.tar
fi

# doing it manually using build and then run to make the container stay around until we can copy out files
docker build --build-arg GIT_COMMIT=${GIT_COMMIT} -t vitess-binary-builder -f square/binaries/Dockerfile .
docker run vitess-binary-builder ./build-linux.sh

if [[ -z ${KOCHIKU_ENV+x} ]]; then
  docker cp $(docker container ls -alq):/vt/src/vitess.io/vitess/vitess-linux-build.tar.gz .
else
  rm -rf $artifact_dir
  mkdir -p "${artifact_dir}/vitess-build"
  docker cp $(docker container ls -alq):/vt/src/vitess.io/vitess/vitess-linux-build.tar.gz $artifact_dir/vitess-build/vitess-linux-build-${GIT_COMMIT}.tar.gz
fi

docker run vitess-binary-builder ./build-osx.sh
if [[ -z ${KOCHIKU_ENV+x} ]]; then
  docker cp $(docker container ls -alq):/vt/src/vitess.io/vitess/vitess-osx-build.tar.gz .
else
  docker cp $(docker container ls -alq):/vt/src/vitess.io/vitess/vitess-osx-build.tar.gz $artifact_dir/vitess-build/vitess-osx-build-${GIT_COMMIT}.tar.gz
  upload_artifacts
fi
