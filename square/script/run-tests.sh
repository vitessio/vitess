#!/usr/bin/env bash
# Runs tests
set -euxo pipefail

if [[ -n ${KOCHIKU_ENV+x} ]]; then
  export GOROOT=/usr/local/go
  export PATH=$GOROOT/bin:$PATH
fi

if [[ "$1" == "1" ]];
then
  # we want to verify that all test shards are mapped to kochiku correctly
  echo "Verifying vitess to kochiku shard mapping"
  KOCHIKU_TOTAL_WORKERS=$2
  go run square/cmd/test_shard_mapper/shard_mapper.go --verify=true --worker_count="$KOCHIKU_TOTAL_WORKERS"
fi

KOCHIKU_WORKER_CHUNK=$1
read -r SHARD_NUMBER USE_DOCKER FLAVOR <<<"$(go run square/cmd/test_shard_mapper/shard_mapper.go --worker="$KOCHIKU_WORKER_CHUNK")"
echo "SHARD_NUMBER $SHARD_NUMBER USE_DOCKER $USE_DOCKER FLAVOR $FLAVOR"

if [[ "$USE_DOCKER" == "false" ]];
then
  # if USE_DOCKER is false, test.go will not call docker so we need to call docker directly
  if [[ -z ${KOCHIKU_ENV+x} ]]; then
    echo "running in local mode"
    GIT_COMMIT=$(git rev-parse HEAD)
  fi

  BUILD_DOCKER_TAG=square-vitess-build-${GIT_COMMIT}
  CONTAINER_NAME=vitess_"$1"

  function cleanup() {
    docker stop "$CONTAINER_NAME"
    docker rm "$CONTAINER_NAME"
  }
  trap cleanup EXIT

  if [[ -n ${KOCHIKU_ENV+x} ]]; then
    echo "running in ci mode, load the docker container"
    docker load -i docker-cache/"${BUILD_DOCKER_TAG}".tar
  fi

  docker run -dit --name "$CONTAINER_NAME" "$BUILD_DOCKER_TAG"
  # start syslog, this is necessary for the tests to pass
  docker exec -u root "$CONTAINER_NAME" /bin/bash -c "syslog-ng && echo done"
  docker exec "$CONTAINER_NAME" /bin/bash -c "go run test.go -docker=false -timeout=15m -exclude=square-fail -print-log -shard=$SHARD_NUMBER -flavor=$FLAVOR"
else
  # if USE_DOCKER is true, test.go will call docker indirectly to create the container
  # FIXME(lcabancla) tests with USE_DOCKER=false fail locally
  go run test.go -docker=true -timeout=15m -exclude=square-fail -print-log -shard="$SHARD_NUMBER -flavor=$FLAVOR"
fi