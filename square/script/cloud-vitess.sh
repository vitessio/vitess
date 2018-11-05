#!/usr/bin/env bash
# Produces the docker image that is used by downstreams cloud builds
# If run on Kuchiko, will also push the image to a shared repo
set -ex

## Build
export REPO=vitess
export TAG=${GIT_COMMIT}

make docker_base

docker build -t $REPO:$TAG -f square/Dockerfile.slim .

if [[ -z ${KOCHIKU_ENV+x} ]]; then
    echo "local mode. not pushing docker files"
else
    # Save the docker image so it can be loaded by later kochiku steps
	CASH_CI_DIR=/tmp/cash-ci
	rm -rf $CASH_CI_DIR
	git clone ssh://git@git.sqcorp.co/cash/cash-ci.git $CASH_CI_DIR
	$CASH_CI_DIR/cash-docker-push -r $REPO -t $TAG -p
fi
