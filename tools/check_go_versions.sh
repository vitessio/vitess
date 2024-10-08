#!/bin/bash
#
# Validate that the go versions in go.mod, CI test workflows
# and docker/bootstrap/Dockerfile.common are compatible.
#
# This is called from the Static Code Checks CI workflow.

set -e

# go.mod
GO_MOD_VERSION="$(awk '/^go [0-9].[0-9]+/{print $(NF-0)}' go.mod)"
if [ -z "${GO_MOD_VERSION}" ]; then
  echo "cannot find go version in go.mod"
  exit 1
fi

# docker/bootstrap/Dockerfile.common
BOOTSTRAP_GO_VERSION="$(awk -F ':' '/golang:/{print $(NF-0)}' docker/bootstrap/Dockerfile.common | cut -d- -f1)"
if [[ ! "${BOOTSTRAP_GO_VERSION}" =~ "${GO_MOD_VERSION}" ]]; then
  echo "expected golang docker version in docker/bootstrap/Dockerfile.common to be equal to go.mod: '${TPL_GO_VERSION}' != '${GO_MOD_VERSION}'"
  exit 1
fi
