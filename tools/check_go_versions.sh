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

# ci workflows
TPL_GO_VERSIONS="$(awk '/go-version: /{print $(NF-0)}' .github/workflows/*.yml test/templates/*.tpl | sort -u)"
TPL_GO_VERSIONS_COUNT=$(echo "$TPL_GO_VERSIONS" | wc -l | tr -d [:space:])
if [ "${TPL_GO_VERSIONS_COUNT}" -gt 1 ]; then
  echo -e "expected a consistent 'go-version:' in CI workflow files/templates, found versions:\n${TPL_GO_VERSIONS}"
  exit 1
fi
TPL_GO_VERSION="${TPL_GO_VERSIONS}"
if [[ ! "${TPL_GO_VERSION}" =~ "${GO_MOD_VERSION}" ]]; then
  echo "expected go-version in test/templates/* to be equal to go.mod: '${TPL_GO_VERSION}' != '${GO_MOD_VERSION}'"
  exit 1
fi

# docker/bootstrap/Dockerfile.common
BOOTSTRAP_GO_VERSION="$(awk -F ':' '/golang:/{print $(NF-0)}' docker/bootstrap/Dockerfile.common | cut -d- -f1)"
if [[ ! "${BOOTSTRAP_GO_VERSION}" =~ "${GO_MOD_VERSION}" ]]; then
  echo "expected golang docker version in docker/bootstrap/Dockerfile.common to be equal to go.mod: '${TPL_GO_VERSION}' != '${GO_MOD_VERSION}'"
  exit 1
elif [ "${TPL_GO_VERSION}" != "${BOOTSTRAP_GO_VERSION}" ]; then
  echo "expected equal go version in CI workflow files/templates and bootstrap Dockerfile: '${TPL_GO_VERSIONS}' != '${BOOTSTRAP_GO_VERSION}'"
  exit 1
fi
