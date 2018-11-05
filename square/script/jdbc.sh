#!/usr/bin/env bash
# Uploads all jvm modules to artifactory
set -ex

cd java
BRANCH=$(git rev-parse --abbrev-ref HEAD)

if [[ "$BRANCH" == "master" ]]; then
    SHA="$(git rev-parse HEAD)"
    CURRENT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
    BASE_VERSION=$(echo "${CURRENT_VERSION}" | grep -o -E '[0-9]+.[0-9]+.[0-9]')

    VERSION=${BASE_VERSION}-square-${SHA}

    mvn versions:set -DnewVersion="${VERSION}"
    mvn versions:update-child-modules

    #only deploy if merging into master
    mvn deploy -DskipTests -DaltDeploymentRepository=jar-releases::default::https://maven.global.square/artifactory/releases/
    echo "Uploaded jdbc driver version $VERSION"
else
    echo "not doing anything until merged into master"
fi