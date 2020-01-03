#!/bin/bash

# This script builds and packages a Vitess release suitable for creating a new
# release on https://github.com/vitessio/vitess/releases.

# http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail

# sudo gem install --no-ri --no-rdoc fpm
# shellcheck disable=SC1091
source build.env

SHORT_REV="$(git rev-parse --short HEAD)"
VERSION="$(git describe --tags --dirty --always | sed  s/v//)"

RELEASE_ID="vitess-${VERSION}-${SHORT_REV}"
RELEASE_DIR="${VTROOT}/releases/${RELEASE_ID}"
DESCRIPTION="A database clustering system for horizontal scaling of MySQL

Vitess is a database solution for deploying, scaling and managing large
clusters of MySQL instances. It's architected to run as effectively in a public
or private cloud architecture as it does on dedicated hardware. It combines and
extends many important MySQL features with the scalability of a NoSQL database."

DEB_FILE="vitess_${VERSION}-${SHORT_REV}_amd64.deb"
RPM_FILE="vitess-${VERSION}-${SHORT_REV}.x86_64.rpm"
TAR_FILE="${RELEASE_ID}.tar.gz"

make tools
make build

mkdir -p releases

# Copy a subset of binaries from issue #5421
mkdir -p "${RELEASE_DIR}/bin"
for binary in vttestserver mysqlctl mysqlctld query_analyzer topo2topo vtaclcheck vtbackup vtbench vtclient vtcombo vtctl vtctlclient vtctld vtexplain vtgate vttablet vtworker vtworkerclient zk zkctl zkctld; do 
 cp "bin/$binary" "${RELEASE_DIR}/bin/"
done;

# Copy remaining files, preserving date/permissions
# But resolving symlinks
cp -rpfL {config,vthook,examples} "${RELEASE_DIR}/"

echo "Follow the binary installation instructions at: https://vitess.io/docs/get-started/local/" > "${RELEASE_DIR}"/README.md

cd "${RELEASE_DIR}/.."
tar -czf "${TAR_FILE}" "${RELEASE_ID}"

fpm \
   --force \
   --input-type dir \
   --name vitess \
   --version "${VERSION}" \
   --url "https://vitess.io/" \
   --description "${DESCRIPTION}" \
   --license "Apache License - Version 2.0, January 2004" \
   --prefix "/vt" \
   --directories "/vt" \
   --before-install "$VTROOT/tools/preinstall.sh" \
   -C "${RELEASE_DIR}" \
   --package "$(dirname "${RELEASE_DIR}")" \
   --iteration "${SHORT_REV}" \
   -t deb --deb-no-default-config-files

fpm \
   --force \
   --input-type dir \
   --name vitess \
   --version "${VERSION}" \
   --url "https://vitess.io/" \
   --description "${DESCRIPTION}" \
   --license "Apache License - Version 2.0, January 2004" \
   --prefix "/vt" \
   --directories "/vt" \
   --before-install "$VTROOT/tools/preinstall.sh" \
   -C "${RELEASE_DIR}" \
   --package "$(dirname "${RELEASE_DIR}")" \
   --iteration "${SHORT_REV}" \
   -t rpm

echo ""
echo "Packages created as of $(date +"%m-%d-%y") at $(date +"%r %Z")"
echo ""
echo "Package | SHA256"
echo "------------ | -------------"
echo "${TAR_FILE} | $(sha256sum "${VTROOT}/releases/${TAR_FILE}" | awk '{print $1}')"
echo "${DEB_FILE} | $(sha256sum "${VTROOT}/releases/${DEB_FILE}" | awk '{print $1}')"
echo "${RPM_FILE} | $(sha256sum "${VTROOT}/releases/${RPM_FILE}" | awk '{print $1}')"
