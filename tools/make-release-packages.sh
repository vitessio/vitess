#!/bin/bash

# This script builds and packages a Vitess release suitable for creating a new
# release on https://github.com/vitessio/vitess/releases.

# http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail

# sudo gem install --no-ri --no-rdoc fpm
# shellcheck disable=SC1091
source build.env

SHORT_REV="$(git rev-parse --short HEAD)"
if [ -n "$*" ]; then
    VERSION="$1"
else
    VERSION="$(git describe --tags --dirty --always | sed  s/^v// | sed s/-dirty//)"
fi

RELEASE_ID="vitess-${VERSION}-${SHORT_REV}"
RELEASE_DIR="${VTROOT}/releases/${RELEASE_ID}"
DESCRIPTION="A database clustering system for horizontal scaling of MySQL

Vitess is a database solution for deploying, scaling and managing large
clusters of MySQL instances. It's architected to run as effectively in a public
or private cloud architecture as it does on dedicated hardware. It combines and
extends many important MySQL features with the scalability of a NoSQL database."

TAR_FILE="${RELEASE_ID}.tar.gz"

make tools
make build

mkdir -p releases

# Copy a subset of binaries from issue #5421
mkdir -p "${RELEASE_DIR}/bin"
for binary in vttestserver mysqlctl mysqlctld query_analyzer topo2topo vtaclcheck vtbackup vtbench vtclient vtcombo vtctl vtctlclient vtctld vtexplain vtgate vttablet vtorc vtworker vtworkerclient zk zkctl zkctld; do 
 cp "bin/$binary" "${RELEASE_DIR}/bin/"
done;

# Copy remaining files, preserving date/permissions
# But resolving symlinks
cp -rpfL examples "${RELEASE_DIR}"

echo "Follow the installation instructions at: https://vitess.io/docs/get-started/local/" > "${RELEASE_DIR}"/examples/README.md

cd "${RELEASE_DIR}/.."
tar -czf "${TAR_FILE}" "${RELEASE_ID}"

cd "${RELEASE_DIR}"
PREFIX=${PREFIX:-/usr}

# For RPMs and DEBs, binaries will be in /usr/bin
# Examples will be in /usr/share/vitess/examples

mkdir -p share/vitess/
mv examples share/vitess/

fpm \
   --force \
   --input-type dir \
   --name vitess \
   --version "${VERSION}" \
   --url "https://vitess.io/" \
   --description "${DESCRIPTION}" \
   --license "Apache License - Version 2.0, January 2004" \
   --prefix "$PREFIX" \
   -C "${RELEASE_DIR}" \
   --before-install "$VTROOT/tools/preinstall.sh" \
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
   --prefix "$PREFIX" \
   -C "${RELEASE_DIR}" \
   --before-install "$VTROOT/tools/preinstall.sh" \
   --package "$(dirname "${RELEASE_DIR}")" \
   --iteration "${SHORT_REV}" \
   -t rpm

cd "${VTROOT}"/releases
echo ""
echo "Packages created as of $(date +"%m-%d-%y") at $(date +"%r %Z")"
echo ""
echo "Package | SHA256"
echo "------------ | -------------"
for file in $(find . -type f -printf '%T@ %p\n' | sort -n | tail -3 | awk '{print $2}' | sed s?^./??); do
    echo "$file | $(sha256sum "$file" | awk '{print $1}')";
done
