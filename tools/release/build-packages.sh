#!/bin/bash

# Copyright 2026 The Vitess Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Builds the release packages (tar.gz, deb, rpm) of a release commit with
# tools/make-release-packages.sh, checksums them and smoke-tests the
# binaries. Run from a clean checkout of the release commit.
#
# Environment:
#   VERSION      required, X.Y.Z
#   RELEASE_SHA  required, the commit the checkout must be at
#   OUT_DIR      required, receives the packages, SHA256SUMS and packages.md

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/release/lib.sh
source "${SELF_DIR}/lib.sh"

VERSION="${VERSION:?VERSION is required}"
RELEASE_SHA="${RELEASE_SHA:?RELEASE_SHA is required}"
OUT_DIR="${OUT_DIR:?OUT_DIR is required}"

# fpm builds the deb and rpm. The version is pinned and the gem verified
# against the checksum published at https://rubygems.org/gems/fpm/versions.
# Reviewers: the expected checksum MUST ALWAYS match the one on that page.
FPM_VERSION="1.16.0"
FPM_SHA256="d9eafe613cfbdf9d3b8ef2e321e194cd0a2d300ce37f716c0be1b3a42b7db5df"

install_fpm() {
  local gem="fpm-${FPM_VERSION}.gem" got
  gem fetch fpm -v "${FPM_VERSION}"
  got="$(sha256sum "${gem}" | awk '{print $1}')"
  if [ "${got}" != "${FPM_SHA256}" ]; then
    die "checksum of ${gem} is ${got}, expected ${FPM_SHA256}"
  fi
  sudo gem install "${gem}"
  rm -f "${gem}"
}

version_parse "${VERSION}" >/dev/null
mkdir -p "${OUT_DIR}"

log "== checking the working tree"
if [ "$(git rev-parse HEAD)" != "${RELEASE_SHA}" ]; then
  die "HEAD is $(git rev-parse HEAD), expected the release commit ${RELEASE_SHA}"
fi
if [ -n "$(git status --porcelain --untracked-files=all)" ]; then
  die "the working tree is not clean"
fi
if [ "$(read_version_go .)" != "${VERSION}" ]; then
  die "version.go reads $(read_version_go .), expected ${VERSION}"
fi
short_sha="$(git rev-parse --short HEAD)"

log "== installing fpm ${FPM_VERSION}"
install_fpm

log "== building the packages for ${VERSION} at ${RELEASE_SHA}"
./tools/make-release-packages.sh "${VERSION}"

log "== checksumming"
packages=()
for package in releases/*.tar.gz releases/*.deb releases/*.rpm; do
  packages+=("$(basename "${package}")")
done
if [ "${#packages[@]}" -ne 3 ]; then
  die "expected one tar.gz, one deb and one rpm under releases/, found: ${packages[*]}"
fi
for package in "${packages[@]}"; do
  case "${package}" in
    *"${VERSION}"*"${short_sha}"*) ;;
    *) die "package ${package} does not carry the version ${VERSION} and the commit ${short_sha} in its name" ;;
  esac
done
(cd releases && sha256sum "${packages[@]}" > SHA256SUMS)

log "== smoke testing the binaries"
scratch="$(mktemp -d)"
tar -xzf "releases/vitess-${VERSION}-${short_sha}.tar.gz" -C "${scratch}"
banner="$("${scratch}/vitess-${VERSION}-${short_sha}/bin/vtgate" --version)"
rm -rf "${scratch}"
log "${banner}"
case "${banner}" in
  "Version: ${VERSION} "*"Git revision ${RELEASE_SHA} "*) ;;
  *) die "vtgate --version does not report version ${VERSION} built from ${RELEASE_SHA}" ;;
esac

for package in "${packages[@]}" SHA256SUMS; do
  cp "releases/${package}" "${OUT_DIR}/"
done

{
  echo "## Release packages for v${VERSION}"
  echo
  echo "Built from \`${RELEASE_SHA}\`."
  echo
  echo "| Package | Size | SHA256 |"
  echo "|---|---|---|"
  for package in "${packages[@]}"; do
    echo "| ${package} | $(du -h "releases/${package}" | awk '{print $1}') | \`$(awk -v p="${package}" '$2 == p {print $1}' releases/SHA256SUMS)\` |"
  done
  echo
  echo '```'
  echo "${banner}"
  echo '```'
} > "${OUT_DIR}/packages.md"
gh_step_summary "${OUT_DIR}/packages.md"
