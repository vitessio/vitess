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

# Shared helpers for the patch release automation driven by
# .github/workflows/release.yml. Every function here is a pure function of
# its arguments and the files it is pointed at; nothing in this file talks
# to GitHub or pushes anything. tools/release/selftest.sh covers it.

set -euo pipefail

# Pinned so that `mvn versions:set` resolves the same plugin on every run
# instead of whatever the latest release happens to be that day.
VERSIONS_MAVEN_PLUGIN="org.codehaus.mojo:versions-maven-plugin:2.18.0"

log() {
  printf '%s\n' "$*" >&2
}

die() {
  log "error: $*"
  exit 1
}

# Appends key=value to $GITHUB_OUTPUT when running under Actions and echoes
# it otherwise, so the scripts can be run and inspected locally.
gh_output() {
  local key="$1" value="$2"
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    printf '%s=%s\n' "${key}" "${value}" >> "${GITHUB_OUTPUT}"
  fi
  log "output: ${key}=${value}"
}

# Appends a markdown file to the step summary when running under Actions.
gh_step_summary() {
  local file="$1"
  if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    cat "${file}" >> "${GITHUB_STEP_SUMMARY}"
  fi
}

# Prints "MAJOR MINOR PATCH" for X.Y.Z or X.Y.Z-SNAPSHOT; anything else,
# including tags with a leading v and release candidates, is rejected.
version_parse() {
  local version="$1"
  if [[ ! "${version}" =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)(-SNAPSHOT)?$ ]]; then
    die "invalid version '${version}': expected X.Y.Z or X.Y.Z-SNAPSHOT"
  fi
  printf '%s %s %s\n' "${BASH_REMATCH[1]}" "${BASH_REMATCH[2]}" "${BASH_REMATCH[3]}"
}

next_patch_version() {
  local major minor patch
  read -r major minor patch <<< "$(version_parse "$1")"
  printf '%s.%s.%s\n' "${major}" "${minor}" "$((patch + 1))"
}

previous_patch_version() {
  local major minor patch
  read -r major minor patch <<< "$(version_parse "$1")"
  if [ "${patch}" -eq 0 ]; then
    die "version $1 has no previous patch: it is a GA version"
  fi
  printf '%s.%s.%s\n' "${major}" "${minor}" "$((patch - 1))"
}

# Prints the major version of a release-X.0 branch name.
release_branch_major() {
  local branch="$1"
  if [[ ! "${branch}" =~ ^release-([0-9]+)\.0$ ]]; then
    die "invalid release branch '${branch}': expected release-X.0"
  fi
  printf '%s\n' "${BASH_REMATCH[1]}"
}

# Prints true when MAJOR is the highest major that has a GA tag vN.0.0,
# given the remote tags (one refs/tags/... per line, as printed by
# git ls-remote --tags). A release branch that only has release candidates
# so far does not count: until its GA ships, patches of the previous major
# are still the latest release.
is_highest_ga_major() {
  local major="$1" tags="$2" highest
  highest="$(printf '%s\n' "${tags}" | sed -nE 's#^refs/tags/v([0-9]+)\.0\.0$#\1#p' | sort -n | tail -1)"
  if [ -z "${highest}" ]; then
    die "no vN.0.0 tags found among the remote tags"
  fi
  if [ "${major}" -eq "${highest}" ]; then
    printf 'true\n'
  else
    printf 'false\n'
  fi
}

read_version_go() {
  local dir="$1" version
  version="$(sed -n 's/^const versionName = "\(.*\)"$/\1/p' "${dir}/go/vt/servenv/version.go")"
  if [ -z "${version}" ]; then
    die "could not find versionName in ${dir}/go/vt/servenv/version.go"
  fi
  printf '%s\n' "${version}"
}

# Rewrites only the versionName constant, leaving the header untouched so
# the diff is one line, as it is in every release commit so far.
update_version_go() {
  local dir="$1" version="$2"
  local file="${dir}/go/vt/servenv/version.go"
  version_parse "${version}" >/dev/null
  sed -i -E "s/^const versionName = \".*\"$/const versionName = \"${version}\"/" "${file}"
  if [ "$(read_version_go "${dir}")" != "${version}" ]; then
    die "failed to update ${file} to ${version}"
  fi
}

# Prints the version of io.vitess:vitess-parent, the first <version> after
# its <artifactId> in java/pom.xml.
read_pom_version() {
  local dir="$1" version
  version="$(sed -n '/<artifactId>vitess-parent<\/artifactId>/,/<version>/ s#.*<version>\(.*\)</version>.*#\1#p' "${dir}/java/pom.xml")"
  if [ -z "${version}" ]; then
    die "could not find the vitess-parent version in ${dir}/java/pom.xml"
  fi
  printf '%s\n' "${version}"
}

update_java() {
  local dir="$1" version="$2"
  version_parse "${version}" >/dev/null
  if ! command -v mvn >/dev/null; then
    die "mvn is required to update the Java poms"
  fi
  mvn -B -q -f "${dir}/java/pom.xml" "${VERSIONS_MAVEN_PLUGIN}:set" \
    -DnewVersion="${version}" -DgenerateBackupPoms=false
  if [ "$(read_pom_version "${dir}")" != "${version}" ]; then
    die "failed to update the Java poms to ${version}"
  fi
}

# Lists the example files whose image tags follow the release: the operator
# manifests and, on branches that still carry it, the compose example.
example_files() {
  local dir="$1"
  find "${dir}/examples/operator" -maxdepth 1 -name '*.yaml'
  if [ -d "${dir}/examples/compose" ]; then
    find "${dir}/examples/compose" -type f \( -name '*.yml' -o -name '*.yaml' -o -name '*.go' \)
  fi
}

# Points every vitess/lite and vitess/vtadmin image reference in the
# examples at vVERSION. A tag ends at whitespace or a quote, so the
# rewrite never eats the rest of a line.
update_examples() {
  local dir="$1" version="$2" tag_chars
  version_parse "${version}" >/dev/null
  tag_chars='[^[:space:]"'"'"'`]+'
  example_files "${dir}" | while IFS= read -r file; do
    sed -i -E "s#vitess/(lite|vtadmin):${tag_chars}#vitess/\\1:v${version}#g" "${file}"
  done
}

# Prints every path that the release commit must not touch and fails if
# there is one. The allowlist is the file set of every release commit so
# far, minus the code_freeze.yml flip that the automation no longer does.
check_release_paths() {
  local path bad=0
  for path in "$@"; do
    case "${path}" in
      changelog/*|examples/operator/*|examples/compose/*|go/vt/servenv/version.go|java/pom.xml|java/*/pom.xml) ;;
      *)
        printf '%s\n' "${path}"
        bad=1
        ;;
    esac
  done
  return "${bad}"
}

# Same for the back-to-dev commit, which only bumps versions.
check_back_to_dev_paths() {
  local path bad=0
  for path in "$@"; do
    case "${path}" in
      go/vt/servenv/version.go|java/pom.xml|java/*/pom.xml) ;;
      *)
        printf '%s\n' "${path}"
        bad=1
        ;;
    esac
  done
  return "${bad}"
}

# Drops the pull requests carrying the "Type: Release" label from a JSON
# array of pull requests with a labels field: the code freeze, release and
# back-to-dev PRs of the manual process are release machinery, not content.
without_release_prs() {
  jq -c '[.[] | select(any(.labels[]?; .name == "Type: Release") | not)]' <<< "$1"
}

# Compares the pull requests recorded in the milestone with the ones found
# behind the commits on the branch. Both are JSON arrays of objects with a
# number; the branch side may carry the milestone each PR actually has.
pr_set_diff() {
  local milestone="$1" branch="$2"
  jq -cn --argjson m "${milestone}" --argjson b "${branch}" '
    ($m | map(.number)) as $mn
    | ($b | map(.number)) as $bn
    | {
        only_in_milestone: [$m[] | select(.number as $n | $bn | index($n) | not)] | sort_by(.number),
        only_on_branch: [$b[] | select(.number as $n | $mn | index($n) | not)] | sort_by(.number),
        common: [$m[] | select(.number as $n | $bn | index($n))] | sort_by(.number)
      }
    | .matches = ((.only_in_milestone | length) == 0 and (.only_on_branch | length) == 0)'
}

# Prints the pull request numbers linked from a generated changelog.md,
# sorted and unique.
changelog_pr_numbers() {
  grep -oE '\[#[0-9]+\]\(https://github.com/vitessio/vitess/pull/[0-9]+\)' "$1" \
    | sed -E 's#.*/pull/([0-9]+)\)#\1#' | sort -n -u
}
