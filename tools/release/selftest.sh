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

# Self-test for the release tooling in tools/release. It exercises the pure
# helpers in lib.sh against a throwaway git repository built from the files
# of this checkout, so it needs no network access and no GitHub credentials
# beyond what Maven needs to download the versions plugin.
#
# Usage: tools/release/selftest.sh
# Set SELFTEST_SKIP_JAVA=1 to skip the Maven-backed checks where mvn is
# unavailable; the skip is reported, never silent.

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SELF_DIR}/../.." && pwd)"

# shellcheck source=tools/release/lib.sh
source "${SELF_DIR}/lib.sh"

failures=0
checks=0

pass() {
  checks=$((checks + 1))
}

fail() {
  checks=$((checks + 1))
  failures=$((failures + 1))
  echo "FAIL: $*" >&2
}

assert_eq() {
  local expected="$1" actual="$2" what="$3"
  if [ "${expected}" = "${actual}" ]; then
    pass
  else
    fail "${what}: expected '${expected}', got '${actual}'"
  fi
}

assert_fails() {
  local what="$1"
  shift
  if ( "$@" ) >/dev/null 2>&1; then
    fail "${what}: expected failure, but it succeeded"
  else
    pass
  fi
}

# Build a fixture repository holding copies of every file the release commit
# touches, plus a synthetic examples/compose tree because main no longer
# carries one while the supported release branches still do.
make_fixture() {
  local dir="$1"
  mkdir -p "${dir}"
  (
    cd "${REPO_ROOT}"
    mkdir -p "${dir}/go/vt/servenv" "${dir}/examples/compose/vtcompose"
    cp go/vt/servenv/version.go "${dir}/go/vt/servenv/version.go"
    cp -r examples/operator "${dir}/examples/operator"
    find java -name pom.xml | while IFS= read -r pom; do
      mkdir -p "${dir}/$(dirname "${pom}")"
      cp "${pom}" "${dir}/${pom}"
    done
  )
  cat > "${dir}/examples/compose/docker-compose.yml" <<'EOF'
services:
  vtctld:
    image: vitess/lite:v23.0.5
  vtgate:
    image: vitess/lite:${VITESS_TAG:-latest}
  vtadmin:
    image: vitess/vtadmin:latest
EOF
  cat > "${dir}/examples/compose/vtcompose/vtcompose.go" <<'EOF'
package main

const tmpl = `
    image: vitess/lite:v23.0.5
`
EOF
  (
    cd "${dir}"
    git init -q
    git config user.name "selftest"
    git config user.email "selftest@example.com"
    git add -A
    git commit -q -m "fixture"
  )
}

# Every vitess/lite and vitess/vtadmin reference in the example files that
# the release commit is allowed to touch.
example_image_tags() {
  local dir="$1"
  local dirs=("${dir}/examples/operator")
  if [ -d "${dir}/examples/compose" ]; then
    dirs+=("${dir}/examples/compose")
  fi
  grep -rhoE 'vitess/(lite|vtadmin):[^[:space:]"'"'"'`]+' --include='*.yaml' --include='*.yml' --include='*.go' "${dirs[@]}" \
    | sed -E 's#^vitess/(lite|vtadmin):##' | sort -u
}

FIXTURE="$(mktemp -d)"
trap 'rm -rf "${FIXTURE}"' EXIT
make_fixture "${FIXTURE}"

echo "== version helpers"
assert_eq "23 0 7" "$(version_parse 23.0.7-SNAPSHOT)" "version_parse strips -SNAPSHOT"
assert_eq "23 0 7" "$(version_parse 23.0.7)" "version_parse of a release version"
assert_fails "version_parse rejects a tag" version_parse v23.0.7
assert_fails "version_parse rejects an rc" version_parse 23.0.0-rc1
assert_fails "version_parse rejects garbage" version_parse "23.0.7; rm -rf /"
assert_eq "23.0.8" "$(next_patch_version 23.0.7)" "next_patch_version"
assert_eq "23.0.6" "$(previous_patch_version 23.0.7)" "previous_patch_version"
assert_fails "previous_patch_version rejects a GA version" previous_patch_version 23.0.0
assert_eq "23" "$(release_branch_major release-23.0)" "release_branch_major"
assert_fails "release_branch_major rejects main" release_branch_major main
assert_fails "release_branch_major rejects a minor branch" release_branch_major release-23.1

echo "== latest computation"
tags=$'refs/tags/v22.0.0\nrefs/tags/v23.0.0\nrefs/tags/v24.0.0\nrefs/tags/v24.0.3\nrefs/tags/v25.0.0-rc1\nrefs/tags/v9.0.0'
assert_eq "true" "$(is_highest_ga_major 24 "${tags}")" "24 is the highest GA while 25 only has a release candidate"
assert_eq "false" "$(is_highest_ga_major 25 "${tags}")" "25 is not latest before its GA"
assert_eq "false" "$(is_highest_ga_major 23 "${tags}")" "23 is not the highest"
assert_eq "false" "$(is_highest_ga_major 9 "${tags}")" "9 is not the highest (numeric, not lexical)"
tags+=$'\nrefs/tags/v25.0.0'
assert_eq "false" "$(is_highest_ga_major 24 "${tags}")" "24 is no longer latest once 25.0.0 ships"
assert_eq "true" "$(is_highest_ga_major 25 "${tags}")" "25 is latest once 25.0.0 ships"
assert_fails "is_highest_ga_major rejects a tag list without GA tags" is_highest_ga_major 24 "refs/tags/v25.0.0-rc1"

echo "== version.go"
main_version="$(read_version_go "${FIXTURE}")"
assert_eq "$(sed -n 's/^const versionName = "\(.*\)"$/\1/p' "${REPO_ROOT}/go/vt/servenv/version.go")" "${main_version}" "read_version_go matches the checkout"
update_version_go "${FIXTURE}" 23.0.7
assert_eq "23.0.7" "$(read_version_go "${FIXTURE}")" "update_version_go rewrites the constant"
assert_eq "go/vt/servenv/version.go" "$(git -C "${FIXTURE}" diff --name-only)" "update_version_go touches only version.go"
assert_eq "1" "$(git -C "${FIXTURE}" diff --numstat | awk '{print $1}')" "update_version_go changes one line"
update_version_go "${FIXTURE}" 23.0.8-SNAPSHOT
assert_eq "23.0.8-SNAPSHOT" "$(read_version_go "${FIXTURE}")" "update_version_go accepts a SNAPSHOT version"
git -C "${FIXTURE}" checkout -q -- .

echo "== examples"
update_examples "${FIXTURE}" 23.0.7
assert_eq "v23.0.7" "$(example_image_tags "${FIXTURE}")" "every lite/vtadmin reference in the examples reads v23.0.7"
assert_eq "" "$(git -C "${FIXTURE}" diff --name-only -- examples/operator/README.md examples/operator/operator.yaml)" "README.md and operator.yaml are untouched"
assert_eq "" "$(find "${FIXTURE}/examples" -name '*.bak')" "no backup files are left behind"
assert_eq "1" "$(grep -c 'planetscale/vitess-operator:latest' "${FIXTURE}/examples/operator/operator.yaml")" "the operator image is untouched"
changed="$(git -C "${FIXTURE}" diff --name-only | sort)"
expected_changed="$( (cd "${FIXTURE}" && for f in examples/operator/*.yaml; do [ "${f}" != examples/operator/operator.yaml ] && echo "${f}"; done; echo examples/compose/docker-compose.yml; echo examples/compose/vtcompose/vtcompose.go) | sort)"
assert_eq "${expected_changed}" "${changed}" "update_examples touches exactly the example files carrying image tags"
git -C "${FIXTURE}" checkout -q -- .

echo "== examples without a compose tree"
rm -r "${FIXTURE}/examples/compose"
update_examples "${FIXTURE}" 23.0.7
assert_eq "v23.0.7" "$(example_image_tags "${FIXTURE}")" "update_examples works when examples/compose is absent"
git -C "${FIXTURE}" checkout -q -- .

echo "== java"
if [ "${SELFTEST_SKIP_JAVA:-0}" = "1" ]; then
  echo "SKIP: SELFTEST_SKIP_JAVA=1, not exercising update_java and read_pom_version"
else
  assert_eq "${main_version}" "$(read_pom_version "${FIXTURE}")" "read_pom_version matches version.go on the checkout"
  update_java "${FIXTURE}" 23.0.7
  assert_eq "23.0.7" "$(read_pom_version "${FIXTURE}")" "update_java rewrites the parent version"
  for pom in java/pom.xml java/client/pom.xml java/example/pom.xml java/grpc-client/pom.xml java/jdbc/pom.xml; do
    assert_eq "1" "$(grep -c '<version>23.0.7</version>' "${FIXTURE}/${pom}")" "${pom} carries the new version exactly once"
  done
  assert_eq "$(printf '%s\n' java/client/pom.xml java/example/pom.xml java/grpc-client/pom.xml java/jdbc/pom.xml java/pom.xml)" \
    "$(git -C "${FIXTURE}" diff --name-only | sort)" "update_java touches exactly the five poms"
  assert_eq "" "$(find "${FIXTURE}/java" -name 'pom.xml.versionsBackup')" "update_java leaves no backup poms"
  git -C "${FIXTURE}" checkout -q -- .
fi

echo "== path allowlists"
assert_eq "" "$(check_release_paths changelog/23.0/23.0.7/changelog.md changelog/23.0/README.md examples/operator/101_initial_cluster.yaml examples/compose/docker-compose.yml go/vt/servenv/version.go java/pom.xml java/client/pom.xml)" "the release commit paths are allowed"
assert_eq ".github/workflows/code_freeze.yml" "$(check_release_paths go/vt/servenv/version.go .github/workflows/code_freeze.yml || true)" "a workflow file is reported"
assert_fails "check_release_paths fails on a disallowed path" check_release_paths go/vt/vtgate/vtgate.go
assert_eq "" "$(check_back_to_dev_paths go/vt/servenv/version.go java/pom.xml java/jdbc/pom.xml)" "the back-to-dev paths are allowed"
assert_fails "check_back_to_dev_paths rejects changelog changes" check_back_to_dev_paths changelog/23.0/README.md

echo "== pull request set reconciliation"
milestone='[{"number":1,"title":"a"},{"number":2,"title":"b"},{"number":4,"title":"d"}]'
branch='[{"number":2,"title":"b","milestone":"v1.0.1"},{"number":3,"title":"c","milestone":"v1.0.0"},{"number":4,"title":"d","milestone":"v1.0.1"}]'
assert_eq "1" "$(pr_set_diff "${milestone}" "${branch}" | jq -r '.only_in_milestone[].number')" "PR in the milestone but not on the branch"
assert_eq "3" "$(pr_set_diff "${milestone}" "${branch}" | jq -r '.only_on_branch[].number')" "PR on the branch but not in the milestone"
assert_eq "v1.0.0" "$(pr_set_diff "${milestone}" "${branch}" | jq -r '.only_on_branch[].milestone')" "the stray PR reports its actual milestone"
assert_eq "2 4" "$(pr_set_diff "${milestone}" "${branch}" | jq -r '[.common[].number] | join(" ")')" "common PRs"
assert_eq "true" "$(pr_set_diff "${milestone}" "${milestone}" | jq -r '.matches')" "identical sets match"
assert_eq "false" "$(pr_set_diff "${milestone}" "${branch}" | jq -r '.matches')" "different sets do not match"
release_prs='[{"number":9,"title":"Release of v1","labels":[{"name":"Type: Release"},{"name":"Component: General"}]},{"number":10,"title":"fix","labels":[{"name":"Type: Bug"}]}]'
assert_eq "10" "$(without_release_prs "${release_prs}" | jq -r '.[].number')" "Type: Release PRs are excluded"

echo "== changelog pull request numbers"
changelog="$(mktemp)"
cat > "${changelog}" <<'EOF'
# Changelog of Vitess v23.0.7
### Bug fixes
#### Query Serving
 * fix one [#20949](https://github.com/vitessio/vitess/pull/20949)
 * fix two [#20989](https://github.com/vitessio/vitess/pull/20989)
EOF
assert_eq "$(printf '20949\n20989')" "$(changelog_pr_numbers "${changelog}")" "changelog_pr_numbers lists the linked PRs"
rm -f "${changelog}"

echo
echo "${checks} checks, ${failures} failures"
if [ "${failures}" -ne 0 ]; then
  exit 1
fi
