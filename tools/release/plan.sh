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

# Plans a patch release: validates the branch, the commit and the version,
# and reconciles the milestone against the commits on the branch. It reads
# from git and the GitHub API and writes nothing anywhere but OUT_DIR, so
# it is safe to run locally against the real repository at any time.
#
# Environment:
#   RELEASE_BRANCH  required, release-X.0
#   OUT_DIR         required, receives plan.json and plan.md
#   SHA             optional, must be the current tip of RELEASE_BRANCH
#   ALLOW_EMPTY     true to allow a release without any pull request
#   REMOTE          git remote of the vitessio/vitess repository (origin)
#   REPO            GitHub repository (vitessio/vitess)
#
# Outputs (to $GITHUB_OUTPUT under Actions, echoed otherwise): version, tag,
# go_tag, next_version, base_sha, prev_tag, latest, milestone_number,
# summary_path.

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/release/lib.sh
source "${SELF_DIR}/lib.sh"

RELEASE_BRANCH="${RELEASE_BRANCH:?RELEASE_BRANCH is required}"
OUT_DIR="${OUT_DIR:?OUT_DIR is required}"
SHA="${SHA:-}"
ALLOW_EMPTY="${ALLOW_EMPTY:-false}"
REMOTE="${REMOTE:-origin}"
REPO="${REPO:-vitessio/vitess}"

mkdir -p "${OUT_DIR}"

major="$(release_branch_major "${RELEASE_BRANCH}")"

log "== resolving the base commit of ${RELEASE_BRANCH}"
tip="$(git ls-remote --heads "${REMOTE}" "refs/heads/${RELEASE_BRANCH}" | awk '{print $1}')"
if [ -z "${tip}" ]; then
  die "branch ${RELEASE_BRANCH} does not exist on ${REMOTE}"
fi
if [ -n "${SHA}" ]; then
  if [[ ! "${SHA}" =~ ^[0-9a-f]{40}$ ]]; then
    die "SHA must be a full 40 character commit hash, got '${SHA}'"
  fi
  if [ "${SHA}" != "${tip}" ]; then
    die "${RELEASE_BRANCH} moved since it was vetted: tip is ${tip}, expected ${SHA}. Re-check the branch and dispatch again with the new SHA."
  fi
fi
base_sha="${tip}"
if ! git cat-file -e "${base_sha}^{commit}" 2>/dev/null; then
  git fetch -q "${REMOTE}" "${base_sha}"
fi
log "base commit: ${base_sha}"

log "== deriving the version from version.go at ${base_sha}"
snapshot="$(git show "${base_sha}:go/vt/servenv/version.go" | sed -n 's/^const versionName = "\(.*\)"$/\1/p')"
if [[ ! "${snapshot}" =~ -SNAPSHOT$ ]]; then
  die "version.go at ${base_sha} reads '${snapshot}', expected an X.Y.Z-SNAPSHOT version: the branch is not in development mode"
fi
version="${snapshot%-SNAPSHOT}"
read -r version_major _ version_patch <<< "$(version_parse "${version}")"
if [ "${version_major}" != "${major}" ]; then
  die "version.go reads ${snapshot} but the branch is ${RELEASE_BRANCH}"
fi
if [ "${version_patch}" -eq 0 ]; then
  die "version.go reads ${snapshot}: this workflow only cuts patch releases, RC and GA releases still go through vitess-releaser"
fi
for pom in java/pom.xml java/client/pom.xml java/example/pom.xml java/grpc-client/pom.xml java/jdbc/pom.xml; do
  if ! git show "${base_sha}:${pom}" | grep -q "<version>${snapshot}</version>"; then
    die "${pom} at ${base_sha} does not carry ${snapshot}: the Java poms and version.go disagree"
  fi
done
tag="v${version}"
go_tag="v0.${major}.${version_patch}"
next_version="$(next_patch_version "${version}")"
prev_tag="v$(previous_patch_version "${version}")"
log "version ${version}, tags ${tag} and ${go_tag}, next ${next_version}-SNAPSHOT, previous ${prev_tag}"

log "== checking the tags"
existing="$(git ls-remote --tags "${REMOTE}" "refs/tags/${tag}" "refs/tags/${go_tag}" | awk '{print $2 " at " $1}')"
if [ -n "${existing}" ]; then
  die "tags already exist on ${REMOTE}: ${existing}"
fi
prev_sha="$(git ls-remote --tags "${REMOTE}" "refs/tags/${prev_tag}" | awk '{print $1}')"
if [ -z "${prev_sha}" ]; then
  die "the previous tag ${prev_tag} does not exist on ${REMOTE}"
fi
if ! git cat-file -e "${prev_sha}^{commit}" 2>/dev/null; then
  git fetch -q "${REMOTE}" "refs/tags/${prev_tag}:refs/tags/${prev_tag}"
fi
if ! git merge-base --is-ancestor "${prev_sha}" "${base_sha}"; then
  die "${prev_tag} (${prev_sha}) is not an ancestor of ${base_sha}"
fi

log "== computing latest"
latest="$(is_highest_ga_major "${major}" "$(git ls-remote --tags "${REMOTE}" 'refs/tags/v*.0.0' | awk '{print $2}')")"
log "latest: ${latest}"

summary_path=""
if git cat-file -e "${base_sha}:changelog/${major}.0/${version}/summary.md" 2>/dev/null; then
  summary_path="changelog/${major}.0/${version}/summary.md"
  log "release summary: ${summary_path}"
fi

log "== reconciling milestone ${tag} with ${prev_tag}..${base_sha}"
milestone_number="$(gh api "repos/${REPO}/milestones?state=all&per_page=100" --paginate \
  --jq ".[] | select(.title == \"${tag}\") | .number")"
if [ -z "${milestone_number}" ]; then
  die "milestone ${tag} does not exist in ${REPO}: create it, assign the pull requests of this patch to it and dispatch again"
fi
milestone_prs_all="$(gh api "repos/${REPO}/issues?milestone=${milestone_number}&state=closed&per_page=100" --paginate \
  --jq '.[] | select(.pull_request != null and .pull_request.merged_at != null) | {number, title, labels: [.labels[] | {name}]}' \
  | jq -cs 'sort_by(.number)')"
milestone_prs="$(without_release_prs "${milestone_prs_all}")"

branch_prs_all="$(
  git rev-list "${prev_sha}..${base_sha}" | while IFS= read -r commit; do
    gh api "repos/${REPO}/commits/${commit}/pulls" \
      --jq ".[] | select(.merged_at != null and .base.ref == \"${RELEASE_BRANCH}\") | {number, title, milestone: (.milestone.title // \"none\"), labels: [.labels[] | {name}]}"
  done | jq -cs 'unique_by(.number) | sort_by(.number)'
)"
branch_prs="$(without_release_prs "${branch_prs_all}")"
diff="$(pr_set_diff "${milestone_prs}" "${branch_prs}")"

{
  echo "## Release plan for ${tag}"
  echo
  echo "| | |"
  echo "|---|---|"
  echo "| Branch | \`${RELEASE_BRANCH}\` at \`${base_sha}\` |"
  echo "| Version | ${version} (tags \`${tag}\`, \`${go_tag}\`), then ${next_version}-SNAPSHOT |"
  echo "| Previous tag | \`${prev_tag}\` |"
  echo "| Latest release | ${latest} |"
  echo "| Milestone | [${tag}](https://github.com/${REPO}/milestone/${milestone_number}) |"
  echo "| Release summary | ${summary_path:-none} |"
  echo
  echo "### Pull requests"
  echo
  echo "| PR | Title | In milestone | On branch |"
  echo "|---|---|---|---|"
  jq -r --arg repo "${REPO}" '
    (.common[] | "| [#\(.number)](https://github.com/\($repo)/pull/\(.number)) | \(.title) | yes | yes |"),
    (.only_in_milestone[] | "| [#\(.number)](https://github.com/\($repo)/pull/\(.number)) | \(.title) | yes | **no** |"),
    (.only_on_branch[] | "| [#\(.number)](https://github.com/\($repo)/pull/\(.number)) | \(.title) | **no** (milestone: \(.milestone)) | yes |")' <<< "${diff}"
  echo
  if [ "$(jq -r '.matches' <<< "${diff}")" = "true" ]; then
    echo "The milestone and the branch agree on $(jq -r '.common | length' <<< "${diff}") pull requests."
  else
    echo "**The milestone and the branch disagree.** Fix the milestones of the pull requests marked in bold, then dispatch again."
  fi
} > "${OUT_DIR}/plan.md"
gh_step_summary "${OUT_DIR}/plan.md"
cat "${OUT_DIR}/plan.md" >&2

if [ "$(jq -r '.matches' <<< "${diff}")" != "true" ]; then
  die "milestone ${tag} does not match the pull requests merged on ${RELEASE_BRANCH} since ${prev_tag}, see the table above"
fi
pr_count="$(jq -r '.common | length' <<< "${diff}")"
if [ "${pr_count}" -eq 0 ] && [ "${ALLOW_EMPTY}" != "true" ]; then
  die "no pull requests were merged on ${RELEASE_BRANCH} since ${prev_tag}: nothing to release (set allow_empty to release anyway)"
fi

jq -n \
  --arg release_branch "${RELEASE_BRANCH}" \
  --arg base_sha "${base_sha}" \
  --arg version "${version}" \
  --arg tag "${tag}" \
  --arg go_tag "${go_tag}" \
  --arg next_version "${next_version}" \
  --arg prev_tag "${prev_tag}" \
  --arg prev_sha "${prev_sha}" \
  --argjson latest "${latest}" \
  --argjson milestone_number "${milestone_number}" \
  --arg summary_path "${summary_path}" \
  --argjson milestone_prs "${milestone_prs_all}" \
  --argjson branch_prs "${branch_prs_all}" \
  '{
    release_branch: $release_branch, base_sha: $base_sha, version: $version, tag: $tag, go_tag: $go_tag,
    next_version: $next_version, prev_tag: $prev_tag, prev_sha: $prev_sha, latest: $latest,
    milestone_number: $milestone_number, summary_path: $summary_path,
    milestone_prs: $milestone_prs, branch_prs: $branch_prs
  }' > "${OUT_DIR}/plan.json"

gh_output version "${version}"
gh_output tag "${tag}"
gh_output go_tag "${go_tag}"
gh_output next_version "${next_version}"
gh_output base_sha "${base_sha}"
gh_output prev_tag "${prev_tag}"
gh_output latest "${latest}"
gh_output milestone_number "${milestone_number}"
gh_output summary_path "${summary_path}"
