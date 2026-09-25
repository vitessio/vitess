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

# Prepares the two commits of a patch release on top of the planned base
# commit: the release commit (release notes, version.go, poms, examples)
# and the back-to-dev commit (next SNAPSHOT version). Both land in a git
# bundle under OUT_DIR; nothing is pushed.
#
# Run from a clean checkout of the planned base commit. Commit dates are
# pinned to COMMIT_DATE so that running this twice with the same inputs
# yields the same commit hashes. Commits bypass local git hooks: the
# content is validated here, and the workflow runs without any hooks.
#
# Environment:
#   PLAN_DIR           required, directory holding plan.json from plan.sh
#   OUT_DIR            required, receives the bundle, notes and summary
#   GIT_AUTHOR_NAME    required, identity of the release commits
#   GIT_AUTHOR_EMAIL   required
#   COMMIT_DATE        optional, ISO 8601 date of both commits (now)
#   REPO               GitHub repository (vitessio/vitess)
#
# Outputs: release_sha, back_to_dev_sha.

set -euo pipefail

SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=tools/release/lib.sh
source "${SELF_DIR}/lib.sh"

PLAN_DIR="${PLAN_DIR:?PLAN_DIR is required}"
OUT_DIR="${OUT_DIR:?OUT_DIR is required}"
: "${GIT_AUTHOR_NAME:?GIT_AUTHOR_NAME is required}"
: "${GIT_AUTHOR_EMAIL:?GIT_AUTHOR_EMAIL is required}"
COMMIT_DATE="${COMMIT_DATE:-$(date -u +%Y-%m-%dT%H:%M:%SZ)}"
REPO="${REPO:-vitessio/vitess}"

export GIT_COMMITTER_NAME="${GIT_COMMITTER_NAME:-${GIT_AUTHOR_NAME}}"
export GIT_COMMITTER_EMAIL="${GIT_COMMITTER_EMAIL:-${GIT_AUTHOR_EMAIL}}"
export GIT_AUTHOR_DATE="${COMMIT_DATE}"
export GIT_COMMITTER_DATE="${COMMIT_DATE}"
# The release notes tool resolves the repository from the git remotes of
# the working directory; pin it so a local run cannot pick a fork.
export GH_REPO="${REPO}"

plan="${PLAN_DIR}/plan.json"
if [ ! -f "${plan}" ]; then
  die "${plan} not found: run plan.sh first"
fi
release_branch="$(jq -r '.release_branch' "${plan}")"
base_sha="$(jq -r '.base_sha' "${plan}")"
version="$(jq -r '.version' "${plan}")"
tag="$(jq -r '.tag' "${plan}")"
next_version="$(jq -r '.next_version' "${plan}")"
summary_path="$(jq -r '.summary_path' "${plan}")"
major="$(release_branch_major "${release_branch}")"
notes_dir="changelog/${major}.0/${version}"

mkdir -p "${OUT_DIR}"

log "== checking the working tree"
if [ "$(git rev-parse HEAD)" != "${base_sha}" ]; then
  die "HEAD is $(git rev-parse HEAD), expected the planned base commit ${base_sha}"
fi
if [ -n "$(git status --porcelain --untracked-files=all)" ]; then
  die "the working tree is not clean"
fi
if [ "$(read_version_go .)" != "${version}-SNAPSHOT" ]; then
  die "version.go reads $(read_version_go .), expected ${version}-SNAPSHOT"
fi
log "base ${base_sha}, releasing ${version} on ${release_branch}, commits dated ${COMMIT_DATE}"

log "== generating the release notes for ${tag}"
notes_args=(--version "${tag}")
if [ -n "${summary_path}" ]; then
  notes_args+=(--summary "${summary_path}")
fi
go run ./go/tools/release-notes "${notes_args[@]}"
go run ./go/tools/releases/releases.go
# The notes become the body of the GitHub release, where the changelog link
# must resolve before the notes reach main; the tag is immutable, main is
# not, so point the link at the tag.
sed -i "s#/blob/main/#/blob/${tag}/#g" "${notes_dir}/release_notes.md"

expected_prs="$(jq -r '.milestone_prs[].number' "${plan}" | sort -n -u)"
actual_prs="$(changelog_pr_numbers "${notes_dir}/changelog.md")"
if [ "${expected_prs}" != "${actual_prs}" ]; then
  log "pull requests in the milestone: $(tr '\n' ' ' <<< "${expected_prs}")"
  log "pull requests in the changelog: $(tr '\n' ' ' <<< "${actual_prs}")"
  die "the generated changelog does not list the pull requests the plan reconciled"
fi
log "the changelog lists all $(wc -l <<< "${expected_prs}" | tr -d ' ') pull requests of the milestone"

log "== updating the version to ${version}"
update_examples . "${version}"
update_version_go . "${version}"
update_java . "${version}"
go build ./go/vt/servenv/

git add -A -- changelog examples go/vt/servenv/version.go java
changed="$(git status --porcelain --untracked-files=all | awk '{print $NF}')"
if [ -z "${changed}" ]; then
  die "the release commit would be empty"
fi
# shellcheck disable=SC2086
if ! disallowed="$(check_release_paths ${changed})"; then
  die "the release commit touches files outside the allowlist: $(tr '\n' ' ' <<< "${disallowed}")"
fi
git commit -q -s --no-verify -m "[${release_branch}] Release of \`${tag}\`"
release_sha="$(git rev-parse HEAD)"
log "release commit ${release_sha}"

log "== bumping to ${next_version}-SNAPSHOT"
update_version_go . "${next_version}-SNAPSHOT"
update_java . "${next_version}-SNAPSHOT"
git add -A -- go/vt/servenv/version.go java
changed="$(git status --porcelain --untracked-files=all | awk '{print $NF}')"
# shellcheck disable=SC2086
if ! disallowed="$(check_back_to_dev_paths ${changed})"; then
  die "the back-to-dev commit touches files outside the allowlist: $(tr '\n' ' ' <<< "${disallowed}")"
fi
git commit -q -s --no-verify -m "[${release_branch}] Bump to \`v${next_version}-SNAPSHOT\` after the \`${tag}\` release"
back_to_dev_sha="$(git rev-parse HEAD)"
log "back-to-dev commit ${back_to_dev_sha}"

log "== bundling"
git update-ref refs/release/release "${release_sha}"
git update-ref refs/release/back-to-dev "${back_to_dev_sha}"
git bundle create "${OUT_DIR}/release.bundle" "^${base_sha}" refs/release/release refs/release/back-to-dev
git bundle verify "${OUT_DIR}/release.bundle"
cp "${notes_dir}/release_notes.md" "${notes_dir}/changelog.md" "${OUT_DIR}/"
git diff "${base_sha}" "${release_sha}" > "${OUT_DIR}/release.diff"
git diff "${release_sha}" "${back_to_dev_sha}" > "${OUT_DIR}/back-to-dev.diff"

jq -n \
  --arg release_sha "${release_sha}" \
  --arg back_to_dev_sha "${back_to_dev_sha}" \
  --arg commit_date "${COMMIT_DATE}" \
  --arg author "${GIT_AUTHOR_NAME} <${GIT_AUTHOR_EMAIL}>" \
  '{release_sha: $release_sha, back_to_dev_sha: $back_to_dev_sha, commit_date: $commit_date, author: $author}' \
  > "${OUT_DIR}/prep.json"

{
  echo "## Release commits for ${tag}"
  echo
  echo "| Commit | Subject |"
  echo "|---|---|"
  git log --reverse --format="| \`%H\` | %s |" "${base_sha}..${back_to_dev_sha}"
  echo
  echo "Authored by ${GIT_AUTHOR_NAME} on ${COMMIT_DATE}."
  echo
  echo "### Release commit"
  echo
  echo '```'
  git diff --stat "${base_sha}" "${release_sha}"
  echo '```'
  echo
  echo "### Back-to-dev commit"
  echo
  echo '```'
  git diff --stat "${release_sha}" "${back_to_dev_sha}"
  echo '```'
  echo
  echo "### Release notes"
  echo
  cat "${notes_dir}/release_notes.md"
} > "${OUT_DIR}/prep.md"
gh_step_summary "${OUT_DIR}/prep.md"

gh_output release_sha "${release_sha}"
gh_output back_to_dev_sha "${back_to_dev_sha}"
