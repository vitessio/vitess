#!/usr/bin/env bash
# Creates PRs for the users of Vitess.
set -euxo pipefail

checkGitIsClean () {
	if [[ -z "$(git status --porcelain)" ]]; then
	    echo "$(pwd) is clean"
	else
	    echo "$(pwd) not clean - please fix first"
	    exit 1
	fi
} 

createUpgradeCommit() {
    FILE=$1
    git fetch origin
    git checkout -B "$(whoami)-upgrade-vitess" origin/master
    echo "${GIT_COMMIT}" > "${FILE}"
    git add "${FILE}"
    git commit -m "Update to Vitess SHA ${GIT_COMMIT}"
    git push -f origin "$(whoami)-upgrade-vitess"
}

GIT_COMMIT=$(git rev-parse HEAD)

cd ~/Development/java/
checkGitIsClean
createUpgradeCommit integration-testing/src/main/resources/vitess.sha

cd ~/Development/go/src/git.sqcorp.co/ods/vttablet
checkGitIsClean
createUpgradeCommit script/vitess.git-sha

cd ~/Development/go/src/square/up
checkGitIsClean
createUpgradeCommit vitess/script/vitess.git-sha
