#!/usr/bin/env bash
# Creates PRs for the users of Vitess.

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
    BRANCH=${2-origin/master}
    git fetch origin
    git checkout -B "$(whoami)-upgrade-vitess" $BRANCH
    echo "${GIT_COMMIT}" > "${FILE}"
    git add "${FILE}"
    git commit -m "Update to Vitess SHA ${GIT_COMMIT}"
    git push -f origin "$(whoami)-upgrade-vitess"
}

checkoutRepoWarning() {
  directory=$1
  repo_name=$2
  remote=$3

  tput bold
  tput setaf 1
  echo "Need $remote be checked out to $directory/$repo_name."
  echo "Run the follow line:"
  echo "mkdir -p $directory && cd $directory && git clone $remote"

  exit 1
}

urlencode () {
  echo $(node -e "console.log(encodeURIComponent(process.argv[1]))" -- "$1")
}

GIT_COMMIT=$(git rev-parse HEAD)

cd ~/Development/java/ || checkoutRepoWarning ~/Development java ssh://git@git.sqcorp.co/sq/java.git
checkGitIsClean
createUpgradeCommit integration-testing/src/main/resources/vitess.sha origin/all-green

cd ~/Development/go/src/git.sqcorp.co/ods/vttablet || checkoutRepoWarning ~/Development/go/src/git.sqcorp.co/ods vttablet ssh://git@git.sqcorp.co/ods/vttablet.git
checkGitIsClean
createUpgradeCommit script/vitess.git-sha

cd ~/Development/go/src/square/up || checkoutRepoWarning ~/Development/go/src/square up ssh://git@git.sqcorp.co/go/square.git
checkGitIsClean
createUpgradeCommit vitess/script/vitess.git-sha origin/green

prTitle=$(urlencode "Test Vitess update $GIT_COMMIT")
prDescription=$(urlencode "Do not merge")

bitbucketAction="pull-requests?create"

tput reset
tput setaf 2
echo Great success!
echo
tput setaf 3
echo "Once the Vitess build goes green, you can open these two PRs:"
tput setaf 4
echo "https://git.sqcorp.co/projects/SQ/repos/java/$bitbucketAction&sourceBranch=refs/heads/$(whoami)-upgrade-vitess&title=$prTitle&description=$prDescription"
echo "https://git.sqcorp.co/projects/GO/repos/square/$bitbucketAction&sourceBranch=refs/heads/$(whoami)-upgrade-vitess&title=$prTitle&description=$prDescription"
echo
tput setaf 3
echo "Once those two are green, we know that this rebase has not broken any integration tests in Franklin"
echo "If the intention is to deploy a new version of Vitess for Franklin, we need to open a PR against ODSs vttablet project"
tput setaf 4
echo "https://git.sqcorp.co/projects/ODS/repos/vttablet/$bitbucketAction&sourceBranch=refs/heads/$(whoami)-upgrade-vitess&title=$prTitle&description=$prDescription"
