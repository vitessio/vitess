---
name: resolve-backport
description: Resolve merge conflicts in Vitess backport PRs. Use when asked to resolve a backport, fix backport conflicts, or given a backport PR URL/number.
---

# Resolve Backport Conflicts

Follow these steps precisely to resolve merge conflicts in a Vitess backport PR.

## Step 1: Parse input and validate

- Accept a PR URL or number. The repo is `vitessio/vitess`.
- Fetch PR metadata: `gh pr view <number> --repo vitessio/vitess --json number,title,body,labels,headRefName,baseRefName,isDraft,assignees`
- Validate the PR has a `Backport` label. If not, **stop and ask the user** what they'd like to do — this skill is designed for backport PRs and should not auto-merge non-backport PRs.
- Check assignees:
  - If the PR is unassigned, continue.
  - If the PR is assigned to the current user (`gh api user --jq .login`), continue.
  - If the PR is assigned to someone else, **ask the user** if they still want to proceed (someone else may already be working on it). Stop if the user says no.
- Determine if the PR has conflicts. Don't rely solely on the `Merge Conflict` label — it may have been removed prematurely. Check the actual mergeable state:
  ```
  gh pr view <number> --repo vitessio/vitess --json mergeable --jq .mergeable
  ```
  If the result is `CONFLICTING`, or if the PR has a `Merge Conflict` label, treat it as conflicting.
- If the PR has no assignees, assign it to yourself early to signal you're working on it:
  ```
  gh pr edit <number> --repo vitessio/vitess --add-assignee "@me"
  ```
- If the PR has conflicts, extract the upstream PR number. Vitess backport PRs use specific patterns:
  - Body contains: `Backport of #<number>` or `backport of #<number>`
  - Title suffix: `(#<number>)`
  Parse from these patterns specifically — do not use a broad `#(\d+)` regex, as the body may reference unrelated issues. If the upstream PR number cannot be found unambiguously, **ask the user** to provide it.
  Continue to Step 2.
- If the PR has no conflicts, skip to the **No conflicts** flow below.

### No conflicts flow
1. Poll all CI checks every 60 seconds until all checks complete. For any failed check, follow the **Handling CI failures** process below.
3. Once all CI passes:
   a. If the PR is a draft, mark as ready: `gh pr ready <number> --repo vitessio/vitess`
   b. Enable auto-merge: `gh pr merge <number> --repo vitessio/vitess --squash --auto`
4. Report completion and stop — do not continue to Step 2.

## Step 2: Gather context

- Fetch the upstream PR diff — this is the source of truth for what the changes should look like:
  ```
  gh pr diff <upstream-number> --repo vitessio/vitess
  ```
- Fetch the upstream PR file list to know which files should be changed:
  ```
  gh pr view <upstream-number> --repo vitessio/vitess --json files
  ```
- Check out the backport branch in a worktree to isolate the work from the main checkout:
  ```
  git fetch origin <headRefName>
  git worktree add /tmp/backport-<number> origin/<headRefName>
  cd /tmp/backport-<number>
  ```

## Step 3: Rebase onto latest base branch

The backport branch may be based on a stale version of the release branch. **Always rebase before resolving conflicts** to avoid creating a resolution that conflicts with the current base.

- Determine the base branch from the PR metadata `baseRefName` (e.g., `release-22.0`).
- Detect the remote pointing to `vitessio/vitess`. Do not assume it is named `upstream`:
  ```
  git remote -v | grep 'vitessio/vitess.*fetch'
  ```
  Use the matching remote name (commonly `upstream` or `origin`) for all subsequent fetch/rebase commands. If no remote points to `vitessio/vitess`, **ask the user** which remote to use.
- Fetch and rebase:
  ```
  git fetch <remote> <base-branch>
  git rebase <remote>/<base-branch>
  ```
- If the rebase itself has conflicts, resolve them as part of Step 5 below (they will be the same conflicts you'd resolve anyway, but now against the correct base).
- If the rebase has no conflicts, continue to the next step.

## Step 4: Ensure correct Go version

- Read `go.mod` on the backport branch to determine the required Go version (e.g., `go 1.24.13` means minor version `1.24`).
- Check local Go version with `go version`.
- If the minor version doesn't match, try these in order:
  1. **govm**: Check if `govm` is installed (`which govm`). If so, use it to switch:
     ```
     govm use <version>
     ```
     For example, if `go.mod` requires `go 1.24.13`, run `govm use 1.24.13`.
  2. **gvm**: Check if `gvm` is installed (`which gvm`). If so, use it to switch:
     ```
     gvm use go<version>
     ```
     For example, if `go.mod` requires `go 1.24.13`, run `gvm use go1.24.13`.
  3. **Homebrew** (fallback): Check if Homebrew has the right version:
     ```
     ls /opt/homebrew/opt/go@<minor>/bin/go
     ```
     If found, prepend it to PATH for all subsequent commands:
     ```
     export PATH="/opt/homebrew/opt/go@<minor>/bin:$PATH"
     ```
  4. If none are available, warn the user and continue with the available Go version.

## Step 5: Resolve conflicts

- Find all files containing conflict markers (`<<<<<<<`, `=======`, `>>>>>>>`).
- For each conflicting file:
  - Use the upstream PR diff as a guide to determine the intended changes.
  - Resolve the conflict to match what the upstream PR intended, adapted to the release branch context.
  - If a conflict is ambiguous and you cannot confidently determine the correct resolution, **ask the user** for guidance before proceeding.
- Discard changes to files that are NOT in the upstream PR's file list.
- **Missing dependencies**: If the conflicts exist because the upstream PR depends on changes from another PR that hasn't been backported yet, or if backporting an additional PR would make the resolution significantly cleaner, **propose this to the user** rather than forcing a messy resolution. For example: "This PR depends on types introduced in #12345 which hasn't been backported to release-22.0 yet. Should we backport that first?"

## Step 6: Verify file scope

- Compare the set of modified files (from `git diff --name-only`) against the upstream PR's file list.
- Any file that is changed but NOT in the upstream PR's file list should be reverted with `git checkout HEAD -- <file>`.
- **File addition mismatch**: Check if any file that was a *modification* in the upstream PR appears as a *new file addition* on the backport branch (i.e., the file doesn't exist on the base release branch). This usually means the file was introduced by a prerequisite PR that hasn't been backported yet. **Ask the user** before proceeding — they may want to backport the prerequisite PR first or skip the file entirely.

## Step 7: Run unit tests

- Identify the Go packages containing changed files.
- Run unit tests on those packages:
  ```
  source build.env && go test -v <changed-packages>
  ```
- If tests fail, investigate and fix. Ask the user if the fix is non-obvious.

## Step 8: Build

- Run the Vitess build. If any files in `web/vtadmin/` were changed, include the VTAdmin build:
  ```
  source build.env && make build
  ```
  Otherwise skip the VTAdmin build:
  ```
  source build.env && NOVTADMINBUILD=1 make build
  ```
- If the build fails, investigate and fix.

## Step 9: Format and lint

- Run formatters on changed `.go` files:
  ```
  gofumpt -w <changed-go-files>
  goimports -local "vitess.io/vitess" -w <changed-go-files>
  ```
- Run linter on affected Go packages:
  ```
  golangci-lint run <changed-packages>
  ```
  Note: different release branches may use different `golangci-lint` versions. If you get unexpected linter results, check the branch's expected version in CI config and consider using that version instead.
- If any `.sh` files were changed, run shellcheck:
  ```
  shellcheck <changed-sh-files>
  ```
- If `go.mod` was changed, run `go mod tidy` and include `go.sum` in the commit.
- If any files in `proto/` were changed, regenerate protobuf code and include the generated files:
  ```
  make proto
  make vtadmin_web_proto_types
  git add web/ go/vt/proto/
  ```
- Fix any issues found.

## Step 10: Ship

- Remove conflict labels **before pushing** (so the push doesn't trigger CI with Skip CI still set):
  ```
  gh pr edit <number> --repo vitessio/vitess --remove-label "Skip CI" --remove-label "Merge Conflict"
  ```
- Stage resolved files: `git add <resolved-files>`
- Commit with signoff:
  ```
  git commit --signoff -m "resolve conflicts for backport of #<upstream-number>"
  ```
- **Ask the user** before pushing. Force push is expected after rebase, so prompt with something like: "Ready to force-push the resolved branch. Shall I proceed?" Only push after the user confirms:
  ```
  git push --force-with-lease
  ```
- Add a PR comment summarizing the conflict resolution. Include which files had conflicts, how they were resolved, and any notable decisions (e.g., keeping functions from prior backports, adapting code to the release branch context):
  ```
  gh pr comment <number> --repo vitessio/vitess --body "<summary>"
  ```
- Report progress to the user with a link to the PR.
- Poll all CI checks every 60 seconds until all checks complete. If we just pushed, wait at least 4 minutes before starting to poll (CI needs time to start). For any failed check, follow the **Handling CI failures** process below.
- Once all CI passes:
  - Mark PR as ready for review:
    ```
    gh pr ready <number> --repo vitessio/vitess
    ```
  - Enable auto-merge (squash):
    ```
    gh pr merge <number> --repo vitessio/vitess --squash --auto
    ```
- If we created a git worktree in Step 2, clean it up:
  ```
  cd <original-directory>
  git worktree remove <worktree-path>
  ```

## Handling CI failures

This applies to **both** the No conflicts flow and the conflict resolution flow. After enabling auto-merge, monitor all CI checks — not just Static Code Checks.

A backport PR only exists because CI passed on the upstream PR. Any CI failure on the backport is a strong signal that our changes introduced a problem.

1. Poll all CI checks with `gh pr checks <number> --repo vitessio/vitess` every 60 seconds.
2. **On each poll**, check for any newly failed checks. Handle failures as soon as they appear — don't wait for all checks to complete.
3. For each failed check, **always investigate the failure first** before deciding to rerun:
   a. Check the CI logs to understand why it failed.
   b. Check if the upstream PR had the same failure — a discrepancy is unexpected and points to a problem with the backport.
   c. Check the recent success rate of the failed job on the target branch to assess flakiness:
      ```
      # Get the e2e workflow ID
      workflow_id=$(gh api repos/vitessio/vitess/actions/workflows --jq '.workflows[] | select(.name | test("Cluster")) | .id' | head -1)
      # Check last 10 runs for the specific job
      pass=0; fail=0; total=0
      while IFS= read -r rid; do
        conclusion=$(gh run view "$rid" --repo vitessio/vitess --json jobs \
          --jq '.jobs[] | select(.name == "<job-name>") | .conclusion' 2>/dev/null | head -1)
        if [ -n "$conclusion" ] && [ "$conclusion" != "null" ] && [ "$conclusion" != "cancelled" ]; then
          total=$((total + 1))
          if [ "$conclusion" = "success" ]; then pass=$((pass + 1)); else fail=$((fail + 1)); fi
        fi
      done < <(gh api "repos/vitessio/vitess/actions/workflows/$workflow_id/runs?branch=<base-branch>&per_page=10" --jq '.workflow_runs[].id')
      ```
      Report the success rate to the user (e.g., "vtgate_reservedconn: 7/10 passed recently on release-22.0").
   d. If the failure is unrelated to the backport (e.g., flaky test, infra issue), rerun the failed CI job and continue polling.
   e. If the backport caused the failure, propose and apply fixes, push, then continue polling to verify the fix resolved it.
4. Continue polling until all checks have passed (including reruns).
