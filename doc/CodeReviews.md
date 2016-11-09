# Code Reviews

Every GitHub pull request must go through a code review and approved before it will be merged into the master branch.

## Assigning a Pull Request

If you want to address your review to a particular set of teammates, add them as Assignee (righthand side on the pull request).
They'll receive an email.

During discussions, you can also refer to somebody using the *@username* syntax and they'll receive an email as well.

If you want to receive notifications even when you aren't mentioned, you can go to the [repository page](https://github.com/youtube/vitess) and click *Watch*.

## Approving a Pull Request

As a reviewer you can approve a pull request through two ways:

* Approve the pull request via GitHub's new code review system
* reply with a comment that contains the word *LGTM*  (Looks Good To Me)

## Merging a Pull Request

Pull requests can be merged after they were approved and the Travis tests have passed.
External contributions will be merged by a team member.
Internal team members can merge their **own** pull requests.

## What reviewers should look for

Here are a few questions reviewers need to answer.
As the author of the pull request, you should also check for these before creating the pull request.

*   Does this change match an existing design / bug?
*   Is there proper unit test coverage for this change? All changes should
    increase coverage. We need at least integration test coverage when unit test
    coverage is not possible.
*   Is this change going to log too much? (Error logs should only happen when
    the component is in bad shape, not because of bad transient state or bad
    user queries)
*   Does this change match our coding conventions / style? Linter was run and is
    happy?
*   Does this match our current patterns? Example include RPC patterns,
    Retries / Waits / Timeouts patterns using Context, ...

## Internal Bug Numbers

Most of the bugs the team is working on are tracked internally.
We reference to them as `b/########` or `B=########` in commit messages and comments.
External users can ignore these.
