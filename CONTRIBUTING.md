# Contributing to Vitess

## Workflow

For all contributors, we recommend the standard [GitHub flow](https://guides.github.com/introduction/flow/)
based on [forking and pull requests](https://guides.github.com/activities/forking/).

For significant changes, please [create an issue](https://github.com/vitessio/vitess/issues)
to let everyone know what you're planning to work on, and to track progress and design decisions.

## Guidance for Novice Vitess Developers

**Please read [vitess.io/contributing/](https://vitess.io/contributing/)** where we provide more information:

* How to make yourself familiar with Go and Vitess.
* How to go through the GitHub workflow.
* What to look for during code reviews.

### Contributions Related to Spelling and Grammar

At this time, we will not be accepting contributions that only fix spelling, naming or grammatical errors in documentation, code, comments or elsewhere, from accounts created in the last 365 days. We appreciate your interest in contributing to Vitess, and we encourage you to contribute in other ways.

## Pull requests and maintainer review

Maintainer review time is the scarce resource in this project. Using AI tools to write code is fine, but a pull request
should represent real work and understanding on the author's side, not only a change that compiles.

We are not looking for contributions where the author's only investment is running an issue through an AI tool and
opening a pull request with the result. Picking issues off the tracker to open pull requests for them ("issue sniping")
is discouraged: an open issue is not an invitation to submit a fix, and such pull requests may be closed without review.
If you want to work on an issue, it should be because the problem affects you or you are invested in the project.

When you open a pull request, please fill out the "Motivation" section of the pull request template: how you use Vitess,
and whether you ran the change against a real Vitess deployment (in any environment, not only production).
For a bug fix, show how you reproduced the problem, ideally with a test that fails without your change.

Pull requests from authors who do not use Vitess, or who did not run their change, are not rejected on principle, but they
are reviewed last and need stronger evidence that the change is correct and wanted. Maintainers may close pull requests that
leave the Motivation section empty or generic without reviewing them. Documentation, CI and test-only changes do not need
to have been run against a deployment.

Please also keep to one open pull request at a time until your first one has been merged.
