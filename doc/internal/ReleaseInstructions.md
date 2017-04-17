# Release Instructions

This page describes the steps for cutting a new [open source release]
(https://github.com/youtube/vitess/releases).

## Versioning

Our versioning strategy is based on [Semantic Versioning](http://semver.org/).

### Major Release (vX)

A new major release is needed when the public API changes in a
backward-incompatible way -- for example, when removing deprecated interfaces.

Our public API includes (but is not limited to):

*   The VTGate [RPC interfaces]
    (https://github.com/youtube/vitess/tree/master/proto).
*   The interfaces exposed by the VTGate client library in each language.

Care must also be taken when changing the format of any data stored by a live
system, such as topology data or Vitess-internal tables (used for sequences,
distributed transactions, etc.). Although this data is considered internal to
Vitess, if any change breaks the upgrade path for a live system (for example,
requiring that it be shut down and reinitialized from scratch), then it must be
considered a breaking change.

### Minor Release (vX.Y)

A new minor release indicates that functionality has been added or changed in a
backward-compatible way. This should be the majority of normal releases.

### Patch Release (vX.Y.Z)

A patch release indicates that only a select set of bugfixes have been
cherrypicked onto the associated minor release. The expectation is that
upgrading by a patch release should be painless (not requiring any config
changes) and safe (isolated from active development on `master`).

### Pre-Release Labels (vX.Y.Z-label.N)

Pre-release versions should be labeled with a suffix like `-beta.2` or `-rc.1`.
The dot before the number is important for correct determination of version
precedence.

## Milestones

[GitHub Milestones](https://github.com/youtube/vitess/milestones) are hotlists
for Issues and Pull Requests.

When it's time to start planning a new Vitess release, create a milestone for it
and tag the following items under it:

*   Issues that are release-blockers for that milestone.
*   For major/minor releases:
    *   Pull Requests that must be merged before cutting that milestone. This
        makes it clear what we're waiting for.
*   For patch releases:
    *   Pull Requests that should be cherrypicked into the release branch for
        that milestone. Any other PRs will be ignored and must wait for the next
        minor release.

## Release Branches

Each minor release level (X.Y) should have a [release branch]
(https://github.com/youtube/vitess/branches/all?query=release) named
`release-X.Y`. This branch should diverge from `master` when the code freeze for
that release is declared, after which point only bugfix PRs should be
cherrypicked onto the branch. All other activity on `master` will go out with a
subsequent minor release.

```bash
git checkout master
git pull --ff-only upstream master

git checkout -b release-X.Y
git push upstream release-X.Y
```

The branches are named `release-X.Y` to distinguish them from point-in-time
tags, which are named `vX.Y.Z`.

## Release Tags

While the release branch is a moving target, release tags mark point-in-time
snapshots of the repository. Essentially, a tag assigns a human-readable name to
a specific Git commit hash. Although it's technically possible to reassign a tag
name to a different hash, we must never do this.

Since a tag represents a particular point in time, only patch releases (X.Y.Z)
have tags. These should be defined as [annotated tags]
(https://git-scm.com/book/en/v2/Git-Basics-Tagging#Annotated-Tags) from the
associated release branch, after all cherrypicks have been applied and tested.

```bash
git checkout release-X.Y
git pull --ff-only upstream release-X.Y

git cherry-pick <commit> ...

git tag -a vX.Y.Z
```

Note that this only creates the tag in your local Git repository. Pushing the
tag up to GitHub will be the last step, because it is the point of no return.
That's because if someone has already fetched the tag, they will not get updated
if you change the tag. Therefore, if you need to tag a different commit after
pushing to upstream, you must increment the version number and create a new tag
(i.e. a new release).

## Docker Images

Note: You'll require an account on Docker Hub to execute the `docker push`
command.

```bash
# Rebuild all dependent images locally. Full chain: common->mysql57->base->lite.
make docker_bootstrap DOCKER_IMAGES="common mysql57"
make docker_base

make docker_lite
make docker_guestbook

MINOR=vX.Y
PATCH=vX.Y.Z

# Tag the new PATCH version.
docker tag vitess/lite vitess/lite:$PATCH
docker tag vitess/guestbook vitess/guestbook:$PATCH

# NOTE: Skip these two steps for pre-releases (e.g. alpha.1, rc.1 releases).
# Update the MINOR Docker tag to point to the latest PATCH version.
docker tag -f vitess/lite vitess/lite:$MINOR
docker tag -f vitess/guestbook vitess/guestbook:$MINOR

# Push the tags and images to Docker Hub.
docker push vitess/lite:$PATCH
docker push vitess/lite:latest
docker push vitess/guestbook:$PATCH
docker push vitess/guestbook:latest
# NOTE: Skip the next two steps for pre-releases (e.g. alpha.1, rc.1 releases).
docker push vitess/lite:$MINOR
docker push vitess/guestbook:$MINOR
```

Note that you **do not** push the `base` image you built. That gets built
automatically by Docker Hub when you push the branch.

After pushing the Docker images, you have to make sure that our tutorials still
work with them.

## Testing

### Local Tutorial

We must check that the [local startup
tutorial](http://vitess.io/getting-started/local-instance.html#start-a-vitess-cluster)
is not broken.

Instead of going through the steps manually, run the `local_example` test which
should have the same commands as the tutorial. You can use our `test.go` test
runner to run the test for all MySQL flavors. `-parallel=2` will run two tests
in parallel to shorten the test duration.

```bash
./test.go -flavor=all -pull=false -parallel=2 local_example
```

### Kubernetes Tutorial

Follow the [Kubernetes tutorial](http://vitess.io/getting-started/), which will
automatically use the latest Docker images you pushed.

TODO(mberlin): Describe how to launch our new cluster tests in `test/cluster`
instead.

## Push the release branch and tag to upstream

Note that we're pushing to upstream (youtube/vitess), not origin (your fork).

<p class="warning"><b>WARNING:</b> After the following push, there's no going
back, since tags don't get updated if someone else has fetched them already.
If you need to re-tag after this point, you MUST increment the version number.
</p>

```bash
# release branch
git push upstream release-X.Y
# release tag
git push upstream vX.Y.Z
```

## Add release notes and send announcement

[Find your new tag](https://github.com/youtube/vitess/tags) and add release
notes. Use the GitHub [Compare](https://github.com/youtube/vitess/compare) tool
to see all the commits since the last release.

Then send an announcement on the [vitess-announce]
(https://groups.google.com/forum/#!forum/vitess-announce) list.
