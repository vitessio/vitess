# Release Instructions

This page describes the steps for cutting a new [open source release](https://github.com/vitessio/vitess/releases).

### Summary

- [Versioning](#versioning)
- [Release Branches](#release-branches)
- [Release Tags](#release-tags)
- [Docker Images](#docker-images)
- [Java Packages](#java-packages)
- [Release Cutover](#release-cutover)

-------

## Versioning

Our versioning strategy is based on [VEP5](https://github.com/vitessio/enhancements/blob/main/veps/vep-5.md).

### Major Release (vX)

A new major release is needed when the public API changes in a
backward-incompatible way -- for example, when removing deprecated interfaces.

Our public API includes (but is not limited to):

*   The VTGate [RPC interfaces](https://github.com/vitessio/vitess/tree/main/proto).
*   The interfaces exposed by the VTGate client library in each language.

Care must also be taken when changing the format of any data stored by a live
system, such as topology data or Vitess-internal tables (used for sequences,
distributed transactions, etc.). Although this data is considered as internal to
Vitess, if any change breaks the upgrade path for a live system (for example,
requiring that it be shut down and reinitialized from scratch), then it must be
considered as a breaking change.

### Minor Release (vX.Y)

A new minor release indicates that functionality has been added or changed in a
backward-compatible way. This should be the majority of normal releases.

### Patch Release (vX.Y.Z)

A patch release indicates that only a select set of bugfixes have been
cherry-picked onto the associated minor release. The expectation is that
upgrading by a patch release should be painless (not requiring any config
changes) and safe (isolated from active development on `main`).

### Pre-Release Labels (vX.Y.Z-labelN)

Pre-release versions should be labeled with a suffix like `-beta2` or `-rc1`.

-------

## Release Branches

Each major and minor releases (X.Y) should have a [release branch](https://github.com/vitessio/vitess/branches/all?query=release) named
`release-X.Y`. This branch should diverge from `main` when the code freeze when the release
is declared, after which point only bugfix PRs should be cherry-picked onto the branch.
All other activity on `main` will go out with a subsequent major or minor release.

```shell
git checkout main
git pull --ff-only upstream main

git checkout -b release-X.Y
git push upstream release-X.Y
```

The branches are named `release-X.Y` to distinguish them from point-in-time
tags, which are named `vX.Y.Z`.

-------

## Release Tags

While the release branch is a moving target, release tags mark point-in-time
snapshots of the repository. Essentially, a tag assigns a human-readable name to
a specific Git commit hash. Although it's technically possible to reassign a tag
name to a different hash, we must never do this.

-------

## Docker Images

Docker images built automatically on DockerHub and can be found [here](https://hub.docker.com/repository/docker/vitess/lite/).

-------

## Java Packages

We publish binary packages for our [JDBC driver and Java client on Maven Central](https://search.maven.org/#search|ga|1|g:"io.vitess").

To do so, we use the http://oss.sonatype.org/ repository.
New packages must be uploaded there ("deployed") and will be automatically published ("released").
Once they are released there, they will be automatically synchronized with Maven Central.
The synchronization takes only several minutes, but the update on http://search.maven.org may take up to two hours.

### Access to oss.sonatype.org

[Sign up here.](https://issues.sonatype.org/secure/Signup!default.jspa)
Then you must be added as member to our `io.vitess` namespace.
Therefore, file a JIRA ticket with Sonatype to get added ([example for a different namespace](https://issues.sonatype.org/browse/OSSRH-30604)).

### One-time setup

#### Set up GPG

Follow [Sonatype's GPG instructions](https://central.sonatype.org/pages/working-with-pgp-signatures.html).

Install `gpg-agent` (needed below) e.g. on Ubuntu via: `sudo apt-get install gnupg-agent`.

#### Login configuration

Create the `settings.xml` in the `$HOME/.m2/` directory as described in their [instructions](https://central.sonatype.org/pages/apache-maven.html).

-------

## Release Cutover 

In this section we describe our current release process. We begin with a list of [**pre-requisites for the release team**](#pre-requisites) and with a short [**overview**](#overview).
The release process is divided into three parts: [**Pre-Release**](#pre-release), [**Release**](#release), [**Post-Release**](#post-release), which are detailed after the overview.

### Pre-Requisites

This section highlights the different pre-requisites the release team has to meet before releasing.

- The tool `gh` must be installed locally and ready to be used.
- You must have access to the Java release, more information in the [**Java Packages**](#java-packages) section.
- You must be able to create branches and have admin right on the `vitessio/vitess` and `planetscale/vitess-operator` repositories.

### Overview

#### Schedule

A new major version of Vitess is released every four months. For each major version there is at least one release candidate, which we release three weeks before the GA version.
We usually create the RC1 during the first week of the month, and the GA version three weeks later.

#### Code Freeze

Before creating RC1, there is a code freeze. Assuming the release of RC1 happens on a Tuesday, the release branch will be frozen Friday of the previous week.
This allows us to test that the release branch can be released and avoid discovering unwanted events during the release day. Once the RC1 is released, there are three more weeks to backport bug fixes into the release branches. However, we also proceed to a code freeze the Friday before the GA release. (Assuming GA is on a Tuesday)
Regarding patch releases, no code freeze is planned.

#### Tracking Issue for each Release

For each release, it is recommended to create an issue like [this one](https://github.com/vitessio/vitess/issues/10476) to track the current and past progress of a release.
It also allows us to document what happened during a release.

### Pre-Release

This step happens a few weeks before the actual release (whether it is an RC, GA or a patch release).
The main goal of this step is to make sure everything is ready to be released for the release day.
That includes:
- **Making sure Pull Requests are being reviewed and merged.**
  > - All the Pull Requests that need to be in the release must be reviewed and merged before the code freeze.
  > - The code freeze usually happens a few days before the release.
- **Making sure the people doing the release have access to all the tools and infrastructure needed to do the release.**
  > - This includes write access to the Vitess repository and to the Maven repository. 
- **Preparing and cleaning the release notes summary.**
  > - One or more Pull Requests have to be submitted in advance to create and update the release summary.
  > - The summary files are located in: `./doc/releasenotes/*_*_*_summary.md`.
  > - The summary file for a release candidate is the same as the one for the GA release.
- **Finishing the blog post, and coordinating with the different organizations for cross-posting. Usually CNCF and PlanetScale. This step applies only for GA releases.**
  > - The blog post must be finished and reviewed.
  > - A Pull Request on the website repository of Vitess has to be created so we can easily publish the blog during the release day.
- **Code freeze.**
  > - As soon as we go into code freeze, if we are doing an RC, create the release branch.
  > - If we are doing a GA release, do not merge any new Pull Requests.
  > - The guide on how to do a code freeze is available in the [How To Code Freeze](#how-to-code-freeze) section.
- **Preparing the Vitess Operator release.**
  > - While the Vitess Operator is located in a different repository, we also need to do a release for it.
  > - The Operator follows the same cycle: RC1 -> GA -> Patches.
  > - Documentation for the pre-release of the Vitess Operator is available [here](https://github.com/planetscale/vitess-operator/blob/main/docs/release-process.md#prepare-for-release).

### Release

On the release day, there are several things to do:

- **Create the Vitess release.**
  > - A guide on how to create a Vitess release is available in the [How To Release Vitess](#how-to-release-vitess) section.
- **Create the corresponding Vitess operator release.**
  > - Applies only to versions greater or equal to `v14.0.0`.
  > - If we are doing an RC release, then we will need to create the Vitess Operator RC too. If we are doing a GA release, we're also doing a GA release in the Operator.
  > - The Vitess Operator release documentation is available [here](https://github.com/planetscale/vitess-operator/blob/main/docs/release-process.md#cut-release).
- **Create the Java release.**
  > - Applies only to GA releases.
  > - This step is explained in the [Java Packages: Deploy & Release](#java-packages-deploy--release) section.
- **Update the website documentation repository.**
  > - Applies only to GA and RC releases.
  > - There are two scripts in the website repository in `./tools/{ga|rc}_release.sh`, use them to update the website documentation. The scripts automate:
  >   - For an RC, we need to create a new version in the sidebar and mark the current version as RC.
  >   - For a GA, we need to mark the version we are releasing as "Stable" and the next one as "Development".
- **Publish the blog post on the Vitess website.**
  > - Applies only to GA releases.
  > - The corresponding Pull Request was created beforehand during the pre-release. Merge it.
- **Make sure _arewefastyet_ starts benchmarking the new release.**
  > - This can be done by visiting [arewefastyet status page](https://benchmark.vitess.io/status).
  > - New elements should be added to the execution queue.
  > - After a while, those elements will finish their execution and their status will be green.
  > - This step is even more important for GA releases as we often include a link to _arewefastyet_ in the blog post.
  > - The benchmarks need to complete before announcing the blog posts or before they get cross-posted.
- **Update the release notes on the release branch and on `main`.**
  > - Two new Pull Requests have to be created.
  > - One against `main`, it will contain only the new release notes.
  > - And another against the release branch, this one contains the release notes and the release commit. (The commit on which we did `git tag`) 
- **Build k8s Docker images and publish them**
  > - The docker image for `base`, `lite`, etc are built automatically by DockerHub. The k8s images however are dependent on these images and are required to be built manually.
  > - These images should be built after the `base` image has been built and available on DockerHub.
  > - To build and publish these images, run `./release.sh` from the directory `vitess/docker`.

### Post-Release

Once the release is over, we need to announce it on both Slack and Twitter. We also want to make sure the blog post was cross-posted, if applicable.
We need to verify that _arewefastyet_ has finished the benchmark too.


### How To Release Vitess
This section is divided into two parts:
- [Creation of the tags and release notes](#creation-of-the-tags-and-release-notes).
- [Creating Release or Release Candidate on the GitHub UI](#creating-release-or-release-candidate-on-the-github-ui)

#### Creation of the tags and release notes

> In this example our current version is `v14` and we release the version `v15.0.0`.
> Alongside Vitess' release, we also release a new version of the operator.
> Since we are releasing a release candidate here, the new version of the operator will also be a release candidate.
> In this example, the new operator version is `2.8.0`.
> 
> It is important to note that before the RC, there is a code freeze during which we create the release branch.
>
> The release branch in this example is `release-15.0`.
> 
> The example also assumes that `origin` is the `vitessio/vitess` remote.

1. Fetch `github.com/vitessio/vitess`'s remote.
    ```shell
    git fetch origin
    ```

2. Creation of the release notes and tags.
   1. Run the release script using the Makefile:
      1. Release Candidate:
          ```shell
          make BASE_BRANCH="release-15.0" BASE_REMOTE="origin" RELEASE_VERSION="15.0.0-rc1" DEV_VERSION="15.0.0-SNAPSHOT" VTOP_VERSION="2.8.0-rc1" do_release
          ```
      2. General Availability:
         ```shell
         make BASE_BRANCH="release-15.0" BASE_REMOTE="origin" RELEASE_VERSION="15.0.0" DEV_VERSION="15.0.1-SNAPSHOT" VTOP_VERSION="2.8.0" do_release
         ```

      The script will prompt you `Pausing so release notes can be added. Press enter to continue`. We are now going to generate the release notes, continue to the next sub-step.

   2. Run the following command to generate the release notes:
      1. Release Candidate:
          ```shell
          make VERSION="v15.0.0-rc1" FROM="v14.0.0" TO="HEAD" SUMMARY="./doc/releasenotes/15_0_0_summary.md" release-notes  
          ```
      2. General Availability:
          ```shell
          make VERSION="v15.0.0-rc1" FROM="v14.0.0" TO="HEAD" SUMMARY="./doc/releasenotes/15_0_0_summary.md" release-notes  
          ```
      This command will generate the release notes by looking at all the commits between the tag `v14.0.0` and the reference `HEAD`.
      It will also use the file located in `./doc/releasenotes/15_0_0_summary.md` to prefix the release notes with a text that the maintainers wrote before the release.


3. Follow the instruction prompted by the `do_release` Makefile command's output in order to push the tags, branches and create the Pull Requests.

4. Create a Pull Request against the `main` branch with the newly created release notes.

5. Release the tag on GitHub UI as explained in the following section.

#### Creating Release or Release Candidate on the GitHub UI

> In the below steps, we use `v8.0.0` and `v9.0.0` as an example.

##### 1. Open the releases page

On Vitess' GitHub repository main page, click on Code -> [Releases](https://github.com/vitessio/vitess/releases).

![alt text](.images/release-01.png)

##### 2. Draft a new release

On the Releases page, click on `Draft a new release`.

![alt text](.images/release-02.png)

##### 3. Tag a new release

When drafting a new release, we are asked to choose the release's tag and branch.
We format the tag this way: `v9.0.0`. We append `-rcN` to the tag name for release candidates,
with `N` being the increment of the release candidate.

![alt text](.images/release-03.png)

##### 4. Add release notes and release

Copy/paste the previously built Release Notes into the description of the release.

If this is a pre-release (`rc`) select the `pre-release` checkbox.

And finally, click on `Publish release`.

![alt text](.images/release-04.png)

### How To Code Freeze

In this example we are going to do a code freeze on the `release-15.0` branch. If we are doing a release candidate, there won't be a branch yet, hence we need to create it.

```
git fetch --all
git checkout -b release-15.0 origin/main
```

The new branch will be based on `origin/main`, here `origin` points to `vitessio/vitess`. If we are not doing a release candidate, then the branch already exists and we can checkout on it.

Now, if we are doing a GA release, let's update the branch:

```
git pull origin release-15.0
```

Finally, let's run the code freeze script:

```
./tools/code_freeze.sh freeze release-15.0
```

The script will prompt the command that will allow you to push the code freeze change. Once pushed, open a PR that will be merged on `release-15.0`.

### Java Packages: Deploy & Release

> **Warning:** This section's steps need to be executed only when releasing a new major version of Vitess,
> or if the Java packages changed from one minor/patch version to another.
> 
> For this example, we assume we juste released `v12.0.0`.

1.  Checkout to the release commit.
    ```shell
    git checkout v12.0.0
    ```

2.  Run `gpg-agent` to avoid that Maven will constantly prompt you for the password of your private key.

    ```bash
    eval $(gpg-agent --daemon --no-grab --write-env-file $HOME/.gpg-agent-info)
    export GPG_TTY=$(tty)
    export GPG_AGENT_INFO
    ```

3.  Deploy (upload) the Java code to the oss.sonatype.org repository:

    > **Warning:** After the deployment, the Java packages will be automatically released. Once released, you cannot delete them. The only option is to upload a newer version (e.g. increment the patch level).</p>

    ```bash
    mvn clean deploy -P release -DskipTests
    cd ..
    ```
