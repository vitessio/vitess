[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc)
[![Build Status](https://travis-ci.org/vitessio/vitess.svg?branch=master)](https://travis-ci.org/vitessio/vitess/builds)
[![codebeat badge](https://codebeat.co/badges/51c9a056-1103-4522-9a9c-dc623821ea87)](https://codebeat.co/projects/github-com-youtube-vitess)
[![Go Report Card](https://goreportcard.com/badge/vitess.io/vitess)](https://goreportcard.com/report/vitess.io/vitess)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fvitessio%2Fvitess.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fvitessio%2Fvitess?ref=badge_shield)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/1724/badge)](https://bestpractices.coreinfrastructure.org/projects/1724)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=vitessio&metric=coverage)](https://sonarcloud.io/dashboard?id=vitessio)

# Branch description

Collapsed commits from

```
26f2fb7a5b (origin/latest, latest) Hide the overly specific Vitess version string from clients (#283)
887e04d397 backups: try to fix short write error (#255)
0ecde2c095 kmsbackup: switch to fully use annoations (#253)
d92f903071 kmsbackup: support annotations (#230)
c5a1d9b953 rebase from upstream main branch, master has been renamed (#235)
babb2c668d Push latest to old PSOP repo (#231)
d7426114a7 kmsbackup: tests and code cleanup (#212)
743ed110df Add back image name to nightly images (#225)
366a36c148 Fix images output var reference in nightly (#224)
8cc1b4221e Build fewer images and push vthead to dev (#222)
1781e7243a Really, actually, for realsies fix nightly (#217)
5509ac7eb7 Fix nightly sha (#216)
f4354d8147 Fix nightly image sha (#214)
5345c46f68 Nightly build main not latest (default) (#213)
cd02bd7840 Implement updated branching strategy.
a3e68bbfc0 Remove upstream sha from build workflows (#207)
06952d46d0 Annotate branch
92156fedc1 kmsbackup: backup implementation for singularity
6416572e30 Simplify CODEOWNERS
fbbf8d6eb6 Add vttablet-starter. (#46)
2618a29eae Setup nightly builds
05a699a6a2 .github: Add workflow to rebase from upstream.
dc63ef0bd6 Add README.md for private fork.
```

to

```
58a25c4f70 (HEAD -> ss-cleanup) Annotate branch
b43ff9fd61 Hide the overly specific Vitess version string from clients (#283)
b2d1a76fd9 kmsbackup: backup implementation for singularity
e4f2248683 Add vttablet-starter. (#46)
a3fca7de96 PlanetScale workflows
f8674ca2b2 Simplify CODEOWNERS
dc63ef0bd6 Add README.md for private fork.
```

Also dropped 887e04d397 because it was not helpful.


# Vitess 

Vitess is a database clustering system for horizontal scaling of MySQL
through generalized sharding.

By encapsulating shard-routing logic, Vitess allows application code and
database queries to remain agnostic to the distribution of data onto
multiple shards. With Vitess, you can even split and merge shards as your needs
grow, with an atomic cutover step that takes only a few seconds.

Vitess has been a core component of YouTube's database infrastructure
since 2011, and has grown to encompass tens of thousands of MySQL nodes.

For more about Vitess, please visit [vitess.io](https://vitess.io).

Vitess has a growing community. You can view the list of adopters
[here](https://github.com/vitessio/vitess/blob/main/ADOPTERS.md).

## Reporting a Problem, Issue, or Bug
To report a problem, the best way to get attention is to create a GitHub [issue](.https://github.com/vitessio/vitess/issues ) using proper severity level based on this [guide](https://github.com/vitessio/vitess/blob/main/SEVERITY.md). 

For topics that are better discussed live, please join the [Vitess Slack](https://vitess.io/slack) workspace.
You may post any questions on the #general channel or join some of the special-interest channels.

Follow [Vitess Blog](https://blog.vitess.io/) for low-frequency updates like new features and releases.

## Security

### Reporting Security Vulnerabilities

To report a security vulnerability, please email [vitess-maintainers](mailto:cncf-vitess-maintainers@lists.cncf.io).

See [Security](SECURITY.md) for a full outline of the security process.

### Security Audit

A third party security audit was performed by Cure53. You can see the full report [here](doc/VIT-01-report.pdf).

## License

Unless otherwise noted, the Vitess source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fvitessio%2Fvitess.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fvitessio%2Fvitess?ref=badge_large)
