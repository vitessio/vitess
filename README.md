[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc)
[![Build Status](https://travis-ci.org/vitessio/vitess.svg?branch=master)](https://travis-ci.org/vitessio/vitess/builds)
[![codebeat badge](https://codebeat.co/badges/51c9a056-1103-4522-9a9c-dc623821ea87)](https://codebeat.co/projects/github-com-youtube-vitess)
[![Go Report Card](https://goreportcard.com/badge/vitess.io/vitess)](https://goreportcard.com/report/vitess.io/vitess)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fvitessio%2Fvitess.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2Fvitessio%2Fvitess?ref=badge_shield)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/1724/badge)](https://bestpractices.coreinfrastructure.org/projects/1724)

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
[here](https://github.com/vitessio/vitess/blob/master/ADOPTERS.md).

# Dolt's Use of Vitess

[Dolt's](https://www.doltdb.com/) fork of Vitess has headed in a different direction from the main project. In addition to adding support for DDL statements, stored procedures, and triggers (never a priority of the Vitess project), Dolt's fork prunes away the 90% of Vitess that isn't vital to Dolt's current roadmap as a [version controlled database](https://www.dolthub.com/blog/2022-08-04-database-versioning/). You can read details about this work in this [blog post](https://www.dolthub.com/blog/2020-09-23-vitess-pruning/).

## Contact

Ask questions in the
[vitess@googlegroups.com](https://groups.google.com/forum/#!forum/vitess)
discussion forum.

For topics that are better discussed live, please join the
[Vitess Slack](https://vitess.io/slack) workspace.

Subscribe to
[vitess-announce@googlegroups.com](https://groups.google.com/forum/#!forum/vitess-announce)
or the [Vitess Blog](https://blog.vitess.io/)
for low-frequency updates like new features and releases.

## Security

### Security Policy

The [security policy](https://github.com/dolthub/vitess/blob/main/SECURITY.md) is maintained in this repository. Please follow the disclosure instructions there. Please do not initially report security issues in this repository's public GitHub issues.

### Security Audit

A third party security audit was performed of the upstream project by Cure53. You can see the full report [here](https://github.com/vitessio/vitess/blob/main/doc/VIT-01-report.pdf).

## License

Unless otherwise noted, the Vitess source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.

[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2Fvitessio%2Fvitess.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2Fvitessio%2Fvitess?ref=badge_large)
