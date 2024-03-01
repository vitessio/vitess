[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc)
[![Coverage Status](https://codecov.io/gh/vitessio/vitess/branch/main/graph/badge.svg)](https://app.codecov.io/gh/vitessio/vitess/tree/main)
[![Go Report Card](https://goreportcard.com/badge/vitess.io/vitess)](https://goreportcard.com/report/vitess.io/vitess)
[![FOSSA Status](https://app.fossa.com/api/projects/custom%2B162%2Fvitess.svg?type=shield&issueType=license)](https://app.fossa.com/projects/custom%2B162%2Fvitess?ref=badge_shield&issueType=license)
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

Vitess has a growing community. [View the list of adopters](https://github.com/vitessio/vitess/blob/main/ADOPTERS.md).

## Reporting a Problem, Issue, or Bug
To report a problem, create a [GitHub issue](https://github.com/vitessio/vitess/issues). 

For topics that are better discussed live, please join the [Vitess Slack](https://vitess.io/slack) workspace.
You may post any questions on the #general channel or join some of the special-interest channels.

Follow [Vitess Blog](https://blog.vitess.io/) for low-frequency updates like new features and releases.

## Security

### Reporting Security Vulnerabilities

To report a security vulnerability, please email [vitess-maintainers](mailto:cncf-vitess-maintainers@lists.cncf.io).

See [Security](SECURITY.md) for a full outline of the security process.

### Security Audit

A third party security audit was performed by ADA Logics. [Read the full report](doc/VIT-03-report-security-audit.pdf).

## License

Unless otherwise noted, the Vitess source files are distributed
under the Apache Version 2.0 license found in the LICENSE file.

[![FOSSA Status](https://app.fossa.com/api/projects/custom%2B162%2Fvitess.svg?type=large&issueType=license)](https://app.fossa.com/projects/custom%2B162%2Fvitess?ref=badge_large&issueType=license)
