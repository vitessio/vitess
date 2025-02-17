[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.vitess/vitess-jdbc)
[![Coverage Status](https://codecov.io/gh/vitessio/vitess/branch/main/graph/badge.svg)](https://app.codecov.io/gh/vitessio/vitess/tree/main)
[![Go Report Card](https://goreportcard.com/badge/vitess.io/vitess)](https://goreportcard.com/report/vitess.io/vitess)
[![FOSSA Status](https://app.fossa.com/api/projects/custom%2B162%2Fvitess.svg?type=shield&issueType=license)](https://app.fossa.com/projects/custom%2B162%2Fvitess?ref=badge_shield&issueType=license)
[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/1724/badge)](https://bestpractices.coreinfrastructure.org/projects/1724)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/vitessio/vitess/badge)](https://scorecard.dev/viewer/?uri=github.com/vitessio/vitess)

# Vitess 

Vitess is a cloud-native horizontally-scalable distributed database system that is built around MySQL.
Vitess can achieve unlimited scaling through generalized sharding.

Vitess allows application code and database queries to remain agnostic to the distribution of data onto
multiple database servers. With Vitess, you can even split and merge shards as your needs
grow, with an atomic cutover step that takes only a few seconds.

Vitess was a core component of YouTube's database infrastructure
from 2011, and grew to encompass tens of thousands of MySQL nodes. 
Starting in 2015, Vitess was adopted by many other large companies, including Slack, Square (now Block), and JD.com.

For more about Vitess, please visit [vitess.io](https://vitess.io).

## Community

Vitess has a growing [community](https://github.com/vitessio/vitess/blob/main/ADOPTERS.md).

If you are interested in contributing or participating in our monthly community meetings, please visit the [Community page on our website](https://vitess.io/community/).

We also maintain a [roadmap](https://vitess.io/docs/roadmap/) on our website.

Follow our [blog](https://blog.vitess.io/) for low-frequency updates like new features and releases.

## Reporting a Problem, Issue, or Bug

To report a problem, create a [GitHub issue](https://github.com/vitessio/vitess/issues).

For topics that are better discussed live, please join the [Vitess Slack](https://vitess.io/slack) workspace.
You may post any questions on the #general channel or join some of the special-interest channels.

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
