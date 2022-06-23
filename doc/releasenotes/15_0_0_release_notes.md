# Release of Vitess v15.0.0
## Major Changes

### Command-line syntax deprecations

#### vttablet startup flag --enable-query-plan-field-caching
This flag is now deprecated. It will be removed in v16. 

### New command line flags and behavior

#### vtctl GetSchema --table-schema-only

The new flag `--table-schema-only` skips columns introspection. `GetSchema` only returns general schema analysis, and specifically it includes the `CREATE TABLE|VIEW` statement in `schema` field.

### Online DDL changes
------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/doc/releasenotes/15_0_0_changelog.md).

The release includes 23 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @K-Kumar-01, @ajm188, @arvind-murty, @dbussink, @frouioui, @harshit-gangal, @notfelineit, @rohit-nayak-ps, @rsajwani, @shlomi-noach, @systay

