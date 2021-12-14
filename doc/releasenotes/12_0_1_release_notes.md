# Release of Vitess v12.0.1

## Announcement

This patch is providing an update regarding the Apache Log4j security vulnerability (CVE-2021-44228) (#9357), along with a few bug fixes.

------------
## Changelog

### Bug fixes
#### Query Serving
* Ensure that hex query predicates are normalized for planner cache #9145
* Gen4: Fail cross-shard join query with aggregation and grouping #9167
* Make sure to copy bindvars when using them concurrently #9246
* Remove keyspace from query before sending it on #9247
* Use decoded hex string when calculating the keyspace ID #9293
#### VReplication
* Fix boolean parameter order in DropSources call for v2 flows #9178
* Take MySQL Column Type Into Account in VStreamer #9355
#### Cluster management
* Restoring 'vtctl VExec' command #9227
    * This change restores vtctl VExec functionality. It was removed based on the assumption the only uses for this command were for Online DDL command. This was wrong, and VExec is also used as a wrapper around VReplication.

### CI/Build
#### Build/CI
* CI: ubuntu-latest now has MySQL 8.0.26, let us override it with latest 8.0.x #9373
### Internal Cleanup
#### Java
* build(deps): bump log4j-api from 2.13.3 to 2.15.0 in /java #9357


The release includes 21 commits (excluding merges)

Thanks to all our contributors: @GuptaManan100, @askdba, @deepthi, @dependabot[bot], @frouioui, @hallaroo, @harshit-gangal, @mattlord, @rohit-nayak-ps, @shlomi-noach, @systay, @vmg 