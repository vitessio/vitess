# Changelog of Vitess v14.0.1

### Bug fixes 
#### Build/CI
 * Fixed the release notes CI check helper #10574 
#### Cluster management
 * Fix Online DDL Revert flakiness #10675 
#### Query Serving
 * Use the old planner version flag in v14 upgrade-downgrade tests #10616
 * BugFix: Keep predicates in join when pushing new ones #10715
 * [release-14.0] BugFix: Gen4CompareV3 planner reverted to Gen4 on Update queries (#10722) #10723
 * [release-14.0] Auto Detect MySQL Version and Use in vtgate mysql_server_version Flag #10733
 * fix: evalengine - check compare numeric of same type #10793
 * Backport v14: reduce ApplySchema complexity, normalize calls to `ReloadSchema` #10796 
#### VReplication
 * VDiff2: ignore errors while attempting to purge vdiff tables  #10725
 * [release-14.0] Add drop_foreign_keys to v2 MoveTables command (#10773) #10774
### CI/Build 
#### Build/CI
 * Remove the review checklist workflow #10656
 * looking into onlineddl_vrepl_stress_suite flakiness in CI #10779 
#### Cluster management
 * Fix examples/compose/docker-compose.yml to run healthy vttablets #10597
 * Fix full status test to work with both MySQL versions and de-flake it #10677 
#### General
 * Upgrade to `go1.18.4` #10705
### Documentation 
#### Cluster management
 * Add the vtorc discovery bug as a known issue to 14.0 #10711
### Enhancement 
#### Build/CI
 * Skip CI workflows on `push` for pull requests #10768
### Release 
#### Documentation
 * release notes: schema tracking no longer experimental #10611 
#### General
 * Release of v14.0.0 on release-14.0 #10607
 * [14] Addition of the release notes summary for v14.0.1 #10821
### Testing 
#### Build/CI
 * CI: mysql8 test for schemadiff_vrepl #10679 
#### Query Serving
 * unit test: fix mysql tests to run on MacOS #10613
 * vtgate e2e: expect different value for mysql80 for a few tests #10680
 * [14.0] set parameter on vtgate than on vttablet #10699 
#### web UI
 * Fixing flaky vtctld2 web test #10541

