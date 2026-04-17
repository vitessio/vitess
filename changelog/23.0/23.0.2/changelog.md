# Changelog of Vitess v23.0.2

### Bug fixes 
#### Backup and Restore
 * [release-23.0] fix(backup): propagate file hashes to manifest after retry (#19336) [#19344](https://github.com/vitessio/vitess/pull/19344) 
#### Query Serving
 * [release-23.0] vtgate: defer implicit transaction start until after query planning (#19277) [#19342](https://github.com/vitessio/vitess/pull/19342)
### CI/Build 
#### Build/CI
 * [release-23.0] Consolidate CI test workflows (#19259) [#19273](https://github.com/vitessio/vitess/pull/19273)
 * [release-23.0] Run tests with gotestsum (#19076) [#19293](https://github.com/vitessio/vitess/pull/19293)
 * [release-23.0] Fix go upgrade tool (#19290) [#19299](https://github.com/vitessio/vitess/pull/19299)
 * [release-23.0] Switch gotestsum output format (#19215) [#19303](https://github.com/vitessio/vitess/pull/19303)
 * [release-23.0] Build boostrap image for local/region example CI (#19310) [#19317](https://github.com/vitessio/vitess/pull/19317)
 * [release-23.0] Don't add "Skip CI" label for Go upgrade PRs (#19307) [#19323](https://github.com/vitessio/vitess/pull/19323)
 * [release-23.0] Explicitly pass local image tags in example CI (#19320) [#19326](https://github.com/vitessio/vitess/pull/19326)
 * [release-23.0] Add lite image build CI job (#19321) [#19330](https://github.com/vitessio/vitess/pull/19330) 
#### Docker
 * [release-23.0] Build bootstrap image locally in ci (#19255) [#19266](https://github.com/vitessio/vitess/pull/19266)
### Compatibility Bug 
#### VTGate
 * [release-23.0] vtgate: fix handling of session variables on targeted connections (#19318) [#19335](https://github.com/vitessio/vitess/pull/19335)
### Dependencies 
#### Docker
 * [release-23.0] Upgrade the Golang version to `go1.25.7` [#19304](https://github.com/vitessio/vitess/pull/19304)
### Enhancement 
#### VTGate
 * [release-23.0] Performance: use `IsSingleShard()` check in `pushDerived` instead of just `engine.EqualUnique` opcode (#18974) [#19346](https://github.com/vitessio/vitess/pull/19346)
### Release 
#### General
 * [release-23.0] Bump to `v23.0.2-SNAPSHOT` after the `v23.0.1` release [#19288](https://github.com/vitessio/vitess/pull/19288)
### Testing 
#### Build/CI
 * [release-23.0] Generate race unit tests (#19078) [#19296](https://github.com/vitessio/vitess/pull/19296)

