### Review Checklist

Hello reviewers! :wave: Please follow this checklist when reviewing this Pull Request.

#### General
- [ ] Ensure that the Pull Request has a descriptive title.
- [ ] Ensure there is a link to an issue (except for internal cleanup and flaky test fixes), new features should have an RFC that documents use cases and test cases.

#### Tests
- [ ] Bug fixes should have at least one unit or end-to-end test, enhancement and new features should have a sufficient number of tests.

#### Documentation
- [ ] Apply the `release notes (needs details)` label if users need to know about this change.
- [ ] New features should be documented.
- [ ] There should be some code comments as to why things are implemented the way they are.
- [ ] There should be a comment at the top of each new or modified test to explain what the test does.

#### New flags
- [ ] Is this flag really necessary?
- [ ] Flag names must be clear and intuitive, use dashes (`-`), and have a clear help text.

#### If a workflow is added or modified:
- [ ] Each item in `Jobs` should be named in order to mark it as `required`.
- [ ] If the workflow needs to be marked as `required`, the maintainer team must be notified.

#### Backward compatibility
- [ ] Protobuf changes should be wire-compatible.
- [ ] Changes to `_vt` tables and RPCs need to be backward compatible.
- [ ] RPC changes should be compatible with vitess-operator
- [ ] If a flag is removed, then it should also be removed from [vitess-operator](https://github.com/planetscale/vitess-operator) and [arewefastyet](https://github.com/vitessio/arewefastyet), if used there.
- [ ] `vtctl` command output order should be stable and `awk`-able.
