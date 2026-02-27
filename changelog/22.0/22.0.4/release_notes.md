# Release of Vitess v22.0.4
# Release of Vitess v22.0.4

## Summary

This is a security focused release. It contains fixes for two recently reported CVEs along with a number of other security related fixes.

### External Decompressor No Longer Read from Backup MANIFEST by Default

This is a fix for the following security advisory and associated CVE

- Advisory: <https://github.com/vitessio/vitess/security/advisories/GHSA-8g8j-r87h-p36x>
- CVE: <https://www.cve.org/CVERecord?id=CVE-2026-27965>

The external decompressor command stored in a backup's `MANIFEST` file is no longer used at restore time by default. Previously, when no `--external-decompressor` flag was provided, VTTablet would fall back to the command specified in the `MANIFEST`. This posed a security risk: an attacker with write access to backup storage could modify the `MANIFEST` to execute arbitrary commands on the tablet.

*Please note that this is a breaking change.* Starting in v22.0.4, the `MANIFEST`-based decompressor is ignored unless you explicitly opt in with the new `--external-decompressor-use-manifest` flag. If you rely on this behavior, add the flag to your VTTablet configuration, but be aware of the security implications.

See [#19460](https://github.com/vitessio/vitess/pull/19460) for details.

### Prevent Path Traversals Via Backup MANIFEST Files On restore

This is a fix for the following security advisory and associated CVE

- Advisory: <https://github.com/vitessio/vitess/security/advisories/GHSA-r492-hjgh-c9gw>
- CVE: <https://www.cve.org/CVERecord?id=CVE-2026-27969>

We now prevent a common [Path Traversal attack](https://owasp.org/www-community/attacks/Path_Traversal) that someone with write access to backup storage could use to escape the target restore directory and write files to arbitrary filesystem paths via modifications to the `MANIFEST`.

See [#19470](https://github.com/vitessio/vitess/pull/19470) for details.

------------
The entire changelog for this release can be found [here](https://github.com/vitessio/vitess/blob/main/changelog/22.0/22.0.4/changelog.md).

The release includes 37 merged Pull Requests.

Thanks to all our contributors: @app/vitess-bot, @mattlord, @vitess-bot

