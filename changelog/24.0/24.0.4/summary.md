# Release of Vitess v24.0.4

## Summary

### lz4 backup engine: library upgrade and `--compression-level` mapping

The `lz4` compression engine now uses the `pierrec/lz4/v4` library instead of `pierrec/lz4` v2. This fixes broken block decoding on amd64 in the v2 library. The frame format is unchanged, so backups written by older Vitess versions remain restorable and backups written by this version are restorable by older versions.

The upgrade changes how `--compression-level` is interpreted for the lz4 engine. Values `0` and `1`, including the default of `1`, select the fast compressor. Values `2` through `9` now select lz4's named hash-chain levels (`Level2` through `Level9`) instead of using the raw value as the hash-chain search depth, so higher values produce a better ratio at more CPU cost. Values above `9` and negative values, which previously requested an unlimited search, select `Level9`. Other compression engines are not affected.

See [#20778](https://github.com/vitessio/vitess/pull/20778) for details.

### Connections whose certificate revocation cannot be checked against a configured CRL are rejected

The certificate revocation lists configured with `--grpc-crl`, `--mysql-server-ssl-crl`, `--tablet-grpc-crl`, `--vtgate-grpc-crl`, and the other `*-crl` flags are now enforced in every SSL mode and on resumed TLS sessions, see [GHSA-fxqj-c35w-x6rq](https://github.com/vitessio/vitess/security/advisories/GHSA-fxqj-c35w-x6rq). A CRL is held against the peer's verified certificate chain: each certificate of the chain, except the trust anchor it ends at, is looked up in the CRLs signed by its issuer, the next certificate of the chain. In the `preferred` and `required` SSL modes, where the server's certificate chain is otherwise not verified, a client with a CRL now builds that chain to the configured CA, or to the system roots when no CA is configured, and rejects the connection when that chain cannot be built or verified rather than leave the CRL unchecked: for example, a client in `required` mode with a CRL but no CA file, connecting to a server whose certificate a private CA issued; a client with a CRL and the root CA configured, connecting to a server that presents its certificate without the intermediate CA that issued it; or a client with a CRL connecting to a server whose certificate has expired or is not valid for server authentication, which those modes accept without a CRL. To keep such connections working, configure the CA that the server's chain leads to, have the server present its whole chain and a valid certificate, or drop the CRL.

Along with that:

- A peer passes when any of its verified chains holds no revoked certificate, as verification itself accepts the peer on any valid chain; every chain used to have to be clean. An intermediate CA cross-signed by two roots and revoked by one of them, as during a CA rollover, still carries the peer through the other root.
- A trust anchor, that is, a CA certificate in the CA file that a chain ends at, is trusted as configured and not checked against a CRL of its own issuer, unless that issuer is in the CA file as well and its CRL lists the anchor, or the anchor was issued, through CA certificates in the CA file, under one so listed: every chain that ends at such an anchor is then rejected, whether or not the peer presents it, and a warning names the CA certificate when the configuration is built. An intermediate CA configured without its root is not checked against the root's CRL.
- When a CRL file holds several complete CRLs from one issuer, only the newest applies, as a complete CRL supersedes the ones issued before it; they used to be combined.
- A CRL only applies when the certificate of its issuer in the chain validates it. A CA that signs its CRLs with a separate certificate under the same name is therefore not supported.
- A gRPC client with a CRL configured (`--tablet-grpc-crl`, `--vtgate-grpc-crl`, `--vtctld-grpc-crl`, and the other `*-grpc-crl` flags) but neither a client certificate nor a CA now connects with TLS, verifying the server against the system roots, rather than in plaintext with the CRL silently ignored. Configure the CA along with the CRL, or drop the CRL.

Several configurations that used to connect with the CRL silently ignored are now refused when the TLS configuration is built, at startup, since the CRL cannot be applied as configured:

- A server-side CRL (`--grpc-crl`, `--mysql-server-ssl-crl`) without the matching CA (`--grpc-ca`, `--mysql-server-ssl-ca`): without a CA no client certificate is requested, so the CRL could not apply. Configure the CA, or drop the CRL.
- A `*-crl` file that holds no CRL. Point the flag at a file with at least one `X509 CRL` block, or drop the flag.
- A CRL that the certificate of its issuer in the CA file does not validate: one whose signature does not verify against that certificate, or one signed by the key of a CA certificate that is not allowed to sign CRLs, that is, without the `cRLSign` key usage. Re-issue the CRL, or the CA certificate with `cRLSign`. When such an issuer is only found in a peer's chain, as an intermediate CA the peer presents, that peer's connections are rejected instead. A CRL whose authority key identifier names another key than the CA certificate's, as the CRL of a re-keyed CA's predecessor does, is not held against that CA's certificates; one that names another key while the certificate's key signed it is refused, since it would otherwise be passed over. Re-issue such a CRL with the certificate's subject key identifier as its authority key identifier.
- A CRL signed with an algorithm that is not supported.
- A CRL signed by a CA certificate in the CA file whose issuer name is encoded differently from that certificate's subject, as a CRL written by another tool than the CA's can be: the names are matched byte for byte, so the CRL would not be applied. Re-issue the CRL with the certificate's subject as its issuer.
- A delta CRL, an indirect CRL, or a CRL that its issuing distribution point limits to end-entity certificates, to CA certificates, to attribute certificates, or to some revocation reasons: only complete CRLs are supported. A CRL that names its distribution point without limiting itself otherwise is accepted, and every such partition of an issuer's CRL is applied.
- A CRL that carries a critical extension other than the issuing distribution point, on the list or on an entry.
- A CRL whose `thisUpdate` lies more than five minutes in the future, so that a CRL staged ahead of time cannot supersede the current one. Provide the current CRL, and check the clocks.

See [#21054](https://github.com/vitessio/vitess/pull/21054) for details.

### Table ACL: statements whose tables cannot be determined are denied under strict table ACL

Under strict table ACL (`--queryserver-config-strict-table-acl`), vttablet checks a statement against the tables its planner derives for it. `DO`, `CALL`, `REPAIR`, `OPTIMIZE` and `LOAD DATA` are parsed into nodes that discard their table-bearing text, so no permission was derived for them and the check had nothing to enforce: any authenticated caller could run them against tables the ACL denies, with vttablet's own MySQL privileges — a `DO` carrying a table-reading subquery or a `CALL` into a procedure body to read, and a server-side `LOAD DATA INFILE` to write. See [GHSA-w6mx-2f8x-pqf4](https://github.com/vitessio/vitess/security/advisories/GHSA-w6mx-2f8x-pqf4).

vttablet now fails closed: when it cannot determine a statement's tables, it denies the statement under strict table ACL rather than skip the check. With strict table ACL on, these five statements are denied for every caller outside the exempt ACL (`--queryserver-config-acl-exempt-acl`), including callers whose table grants would otherwise have sufficed, since the tablet cannot confirm which tables the statement touches. Operators who need them should issue them as a caller in the exempt ACL. With dry-run (`--queryserver-config-enable-table-acl-dry-run`) the denial is only recorded and the statement runs. Nothing changes with strict table ACL off.

These denials have no table to name, so they are counted under a new `TableName` label, `undetermined-table-set` (with an empty `TableGroup`), in `TableACLDenied`. With dry-run on, every non-exempt `DO`, `CALL`, `REPAIR`, `OPTIMIZE` and `LOAD DATA` from a request carrying a caller id increments `TableACLPseudoDenied` under that label whether or not strict table ACL is on, so operators sizing a strict-ACL rollout will see a new series appear.

The exported `planbuilder.BuildPermissions` now returns a second result, `tablesUndetermined bool`, alongside the permissions. This breaks any out-of-tree caller on purpose: a one-result compatibility wrapper would keep returning "no permissions" for exactly these statements with no way to learn the table set was undetermined, so a caller left on it would silently keep the behavior this fix closes. Callers should take the new result and deny the statement when it is true.

This covers statements. A stored **function** invoked inside an expression (`SELECT f()`, a `WHERE` clause, a `SET` in DML) is not `CALL`ed, so it still runs its body with vttablet's MySQL privileges while the ACL checks only the tables the statement itself names; Vitess does not parse `CREATE FUNCTION`, so this applies to functions defined directly in MySQL. That gap is tracked in [#21134](https://github.com/vitessio/vitess/issues/21134).

See [#21053](https://github.com/vitessio/vitess/pull/21053) for details.
