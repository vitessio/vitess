# Release of Vitess v23.0.7

## Summary

### lz4 backup engine: library upgrade and `--compression-level` mapping

The `lz4` compression engine now uses the `pierrec/lz4/v4` library instead of `pierrec/lz4` v2. This fixes broken block decoding on amd64 in the v2 library. The frame format is unchanged, so backups written by older Vitess versions remain restorable and backups written by this version are restorable by older versions.

The upgrade changes how `--compression-level` is interpreted for the lz4 engine. Values `0` and `1`, including the default of `1`, select the fast compressor; `1` previously requested a hash-chain search depth of 1. Values `2` through `9` now select lz4's named hash-chain levels (`Level2` through `Level9`) instead of using the raw value as the hash-chain search depth, so higher values produce a better ratio at more CPU cost. Negative values, which previously requested an unlimited search, and values above `9`, which were used as the search depth, select `Level9`. Other compression engines are not affected.

See [#20778](https://github.com/vitessio/vitess/pull/20778) for details.

### Connections whose certificate revocation cannot be checked against a configured CRL are rejected

The certificate revocation lists configured with `--grpc-crl`, `--mysql-server-ssl-crl`, `--tablet-grpc-crl`, `--vtgate-grpc-crl`, and the other `*-crl` flags are now enforced in every SSL mode and on resumed TLS sessions, see [GHSA-fxqj-c35w-x6rq](https://github.com/vitessio/vitess/security/advisories/GHSA-fxqj-c35w-x6rq). Each certificate of the peer's verified chain, except the trust anchor the chain ends at, is checked against the CRLs signed by its issuer, the next certificate of the chain.

#### Connections that are now rejected

In the `preferred` and `required` SSL modes, the server's certificate chain is otherwise not verified. A client with a CRL now builds that chain to the configured CA, or to the system roots when no CA is configured, and rejects the connection when the chain cannot be built or verified, rather than leave the CRL unchecked. For example:

- A client in `required` mode with a CRL but no CA file, connecting to a server whose certificate a private CA issued.
- A client with a CRL and the root CA configured, connecting to a server that presents its certificate without the intermediate CA that issued it.
- A client with a CRL connecting to a server whose certificate has expired or is not valid for server authentication, which these modes accept without a CRL.

To keep such connections working, configure the CA that the server's chain leads to, have the server present its whole chain and a valid certificate, or drop the CRL.

A gRPC client with a CRL configured (`--tablet-grpc-crl`, `--vtgate-grpc-crl`, `--vtctld-grpc-crl`, and the other `*-grpc-crl` flags) but neither a client certificate nor a CA now connects with TLS, verifying the server against the system roots, rather than in plaintext with the CRL silently ignored. Configure the CA along with the CRL, or drop the CRL.

#### Configurations that are refused at startup

The following configurations are now refused when the TLS configuration is built, at startup, because the CRL cannot be applied as configured. Most of them used to run with the CRL silently ignored.

- A server-side CRL (`--grpc-crl`, `--mysql-server-ssl-crl`) without the matching CA (`--grpc-ca`, `--mysql-server-ssl-ca`). Without a CA no client certificate is requested, so the CRL could not apply. Configure the CA, or drop the CRL.
- A server-side CRL (`--grpc-crl`, `--mysql-server-ssl-crl`) without the matching certificate and key (`--grpc-cert` and `--grpc-key`, `--mysql-server-ssl-cert` and `--mysql-server-ssl-key`). The server is then not configured for TLS at all: the gRPC server used to start in plaintext, and the MySQL server without TLS, with the CRL silently ignored. Configure the certificate and the key along with the CA, or drop the CRL.
- A `*-crl` file that holds no CRL. Point the flag at a file with at least one `X509 CRL` block, or drop the flag.
- A CRL that the certificate of its issuer in the CA file does not validate: its signature does not verify against that certificate, or that certificate lacks the `cRLSign` key usage. Re-issue the CRL, or the CA certificate with `cRLSign`. When such an issuer is only found in a peer's chain, as an intermediate CA the peer presents, that peer's connections are rejected instead.
- A CRL whose authority key identifier names a different key than its issuing CA certificate's, although that certificate's key signed it. It would otherwise be passed over. Re-issue it with the certificate's subject key identifier as its authority key identifier.
- A CRL signed with an algorithm that is not supported.
- A CRL whose issuer name is encoded differently from the subject of the CA certificate in the CA file that signed it, as a CRL written by another tool than the CA's can be. The names are matched byte for byte, so the CRL would not be applied. Re-issue the CRL with the certificate's subject as its issuer.
- A delta CRL, an indirect CRL, or a CRL that its issuing distribution point limits to end-entity certificates, to CA certificates, to attribute certificates, or to some revocation reasons: only complete CRLs are supported. A CRL that names its distribution point without limiting itself otherwise is accepted, and every such partition of an issuer's CRL is applied.
- A CRL that carries a critical extension other than the issuing distribution point, on the list or on an entry.
- A CRL whose `thisUpdate` lies more than five minutes in the future, so that a CRL staged ahead of time cannot supersede the current one. Provide the current CRL, and check the clocks.

#### Other changes to how CRLs are applied

- A peer passes when any of its verified chains holds no revoked certificate, as verification itself accepts the peer on any valid chain; every chain used to have to be clean. An intermediate CA cross-signed by two roots and revoked by one of them, as during a CA rollover, still carries the peer through the other root.
- A trust anchor, that is, a CA certificate in the CA file that a chain ends at, is trusted as configured and not checked against its issuer's CRL. The exception is an anchor whose issuer is in the CA file as well and whose issuer's CRL lists it, or an anchor issued, through CA certificates in the CA file, under a CA certificate listed that way. Every chain that ends at such an anchor is rejected, whether or not the peer presents it, and a warning names the CA certificate when the configuration is built. An intermediate CA configured without its root is not checked against the root's CRL.
- When a CRL file holds several complete CRLs from one issuer, only the newest applies, as a complete CRL supersedes the ones issued before it; they used to be combined.
- A CRL only applies when the certificate of its issuer in the chain validates it. A CA that signs its CRLs with a separate certificate under the same name is therefore not supported.
- A CRL whose authority key identifier names a different key than the CA certificate's, as the CRL of a re-keyed CA's predecessor does, is not held against that CA's certificates.

See [#21054](https://github.com/vitessio/vitess/pull/21054) and [#21153](https://github.com/vitessio/vitess/pull/21153) for details.

### Table ACL: statements whose tables cannot be determined are denied under strict table ACL

Under strict table ACL (`--queryserver-config-strict-table-acl`), vttablet checks a statement against the tables its planner derives for it. `DO`, `CALL`, `REPAIR`, `OPTIMIZE` and `LOAD DATA` are parsed into nodes that discard their table-bearing text, so no permission was derived for them and the check had nothing to enforce: any authenticated caller could run them against tables the ACL denies, with vttablet's own MySQL privileges — a `DO` carrying a table-reading subquery or a `CALL` into a procedure body to read, and a server-side `LOAD DATA INFILE` to write. See [GHSA-w6mx-2f8x-pqf4](https://github.com/vitessio/vitess/security/advisories/GHSA-w6mx-2f8x-pqf4).

vttablet now fails closed: when it cannot determine a statement's tables, it denies the statement under strict table ACL rather than skip the check. With strict table ACL on, these five statements are denied for every caller outside the exempt ACL (`--queryserver-config-acl-exempt-acl`), including callers whose table grants would otherwise have sufficed, since the tablet cannot confirm which tables the statement touches. Operators who need them should issue them as a caller in the exempt ACL. With dry-run (`--queryserver-config-enable-table-acl-dry-run`) the denial is only recorded and the statement runs. Nothing changes with strict table ACL off.

These denials have no table to name, so they are counted under a new `TableName` label, `undetermined-table-set` (with an empty `TableGroup`), in `TableACLDenied`. With dry-run on, every non-exempt `DO`, `CALL`, `REPAIR`, `OPTIMIZE` and `LOAD DATA` from a request carrying a caller id increments `TableACLPseudoDenied` under that label whether or not strict table ACL is on, so operators sizing a strict-ACL rollout will see a new series appear.

The exported `planbuilder.BuildPermissions` now returns a second result, `tablesUndetermined bool`, alongside the permissions. This breaks any out-of-tree caller on purpose: a one-result compatibility wrapper would keep returning "no permissions" for exactly these statements with no way to learn the table set was undetermined, so a caller left on it would silently keep the behavior this fix closes. Callers should take the new result and deny the statement when it is true.

This covers statements. A stored **function** invoked inside an expression (`SELECT f()`, a `WHERE` clause, a `SET` in DML) is not `CALL`ed, so it still runs its body with vttablet's MySQL privileges while the ACL checks only the tables the statement itself names; Vitess does not parse `CREATE FUNCTION`, so this applies to functions defined directly in MySQL. That gap is tracked in [#21134](https://github.com/vitessio/vitess/issues/21134).

See [#21053](https://github.com/vitessio/vitess/pull/21053) for details.

### Table ACL: reads embedded in DDL, `EXPLAIN`, `SHOW`, `SET`, and queries against `dual` are checked under strict table ACL

This completes the fix for [GHSA-w6mx-2f8x-pqf4](https://github.com/vitessio/vitess/security/advisories/GHSA-w6mx-2f8x-pqf4) begun [above](#table-acl-statements-whose-tables-cannot-be-determined-are-denied-under-strict-table-acl): that change fails closed on statements whose tables the parser discards; this one derives permissions for reads embedded in statements the planner does parse but never checked, so a caller with no grant on a table could read it through them. Each read is now checked like a plain `SELECT` of the same tables, under strict table ACL (`--queryserver-config-strict-table-acl`), with the same dry-run and exempt-ACL behavior as any other table ACL check:

- `CREATE TABLE ... AS SELECT` requires `READER` on the tables the `SELECT` reads (through CTEs, joins and unions included), in addition to `ADMIN` on the table it creates. `CREATE VIEW ... AS SELECT` and `ALTER VIEW ... AS SELECT` require the same `READER` on their source tables: a view reads nothing when it is defined, but it reads its sources as the tablet's MySQL user whenever it is queried, and the ACL then sees only the view's name, so the source is checked when the view is defined, as MySQL requires `SELECT` on it.
- `EXPLAIN`, in any format, and `DESCRIBE <statement>` now require the explained statement's permissions, `WRITER` on the target of a DML included, as MySQL requires the explained statement's privileges. `EXPLAIN ANALYZE` executes the statement, and a plain `EXPLAIN` reads too: MySQL reads single-row tables and evaluates uncorrelated subqueries while it optimizes, and the plan shows the outcome (`Impossible WHERE`), so an `EXPLAIN` answers a yes/no question about the data.
- `SHOW ... WHERE <expr>` requires `READER` on the tables read by any subquery in the filter, which MySQL evaluates. The `SHOW`'s own subject (the table of `SHOW COLUMNS FROM t`) remains unchecked. This covers `SHOW VITESS_MIGRATIONS ... WHERE` as well.
- `SET` requires `READER` on the tables read by any subquery in its expressions.
- A query against `dual` requires `READER` on the tables read by any subquery in it, such as `SELECT (SELECT v FROM t) FROM dual`. vttablet exempted a query against `dual` from the table ACL entirely, subqueries included; only a query that reads nothing but `dual` is exempt now.

A `CREATE TABLE` that vttablet's parser cannot fully parse is forwarded to MySQL as the client's raw text, with only the `CREATE TABLE <name>` prefix known to the planner. Some such statements copy rows from a table the planner never sees (`CREATE TABLE t (SELECT ...)`, `CREATE TABLE t AS TABLE src`, an `EXCEPT` or `INTERSECT` source), and the planner cannot tell them from a valid statement in syntax Vitess lacks. Every partially parsed `CREATE TABLE` is therefore treated as a statement whose tables cannot be determined and denied the same way, for callers outside the exempt ACL; a `CREATE TABLE` in syntax vttablet does not parse must be issued by a caller in the exempt ACL. Parsing these sources is tracked in [#21138](https://github.com/vitessio/vitess/issues/21138).

A statement flagged this way now has the permissions the planner did derive checked first, so a caller lacking `ADMIN` on the table a partial `CREATE TABLE` creates is denied on that table by name, and a dry run records both that denial and the undetermined one.

Connection settings — the SET statements vtgate attaches to a session's queries, and the pre-queries of a reservation — are applied to a connection with no table ACL check. Under strict table ACL, vttablet now rejects a setting whose expressions contain a subquery, and a reservation pre-query it cannot parse as a `SET`: settings carry constants, and vtgate only sends values. Without strict table ACL every setting is accepted as before, since there is nothing for the check to protect. With table ACL dry run (`--queryserver-config-enable-table-acl-dry-run`), the setting is accepted as well, as dry run lets through any request the table ACL would deny, and vttablet logs a throttled warning naming the setting that strict table ACL would reject (with `--sanitize-log-messages`, only the variables it sets). So that a targeted session's settings are constants too, vtgate now evaluates a targeted `SET` of a system variable before storing it; see [below](#vtgate-a-targeted-sessions-set-of-a-system-variable-is-evaluated-once).

See [#21139](https://github.com/vitessio/vitess/pull/21139) for details.

### VTGate: a targeted session's `SET` of a system variable is evaluated once

A `SET` of a system variable in a session targeted at a shard (`use ks:-80`) is now evaluated once on the target shard, with the tablet's table ACL checking any read, and the session applies and stores the resulting value, as an untargeted session does. Previously such a session stored the expression as written and re-evaluated it on every reserved connection. As a result:

- `SELECT @@var` after a non-constant targeted `SET` now returns the value instead of failing to evaluate the stored text.
- A targeted `SET` that fails no longer leaves its value in the session.
- Each targeted `SET` costs one additional round trip to the shard.

**Compatibility note:** a vtgate without this change (before v23.0.7 or v24.0.4) still stores a targeted session's `SET` expression as written. Against a v23.0.7 vttablet running strict table ACL without dry run, a session on such a vtgate that runs `SET @@var = (<subquery>)` while targeted has that setting rejected on every later query until the client reconnects. In the usual upgrade order, vttablets before vtgates, this applies for as long as the upgrade lasts: avoid subqueries in targeted `SET` statements until every vtgate is upgraded, or upgrade vtgates first. Without strict table ACL nothing changes for such a session.

See [#21139](https://github.com/vitessio/vitess/pull/21139) for details.

### Legacy vtctld HTTP API removed

The HTTP API that vtctld served under `/api/` has been removed because it was dead code that exposed a security attack surface. It was built for the vtctld web UI, which VTAdmin replaced in v16. Nothing has served or called it since, and VTAdmin reaches vtctld over gRPC. What remained was an unauthenticated HTTP surface that served topology data, tablet health, arbitrary vtctl commands, schema changes, and keyspace and shard validations. Its `--security-policy` coverage varied from one endpoint to the next. Removing it removes that surface, rather than patching it endpoint by endpoint.

The removed endpoints are `cells`, `keyspaces`, `keyspace`, `shards`, `srv_keyspace`, `tablets`, `tablet_statuses`, `tablet_health`, `topology_info`, `topodata`, `vtctl`, `schema/apply`, and `features`, and the keyspace, shard, and tablet action endpoints behind them. The `--cell`, `--proxy-tablets`, `--action-timeout`, and `--tablet-health-keep-alive` flags of vtctld and vtcombo that configured the API are now deprecated no-ops, so a process started with them still starts. They will be removed in v26.

**Migration**: use `vtctldclient`, or the `VtctldServer` gRPC service it calls, for anything a script did against `/api/`. Remove `--cell`, `--proxy-tablets`, `--action-timeout`, and `--tablet-health-keep-alive` from vtctld and vtcombo startup arguments. The `/debug/health` and `/debug/status` endpoints are unchanged.

**Impact**: requests to `/api/` on vtctld's HTTP port return `404 Not Found`. Passing one of the four flags logs a deprecation warning and has no effect. For anyone who builds on the Go packages, `vtctld.InitVtctld`, `vtctld.ActionRepository`, `vtctld.ActionResult`, and `vtctld.TabletWithURL` are gone.

See [#21171](https://github.com/vitessio/vitess/pull/21171) for the removal and [#21170](https://github.com/vitessio/vitess/issues/21170) for the removal of the flags in v26.

### Optional gRPC TLS: connections are counted by transport

A gRPC server started with `--grpc-enable-optional-tls` now reports its connections by transport, `tls` or `plaintext`, in two new stats: `GrpcOptionalTlsOpenConnections`, the connections currently open, and `GrpcOptionalTlsConnections`, the connections handshaken so far. Optional TLS serves plain-text connections unauthenticated so that clients can be moved to TLS one at a time, including when `--grpc-ca` is set, whose client certificate check only applies to the TLS connections. The stats are the evidence to check before dropping `--grpc-enable-optional-tls`: the first shows whether a plain-text client is connected right now, which matters because gRPC connections are long-lived and a client that connected long ago does not handshake again, and the second whether any has connected lately. Neither shows a client that is offline or connects only now and then, so they support the decision rather than prove it. A server that has both flags also says so in its startup warning now.

See [#21162](https://github.com/vitessio/vitess/pull/21162) for details.
