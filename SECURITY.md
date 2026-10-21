# Security Release Process

Vitess is a large growing community of volunteers, users, and vendors. The Vitess community has
adopted this security disclosure and response policy to ensure we responsibly handle critical
issues.

## Deployment Trust Boundary

Application clients should connect through VTGate. Restrict VTTablet's gRPC endpoint to trusted
VTGate instances and trusted administrative and replication components, using network access
controls and appropriately scoped authentication credentials. Treat access to this endpoint as
administrative access, not as an application user's table-level grant.

VTTablet's query service and tablet manager service share a gRPC server and its authentication
interceptors. The built-in `static` and `mtls` authentication plugins do not distinguish permissions
by RPC method. When the tablet manager service is enabled, an authenticated caller can invoke
`ExecuteFetchAsDba`, which executes SQL as the tablet's DBA user without consulting table ACLs.
Granting unrelated services access through shared credentials or a broad
`--grpc-auth-mtls-allowed-substrings` pattern therefore grants more than ordinary query access.
The service map can disable services, and custom authentication plugins can restrict methods;
neither changes how the query service trusts forwarded caller IDs.

Table ACLs constrain queries forwarded by a trusted VTGate on behalf of application users.
VTGate derives the immediate caller ID from the client's authentication information, such as
MySQL authentication `UserData` or a verified gRPC client certificate. VTTablet trusts the
`ImmediateCallerId` username and groups supplied in the query request; it does not bind them
to the connection's authenticated identity. A caller with direct query-service access can
therefore claim another user's identity. Table ACLs do not isolate mutually untrusted callers
that have direct VTTablet access, even when the tablet manager service is disabled.

### VTGate Caller-ID Overrides

`--grpc-use-effective-callerid` allows VTGate gRPC clients to supply the immediate caller ID
through the effective caller ID's principal when no username is available from a verified
client certificate. This includes TLS connections without a verified client certificate and
verified certificates with an empty Common Name. Encryption alone does not prevent the override.
When this override supplies a non-empty principal, it takes precedence over
`--grpc-use-static-authentication-callerid`.

`--grpc-use-effective-groups` copies non-empty effective caller ID groups when the
`--grpc-use-effective-callerid` fallback is used. Both flags are disabled by default. Enable
them only for trusted clients: the principal and groups are client-supplied assertions, not
proof of the user's identity or group membership.

For configuration details, see the
[transport security model](https://vitess.io/docs/reference/features/transport-security-model/)
and [table authorization guide](https://vitess.io/docs/user-guides/configuration-advanced/authorization/).

## Maintainers Team

Security vulnerabilities should be handled quickly and sometimes privately. The primary goal of this
process is to reduce the total time users are vulnerable to publicly known exploits.

The Vitess [maintainers](MAINTAINERS.md) team is responsible for the entire response including internal communication and external disclosure. In the future, we may delegate responsibility to a sub-team as other projects have elected to do so.

## Disclosures

### Private Disclosure Processes

The Vitess community asks that all suspected vulnerabilities be privately and responsibly disclosed via the [reporting policy](README.md#reporting-security-vulnerabilities).

### Public Disclosure Processes

If you know of a publicly disclosed security vulnerability please IMMEDIATELY email
[vitess-maintainers](mailto:cncf-vitess-maintainers@lists.cncf.io) so that we may start the patch, release, and communication process.

## Patch, Release, and Public Communication

For each reported vulnerability, a member of the maintainers team will volunteer to lead coordination of the fix (Fix Lead), and ensure that it is backported to each supported branch. They will then coordinate with the remainder of the maintainers team to coordinate new releases and ensure a communication plan is in place for vulnerability disclosure.

All of the timelines below are suggestions and assume a private disclosure. The Fix Lead drives the
schedule using their best judgment based on severity and development time. If the Fix Lead is
dealing with a public disclosure all timelines become ASAP (assuming the vulnerability has a CVSS
score >= 4; see below). If the fix relies on another upstream project's disclosure timeline, that
will adjust the process as well. We will work with the upstream project to fit their timeline and
best protect our users.

#### Policy for main-only vulnerabilities

If a security vulnerability affects main, but not a currently supported branch, then the following process will apply:

* The fix will land in main.
* A courtesy notice will be posted in #developers on Vitess Slack.

#### Policy for unsupported releases

If a security vulnerability affects only a stable release which is no longer under active support, then the following process will apply:

* A fix **will not** be issued (exceptions may be made for extreme circumstances)
* A notice will be posted to Vitess Slack in #general channel identifying the threat, and encouraging users to upgrade.

#### Policy for supported releases 

If a security vulnerability affects supported branches, then a Fix Lead will be appointed and the full security process as defined below will apply.

### Fix Development Process

These steps should be completed within the 1-7 days of Disclosure.

- The Fix Lead will create a
  [CVSS](https://www.first.org/cvss/specification-document) using the [CVSS
  Calculator](https://www.first.org/cvss/calculator/3.0). The Fix Lead makes the final call on the
  calculated CVSS; it is better to move quickly than making the CVSS perfect.
- The Fix Lead will notify the maintainers that work on the fix branch is complete. Maintainers will review the fix branch in a private repo and provide require LGTMs.

If the CVSS score is under 4.0 ([a low severity
score](https://www.first.org/cvss/specification-document#i5)) the maintainers can decide to slow the
release process down in the face of holidays, developer bandwidth and other related circumstances.

### Fix Disclosure Process

With the fix development underway, the Fix Lead needs to come up with an overall communication plan
for the wider community. This Disclosure process should begin after the Fix Lead has developed a Fix
or mitigation so that a realistic timeline can be communicated to users.

**Disclosure of Forthcoming Fix to Users** (Completed within 1-7 days of Disclosure)

- The Fix Lead will update Vitess Slack informing users that a security vulnerability has been disclosed and that a fix will be made available at YYYY-MM-DD HH:MM UTC in the future via this list. This time is the Release Date.
- The Fix Lead will include any mitigating steps users can take until a fix is available.

The communication to users should be actionable. They should know when to block time to apply
patches, understand exact mitigation steps, etc.

**Disclosure of Fixed Vulnerability**

- The Fix Lead will post a notice on Vitess Slack informing users that there are new releases available to address an identified vulnerability.  
- As much as possible this notice should be actionable and include links to CVEs, and how to apply the fix to user's environments; this can include links to external distributor documentation.

### Embargo Policy

The Vitess team currently does not provide advance notice of undisclosed vulnerabilities to any third parties. We are open to feedback on what such a policy can or should look like.  For the interim, the best way to receive advanced notice of undisclosed vulnerabilities is to apply to join the maintainers team.
