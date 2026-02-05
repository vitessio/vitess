# Vitess Protobuf Definitions

This directory contains all Vitess protobuf definitions.

Our protobuf messages are both used as wire format (e.g. `query.proto`) and for
storage (e.g. `topodata.proto`).

RPC messages and service definitions are in separate files (e.g. `vtgate.proto`
and `vtgateservice.proto`) on purpose because our internal deployment does not
use gRPC.

## Style Guide

Before creating new messages or services, please make yourself familiar with the
style of the existing definitions first.

Additionally, new definitions must adhere to the Google Cloud API Design Guide:
https://cloud.google.com/apis/design/

### Comments

We are more strict than the Design Guide on the format for comments. Similar to
comments for Go types or fields, protobuf comments must start with the name.
For example:
```protobuf
// TabletAlias is a globally unique tablet identifier.
message TabletAlias {
  // cell is the cell (or datacenter) the tablet is in.
  string cell = 1;
  ...
}
```

Note that the [Design Guide also has the following ask](https://cloud.google.com/apis/design/documentation#field_and_parameter_descriptions):

> If the field value is required, input only, output only, it must be documented
> at the start of the field description. By default, all fields and parameters
> are optional.

Here's an example which combines this ask with our stricter comments style:

```protobuf
// ExecuteKeyspaceIdsRequest is the payload to ExecuteKeyspaceIds.
message ExecuteKeyspaceIdsRequest {
  ...
  // Required. keyspace to target the query to.
  string keyspace = 4;
  ...
}
```

Note that most of our existing files (as of March 2017) do not have e.g.
`"Required."` comments. Nonetheless, new files should follow this where
applicable.
