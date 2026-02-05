# API Server

VTAdmin uses [`cmux`](https://github.com/soheilhy/cmux) to multiplex gRPC and
HTTP traffic over a single port.

Internally, VTAdmin has an adaptation layer (see [`go/vt/vtadmin/http/api.go`][http/api.go]) that
converts query parameters into protobuf messages, so that all HTTP and gRPC requests
are powered by the same implementation.

For a full listing of the available gRPC methods, see [vtadmin.proto][vtadmin.proto],
and for a full listing of available HTTP routes, see [`go/vt/vtadmin/api.go`][vtadmin/api.go].

[http/api.go]: https://github.com/vitessio/vitess/blob/a787cd1e80ef2392bfe1050dd0c5ddd4efde066f/go/vt/vtadmin/http/api.go#L64-L84
[vtadmin.proto]: ../../../proto/vtadmin.proto
[vtadmin/api.go]: https://github.com/vitessio/vitess/blob/a787cd1e80ef2392bfe1050dd0c5ddd4efde066f/go/vt/vtadmin/api.go#L153-L191
