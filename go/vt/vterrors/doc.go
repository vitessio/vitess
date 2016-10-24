// Package vterrors provides helpers for propagating internal errors
// through the Vitess system (including across RPC boundaries) in a
// structured way.
package vterrors

/*

Vitess uses canonical error codes for error reporting. This is based
on years of industry experience with error reporting. This idea is
that errors should be classified into a small set of errors (10 or so)
with very specific meaning. Each error has a code, and a message. When
errors are passed around (even through RPCs), the code is
propagated. To handle errors, only the code should be looked at (and
not string-matching on the error message).

For instance, see this document for the Google Cloud Error Codes.
https://cloud.google.com/datastore/docs/concepts/errors

Vitess defines the error codes in /proto/vtrpc.proto. Along with an
RPCError message that can be used to transmit errors through RPCs, in
the message payloads.

Vitess then defines the VtError interface, for all errors that have a code.
See vterrors.go in this library.

Vitess also defines a VitessError error implementation, that can wrap
any error and add a code to it.

To easily transmit these codes through gRPC, we map these codes to
gRPC error codes in grpc.go, in this library. So if a gRPC call only
returns an error, we return a gRPC error with the right gRPC error
code. If a gRPC call needs to return both an error and some data (like
vtgateservice.Execute that can return an updated Session along with
the error), we can just return an RPCError in the result.

Some libraries define their own error structures that implement the
VtError interface. Usually, it is to add extra data to it. For an
example, see ../tabletserver/tablet_error.go that adds the SQL error
codes to the error structure. These SQL errors however are all mapped
to their appropriate canonical error code, see the function NewTabletErrorSQL
in that file for the mapping.

When transmitting any error through RPC boundaries, we are careful to
always preserve the error code.  When augmenting / aggregating errors,
we also preserve the error codes:
- See WithPrefix and WithSuffix in this package for augmentation.
- See aggregate.go in this package for aggregation.

*/
