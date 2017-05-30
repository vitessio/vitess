/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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

Vitess defines the error codes in /proto/vtrpc.proto. Along with an
RPCError message that can be used to transmit errors through RPCs, in
the message payloads. These codes match the names and numbers defined
by gRPC.

Vitess also defines a standardized error implementation that allows
you to build an error with an associated canonical code.

While sending an error through gRPC, these codes are transmitted
using gRPC's error propagation mechanism and decoded back to
the original code on the other end.

*/
