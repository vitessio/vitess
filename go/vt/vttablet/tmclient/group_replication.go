/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package tmclient

import (
	"errors"

	"vitess.io/vitess/go/vt/vterrors"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// GroupBootstrapRefusedError is the error of a StartGroupReplication bootstrap that the tablet refused
// definitively: the request did not start MySQL's START GROUP_REPLICATION, and never will. The tablet
// returns it only when MySQL lacks a transaction that the request requires
// (StartGroupReplicationRequest.required_gtid_set), as it found under its action lock, while no START
// GROUP_REPLICATION ran on MySQL. Every START of the tablet runs under that lock, and MySQL starts
// none on its own, so none could start afterwards for the request either.
//
// Over gRPC, the tablet reports it in StartGroupReplicationResponse.definitive_refusal when the
// request asks for it (report_definitive_refusal), and the client returns this error again. Any
// other error, a timeout or a transport error included, may hide a bootstrap that MySQL still runs.
//
// Its code is the refusal's, FAILED_PRECONDITION. Wrapping it with vterrors hides it from
// IsGroupBootstrapRefused, which only sees through errors that implement Unwrap.
type GroupBootstrapRefusedError struct {
	err error
}

// NewGroupBootstrapRefusedError returns err, the reason for a definitive refusal of a bootstrap, as
// a GroupBootstrapRefusedError.
func NewGroupBootstrapRefusedError(err error) *GroupBootstrapRefusedError {
	return &GroupBootstrapRefusedError{err: err}
}

// Error is part of the error interface.
func (e *GroupBootstrapRefusedError) Error() string {
	return e.err.Error()
}

// Unwrap returns the reason for the refusal.
func (e *GroupBootstrapRefusedError) Unwrap() error {
	return e.err
}

// ErrorCode is part of the vterrors.ErrorWithCode interface: the code of the reason for the
// refusal.
func (e *GroupBootstrapRefusedError) ErrorCode() vtrpcpb.Code {
	return vterrors.Code(e.err)
}

// IsGroupBootstrapRefused returns whether err is a GroupBootstrapRefusedError: the tablet refused
// the bootstrap definitively.
func IsGroupBootstrapRefused(err error) bool {
	_, ok := errors.AsType[*GroupBootstrapRefusedError](err)
	return ok
}
