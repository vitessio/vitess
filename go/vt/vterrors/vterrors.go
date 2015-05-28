// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package vterrors provides helpers for propagating internal errors through the Vitess
// system (including across RPC boundaries) in a structured way.
package vterrors

import (
	"fmt"

	mproto "github.com/youtube/vitess/go/mysql/proto"
)

const (
	// TabletError is the base VtTablet error. All VtTablet errors should be 4 digits, starting with 1.
	TabletError = 1000
	// UnknownTabletError is the code for an unknown error that came from VtTablet.
	UnknownTabletError = 1999
	// VtgateError is the base VTGate error code. All VTGate errors should be 4 digits, starting with 2.
	VtgateError = 2000
	// UnknownVtgateError is the code for an unknown error that came from VTGate.
	UnknownVtgateError = 2999
)

// VitessError is the error type that we use internally for passing structured errors
type VitessError struct {
	// Error code of the Vitess error
	Code int
	// Additional context string, distinct from the error message. For example, if
	// you wanted an error like "foo error: original error", the Message string
	// should be "foo error: "
	Message string
	err     error
}

// Error implements the error interface. For now, it should exactly recreate the original error string.
// It intentionally (for now) does not expose all the information that VitessError has. This
// is so that it can be used in the mixed state where parts of the stack are trying to parse
// error strings.
func (e *VitessError) Error() string {
	return fmt.Sprintf("%v", e.err)
}

// AsString returns a VitessError as a string, with more detailed information than Error().
func (e *VitessError) AsString() string {
	if e.Message != "" {
		return fmt.Sprintf("Code: %v, Message: %v, err: %v", e.Code, e.Message, e.err)
	}
	return fmt.Sprintf("Code: %v, err: %v", e.Code, e.err)
}

// FromRPCError recovers a VitessError from an RPCError (which is how VitessErrors
// are transmitted across RPC boundaries).
func FromRPCError(rpcErr mproto.RPCError) error {
	fmt.Printf("RPCError: %v\n", rpcErr)
	if rpcErr.Code == 0 && rpcErr.Message == "" {
		return nil
	}
	return &VitessError{
		Code: rpcErr.Code,
		err:  fmt.Errorf("%v", rpcErr.Message),
	}
}

// AddPrefix allows a string to be prefixed to an error, without nesting a new VitessError.
func AddPrefix(prefix string, in error) error {
	vtErr, ok := in.(*VitessError)
	if !ok {
		return fmt.Errorf("%s: %s", prefix, in)
	}

	return &VitessError{
		Code:    vtErr.Code,
		err:     vtErr.err,
		Message: fmt.Sprintf("%s: %s", prefix, vtErr.Message),
	}
}
