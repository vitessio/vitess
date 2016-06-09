// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gateway

import (
	"fmt"

	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// ShardError is the error about a specific shard.
// It implements vterrors.VtError.
type ShardError struct {
	// ShardIdentifier is the keyspace+shard.
	ShardIdentifier string
	// InTransaction indicates if it is inside a transaction.
	InTransaction bool
	// Err preserves the original error, so that we don't need to parse the error string.
	Err error
	// ErrorCode is the error code to use for all the tablet errors in aggregate
	ErrorCode vtrpcpb.ErrorCode
}

// Error returns the error string.
func (e *ShardError) Error() string {
	if e.ShardIdentifier == "" {
		return fmt.Sprintf("%v", e.Err)
	}
	return fmt.Sprintf("shard, host: %s, %v", e.ShardIdentifier, e.Err)
}

// VtErrorCode returns the underlying Vitess error code.
// This is part of vterrors.VtError interface.
func (e *ShardError) VtErrorCode() vtrpcpb.ErrorCode {
	return e.ErrorCode
}
