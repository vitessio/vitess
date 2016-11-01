// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package gateway

import (
	"fmt"

	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	return fmt.Sprintf("%s, %v", e.ShardIdentifier, e.Err)
}

// VtErrorCode returns the underlying Vitess error code.
// This is part of vterrors.VtError interface.
func (e *ShardError) VtErrorCode() vtrpcpb.ErrorCode {
	return e.ErrorCode
}

// NewShardError returns a ShardError which preserves the original
// error code if possible, adds the connection context and adds a bit
// to determine whether the keyspace/shard needs to be re-resolved for
// a potential sharding event (namely, if we were in a transaction).
func NewShardError(in error, target *querypb.Target, tablet *topodatapb.Tablet, inTransaction bool) error {
	if in == nil {
		return nil
	}
	var shardIdentifier string
	if tablet != nil {
		shardIdentifier = fmt.Sprintf("target: %s.%s.%s, used tablet: (%+v)", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType), tablet)
	} else {
		shardIdentifier = fmt.Sprintf("target: %s.%s.%s", target.Keyspace, target.Shard, topoproto.TabletTypeLString(target.TabletType))
	}

	return &ShardError{
		ShardIdentifier: shardIdentifier,
		InTransaction:   inTransaction,
		Err:             in,
		ErrorCode:       vterrors.RecoverVtErrorCode(in),
	}
}
