/*
Copyright 2020 The Vitess Authors.

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

package engine

import (
	"context"
	"time"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Send)(nil)

// Send is an operator to send query to the specific keyspace, tabletType and destination
type Send struct {
	// Keyspace specifies the keyspace to send the query to.
	Keyspace *vindexes.Keyspace

	// TargetDestination specifies an explicit target destination to send the query to.
	TargetDestination key.Destination

	// Query specifies the query to be executed.
	Query string

	// IsDML specifies how to deal with autocommit behaviour
	IsDML bool

	// SingleShardOnly specifies that the query must be send to only single shard
	SingleShardOnly bool

	// ShardNameNeeded specified that the shard name is added to the bind variables
	ShardNameNeeded bool

	// MultishardAutocommit specifies that a multishard transaction query can autocommit
	MultishardAutocommit bool

	QueryTimeout int

	noInputs
}

// ShardName as key for setting shard name in bind variables map
const ShardName = "__vt_shard"

// NeedsTransaction implements the Primitive interface
func (s *Send) NeedsTransaction() bool {
	return s.IsDML
}

// RouteType implements Primitive interface
func (s *Send) RouteType() string {
	if s.IsDML {
		return "SendDML"
	}

	return "Send"
}

// GetKeyspaceName implements Primitive interface
func (s *Send) GetKeyspaceName() string {
	return s.Keyspace.Name
}

// GetTableName implements Primitive interface
func (s *Send) GetTableName() string {
	return ""
}

// TryExecute implements Primitive interface
func (s *Send) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, s.QueryTimeout)
	defer cancelFunc()
	rss, _, err := vcursor.ResolveDestinations(ctx, s.Keyspace.Name, nil, []key.Destination{s.TargetDestination})
	if err != nil {
		return nil, err
	}

	if !s.Keyspace.Sharded && len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}

	if s.SingleShardOnly && len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %s, got: %v", s.Query, s.TargetDestination)
	}

	queries := make([]*querypb.BoundQuery, len(rss))
	for i, rs := range rss {
		bv := bindVars
		if s.ShardNameNeeded {
			bv = copyBindVars(bindVars)
			bv[ShardName] = sqltypes.StringBindVariable(rs.Target.Shard)
		}
		queries[i] = &querypb.BoundQuery{
			Sql:           s.Query,
			BindVariables: bv,
		}
	}

	rollbackOnError := s.IsDML // for non-dml queries, there's no need to do a rollback
	result, errs := vcursor.ExecuteMultiShard(ctx, rss, queries, rollbackOnError, s.canAutoCommit(vcursor, rss))
	err = vterrors.Aggregate(errs)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Send) canAutoCommit(vcursor VCursor, rss []*srvtopo.ResolvedShard) bool {
	if s.IsDML {
		return (len(rss) == 1 || s.MultishardAutocommit) && vcursor.AutocommitApproval()
	}
	return false
}

func copyBindVars(in map[string]*querypb.BindVariable) map[string]*querypb.BindVariable {
	out := make(map[string]*querypb.BindVariable, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

// TryStreamExecute implements Primitive interface
func (s *Send) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if s.QueryTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(s.QueryTimeout)*time.Millisecond)
		defer cancel()
	}
	rss, _, err := vcursor.ResolveDestinations(ctx, s.Keyspace.Name, nil, []key.Destination{s.TargetDestination})
	if err != nil {
		return err
	}

	if !s.Keyspace.Sharded && len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}

	if s.SingleShardOnly && len(rss) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "Unexpected error, DestinationKeyspaceID mapping to multiple shards: %s, got: %v", s.Query, s.TargetDestination)
	}

	multiBindVars := make([]map[string]*querypb.BindVariable, len(rss))
	for i, rs := range rss {
		bv := bindVars
		if s.ShardNameNeeded {
			bv = copyBindVars(bindVars)
			bv[ShardName] = sqltypes.StringBindVariable(rs.Target.Shard)
		}
		multiBindVars[i] = bv
	}
	errors := vcursor.StreamExecuteMulti(ctx, s.Query, rss, multiBindVars, s.IsDML, s.canAutoCommit(vcursor, rss), callback)
	return vterrors.Aggregate(errors)
}

// GetFields implements Primitive interface
func (s *Send) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	qr, err := vcursor.ExecutePrimitive(ctx, s, bindVars, false)
	if err != nil {
		return nil, err
	}
	qr.Rows = nil
	return qr, nil
}

func (s *Send) description() PrimitiveDescription {
	other := map[string]any{
		"Query": s.Query,
		"Table": s.GetTableName(),
	}
	if s.IsDML {
		other["IsDML"] = true
	}
	if s.SingleShardOnly {
		other["SingleShardOnly"] = true
	}
	if s.ShardNameNeeded {
		other["ShardNameNeeded"] = true
	}
	if s.MultishardAutocommit {
		other["MultishardAutocommit"] = true
	}
	return PrimitiveDescription{
		OperatorType:      "Send",
		Keyspace:          s.Keyspace,
		TargetDestination: s.TargetDestination,
		Other:             other,
	}
}
