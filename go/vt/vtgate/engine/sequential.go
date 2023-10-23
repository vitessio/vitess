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

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// Concatenate Primitive is used to concatenate results from multiple sources.
var _ Primitive = (*Sequential)(nil)

// Sequential specified the parameter for concatenate primitive
type Sequential struct {
	Sources []Primitive
}

// NewSequential creates a Sequential primitive.
func NewSequential(Sources []Primitive) *Sequential {
	return &Sequential{
		Sources: Sources,
	}
}

// RouteType returns a description of the query routing type used by the primitive
func (s *Sequential) RouteType() string {
	return "Sequential"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to
func (s *Sequential) GetKeyspaceName() string {
	res := s.Sources[0].GetKeyspaceName()
	for i := 1; i < len(s.Sources); i++ {
		res = formatTwoOptionsNicely(res, s.Sources[i].GetKeyspaceName())
	}
	return res
}

// GetTableName specifies the table that this primitive routes to.
func (s *Sequential) GetTableName() string {
	res := s.Sources[0].GetTableName()
	for i := 1; i < len(s.Sources); i++ {
		res = formatTwoOptionsNicely(res, s.Sources[i].GetTableName())
	}
	return res
}

// TryExecute performs a non-streaming exec.
func (s *Sequential) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantFields bool) (*sqltypes.Result, error) {
	var finalRes *sqltypes.Result
	for _, source := range s.Sources {
		res, err := source.TryExecute(ctx, vcursor, bindVars, wantFields)
		if err != nil {
			return nil, err
		}
		if finalRes == nil {
			finalRes = res
		}
	}
	return finalRes, nil
}

// TryStreamExecute performs a streaming exec.
func (s *Sequential) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantFields bool, callback func(*sqltypes.Result) error) error {
	for _, source := range s.Sources {
		err := source.TryStreamExecute(ctx, vcursor, bindVars, wantFields, callback)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetFields fetches the field info.
func (s *Sequential) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	res, err := s.Sources[0].GetFields(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	columns := make([][]sqltypes.Type, len(res.Fields))

	addFields := func(fields []*querypb.Field) {
		for idx, field := range fields {
			columns[idx] = append(columns[idx], field.Type)
		}
	}

	addFields(res.Fields)

	for i := 1; i < len(s.Sources); i++ {
		result, err := s.Sources[i].GetFields(ctx, vcursor, bindVars)
		if err != nil {
			return nil, err
		}
		addFields(result.Fields)
	}

	// The resulting column types need to be the coercion of all the input columns
	for colIdx, t := range columns {
		res.Fields[colIdx].Type = evalengine.AggregateTypes(t)
	}

	return res, nil
}

// NeedsTransaction returns whether a transaction is needed for this primitive
func (s *Sequential) NeedsTransaction() bool {
	return true
}

// Inputs returns the input primitives for this
func (s *Sequential) Inputs() ([]Primitive, []map[string]any) {
	return s.Sources, nil
}

func (s *Sequential) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: s.RouteType()}
}
