/*
Copyright 2023 The Vitess Authors.

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
)

// Sequential Primitive is used to execute DML statements in a fixed order.
// Any failure, stops the execution and returns.
type Sequential struct {
	txNeeded
	noFields

	Sources []Primitive
}

var _ Primitive = (*Sequential)(nil)

// NewSequential creates a Sequential primitive.
func NewSequential(Sources []Primitive) *Sequential {
	return &Sequential{
		Sources: Sources,
	}
}

// TryExecute performs a non-streaming exec.
func (s *Sequential) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantFields bool) (*sqltypes.Result, error) {
	finalRes := &sqltypes.Result{}
	for _, source := range s.Sources {
		res, err := vcursor.ExecutePrimitive(ctx, source, bindVars, wantFields)
		if err != nil {
			return nil, err
		}
		finalRes.RowsAffected += res.RowsAffected
		if finalRes.InsertID == 0 {
			finalRes.InsertID = res.InsertID
		}
		if res.Info != "" {
			finalRes.Info = res.Info
		}
	}
	return finalRes, nil
}

// TryStreamExecute performs a streaming exec.
func (s *Sequential) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantFields bool, callback func(*sqltypes.Result) error) error {
	qr, err := s.TryExecute(ctx, vcursor, bindVars, wantFields)
	if err != nil {
		return err
	}
	return callback(qr)
}

// Inputs returns the input primitives for this
func (s *Sequential) Inputs() ([]Primitive, []map[string]any) {
	return s.Sources, nil
}

func (s *Sequential) description() PrimitiveDescription {
	return PrimitiveDescription{OperatorType: "Sequential"}
}
