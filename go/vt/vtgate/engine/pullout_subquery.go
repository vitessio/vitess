/*
Copyright 2018 The Vitess Authors.

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
	"fmt"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ Primitive = (*PulloutSubquery)(nil)

// PulloutSubquery executes a "pulled out" subquery and stores
// the results in a bind variable.
type PulloutSubquery struct {
	Opcode         PulloutOpcode
	SubqueryResult string
	HasValues      string
	Subquery       Primitive
	Underlying     Primitive
}

// RouteType returns a description of the query routing type used by the primitive
func (ps *PulloutSubquery) RouteType() string {
	return ps.Opcode.String()
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (ps *PulloutSubquery) GetKeyspaceName() string {
	return ps.Underlying.GetKeyspaceName()
}

// GetTableName specifies the table that this primitive routes to.
func (ps *PulloutSubquery) GetTableName() string {
	return ps.Underlying.GetTableName()
}

// Execute satisfies the Primitive interface.
func (ps *PulloutSubquery) Execute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	combinedVars, err := ps.execSubquery(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	return ps.Underlying.Execute(ctx, vcursor, combinedVars, wantfields)
}

// StreamExecute performs a streaming exec.
func (ps *PulloutSubquery) StreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	combinedVars, err := ps.execSubquery(ctx, vcursor, bindVars)
	if err != nil {
		return err
	}
	return ps.Underlying.StreamExecute(ctx, vcursor, combinedVars, wantfields, callback)
}

// GetFields fetches the field info.
func (ps *PulloutSubquery) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+1)
	for k, v := range bindVars {
		combinedVars[k] = v
	}
	switch ps.Opcode {
	case PulloutValue:
		combinedVars[ps.SubqueryResult] = sqltypes.NullBindVariable
	case PulloutIn, PulloutNotIn:
		combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
		combinedVars[ps.SubqueryResult] = &querypb.BindVariable{
			Type:   querypb.Type_TUPLE,
			Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
		}
	case PulloutExists:
		combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
	}
	return ps.Underlying.GetFields(ctx, vcursor, combinedVars)
}

func (ps *PulloutSubquery) execSubquery(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (map[string]*querypb.BindVariable, error) {
	result, err := ps.Subquery.Execute(ctx, vcursor, bindVars, false)
	if err != nil {
		return nil, err
	}
	combinedVars := make(map[string]*querypb.BindVariable, len(bindVars)+1)
	for k, v := range bindVars {
		combinedVars[k] = v
	}
	switch ps.Opcode {
	case PulloutValue:
		switch len(result.Rows) {
		case 0:
			combinedVars[ps.SubqueryResult] = sqltypes.NullBindVariable
		case 1:
			if len(result.Rows[0]) != 1 {
				return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "subquery returned more than one column")
			}
			combinedVars[ps.SubqueryResult] = sqltypes.ValueBindVariable(result.Rows[0][0])
		default:
			return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "subquery returned more than one row")
		}
	case PulloutIn, PulloutNotIn:
		switch len(result.Rows) {
		case 0:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
			// Add a bogus value. It will not be checked.
			combinedVars[ps.SubqueryResult] = &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: []*querypb.Value{sqltypes.ValueToProto(sqltypes.NewInt64(0))},
			}
		default:
			if len(result.Rows[0]) != 1 {
				return nil, vterrors.New(vtrpcpb.Code_INVALID_ARGUMENT, "subquery returned more than one column")
			}
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(1)
			values := &querypb.BindVariable{
				Type:   querypb.Type_TUPLE,
				Values: make([]*querypb.Value, len(result.Rows)),
			}
			for i, v := range result.Rows {
				values.Values[i] = sqltypes.ValueToProto(v[0])
			}
			combinedVars[ps.SubqueryResult] = values
		}
	case PulloutExists:
		switch len(result.Rows) {
		case 0:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(0)
		default:
			combinedVars[ps.HasValues] = sqltypes.Int64BindVariable(1)
		}
	}
	return combinedVars, nil
}

// PulloutOpcode is a number representing the opcode
// for the PulloutSubquery primitive.
type PulloutOpcode int

// This is the list of PulloutOpcode values.
const (
	PulloutValue = PulloutOpcode(iota)
	PulloutIn
	PulloutNotIn
	PulloutExists
)

var pulloutName = map[PulloutOpcode]string{
	PulloutValue:  "PulloutValue",
	PulloutIn:     "PulloutIn",
	PulloutNotIn:  "PulloutNotIn",
	PulloutExists: "PulloutExists",
}

func (code PulloutOpcode) String() string {
	return pulloutName[code]
}

// MarshalJSON serializes the PulloutOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code PulloutOpcode) MarshalJSON() ([]byte, error) {
	return ([]byte)(fmt.Sprintf("\"%s\"", code.String())), nil
}
