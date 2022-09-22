/*
Copyright 2019 The Vitess Authors.

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
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ Primitive = (*VindexFunc)(nil)

// VindexFunc is a primitive that performs vindex functions.
type VindexFunc struct {
	Opcode VindexOpcode
	// Fields is the field info for the result.
	Fields []*querypb.Field
	// Cols contains source column numbers: 0 for id, 1 for keyspace_id.
	Cols []int
	// TODO(sougou): add support for MultiColumn.
	Vindex vindexes.SingleColumn
	Value  evalengine.Expr

	// VindexFunc does not take inputs
	noInputs

	// VindexFunc does not need to work inside a tx
	noTxNeeded
}

// VindexOpcode is the opcode for a VindexFunc.
type VindexOpcode int

// These are opcode values for VindexFunc.
const (
	VindexNone = VindexOpcode(iota)
	VindexMap
	NumVindexCodes
)

var vindexOpcodeName = map[VindexOpcode]string{
	VindexMap: "VindexMap",
}

// MarshalJSON serializes the VindexOpcode into a JSON representation.
// It's used for testing and diagnostics.
func (code VindexOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(vindexOpcodeName[code])
}

// RouteType returns a description of the query routing type used by the primitive
func (vf *VindexFunc) RouteType() string {
	return vindexOpcodeName[vf.Opcode]
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (vf *VindexFunc) GetKeyspaceName() string {
	return ""
}

// GetTableName specifies the table that this primitive routes to.
func (vf *VindexFunc) GetTableName() string {
	return ""
}

// TryExecute performs a non-streaming exec.
func (vf *VindexFunc) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return vf.mapVindex(ctx, vcursor, bindVars)
}

// TryStreamExecute performs a streaming exec.
func (vf *VindexFunc) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	r, err := vf.mapVindex(ctx, vcursor, bindVars)
	if err != nil {
		return err
	}
	if err := callback(&sqltypes.Result{Fields: r.Fields}); err != nil {
		return err
	}
	return callback(&sqltypes.Result{Rows: r.Rows})
}

// GetFields fetches the field info.
func (vf *VindexFunc) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{Fields: vf.Fields}, nil
}

func (vf *VindexFunc) mapVindex(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	k, err := env.Evaluate(vf.Value)
	if err != nil {
		return nil, err
	}
	var values []sqltypes.Value
	if k.Value().Type() == querypb.Type_TUPLE {
		values = k.TupleValues()
	} else {
		values = append(values, k.Value())
	}
	result := &sqltypes.Result{
		Fields: vf.Fields,
	}
	for _, value := range values {
		vkey, err := evalengine.Cast(value, sqltypes.VarBinary)
		if err != nil {
			return nil, err
		}
		destinations, err := vf.Vindex.Map(ctx, vcursor, []sqltypes.Value{value})
		if err != nil {
			return nil, err
		}
		switch d := destinations[0].(type) {
		case key.DestinationKeyRange:
			if d.KeyRange != nil {
				row, err := vf.buildRow(vkey, nil, d.KeyRange)
				if err != nil {
					return result, err
				}
				result.Rows = append(result.Rows, row)
			}
		case key.DestinationKeyspaceID:
			if len(d) > 0 {
				if vcursor != nil {
					resolvedShards, _, err := vcursor.ResolveDestinations(ctx, vcursor.GetKeyspace(), nil, []key.Destination{d})
					if err != nil {
						return nil, err
					}
					if len(resolvedShards) > 0 {
						kr, err := key.ParseShardingSpec(resolvedShards[0].Target.Shard)
						if err != nil {
							return nil, err
						}
						row, err := vf.buildRow(vkey, d, kr[0])
						if err != nil {
							return result, err
						}
						result.Rows = append(result.Rows, row)
						break
					}
				}

				row, err := vf.buildRow(vkey, d, nil)
				if err != nil {
					return result, err
				}
				result.Rows = append(result.Rows, row)
			}
		case key.DestinationKeyspaceIDs:
			for _, ksid := range d {
				row, err := vf.buildRow(vkey, ksid, nil)
				if err != nil {
					return result, err
				}
				result.Rows = append(result.Rows, row)
			}
		case key.DestinationNone:
			// Nothing to do.
		default:
			return result, vterrors.NewErrorf(vtrpcpb.Code_INTERNAL, vterrors.WrongTypeForVar, "unexpected destination type: %T", d)
		}
	}
	return result, nil
}

func (vf *VindexFunc) buildRow(id sqltypes.Value, ksid []byte, kr *topodatapb.KeyRange) ([]sqltypes.Value, error) {
	row := make([]sqltypes.Value, 0, len(vf.Fields))
	for _, col := range vf.Cols {
		switch col {
		case 0:
			row = append(row, id)
		case 1:
			if ksid != nil {
				row = append(row, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid))
			} else {
				row = append(row, sqltypes.NULL)
			}
		case 2:
			if kr != nil {
				row = append(row, sqltypes.MakeTrusted(sqltypes.VarBinary, kr.Start))
			} else {
				row = append(row, sqltypes.NULL)
			}
		case 3:
			if kr != nil {
				row = append(row, sqltypes.MakeTrusted(sqltypes.VarBinary, kr.End))
			} else {
				row = append(row, sqltypes.NULL)
			}
		case 4:
			if ksid != nil {
				row = append(row, sqltypes.NewVarBinary(fmt.Sprintf("%x", ksid)))
			} else {
				row = append(row, sqltypes.NULL)
			}
		case 5:
			if ksid != nil {
				row = append(row, sqltypes.NewVarBinary(key.KeyRangeString(kr)))
			} else {
				row = append(row, sqltypes.NULL)
			}
		default:
			return row, vterrors.NewErrorf(vtrpcpb.Code_OUT_OF_RANGE, vterrors.BadFieldError, "column %v out of range", col)
		}
	}
	return row, nil
}

func (vf *VindexFunc) description() PrimitiveDescription {
	fields := map[string]string{}
	for _, field := range vf.Fields {
		fields[field.Name] = field.Type.String()
	}

	other := map[string]any{
		"Fields":  fields,
		"Columns": vf.Cols,
		"Value":   evalengine.FormatExpr(vf.Value),
	}
	if vf.Vindex != nil {
		other["Vindex"] = vf.Vindex.String()
	}

	return PrimitiveDescription{
		OperatorType: "VindexFunc",
		Variant:      vindexOpcodeName[vf.Opcode],
		Other:        other,
	}
}
