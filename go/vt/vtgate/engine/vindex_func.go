/*
Copyright 2017 Google Inc.

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
	"encoding/json"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var _ Primitive = (*VindexFunc)(nil)

// VindexFunc is a primitive that performs vindex functions.
type VindexFunc struct {
	Opcode VindexOpcode
	// Fields is the field info for the result.
	Fields []*querypb.Field
	// Cols contains source column numbers: 0 for id, 1 for keyspace_id.
	Cols   []int
	Vindex vindexes.Vindex
	Value  sqltypes.PlanValue
}

// MarshalJSON serializes the VindexFunc into a JSON representation.
// It's used for testing and diagnostics.
func (vf *VindexFunc) MarshalJSON() ([]byte, error) {
	v := struct {
		Opcode VindexOpcode
		Fields []*querypb.Field
		Cols   []int
		Vindex string
		Value  sqltypes.PlanValue
	}{
		Opcode: vf.Opcode,
		Fields: vf.Fields,
		Cols:   vf.Cols,
		Vindex: vf.Vindex.String(),
		Value:  vf.Value,
	}
	return json.Marshal(v)
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

// KeyspaceTableNames specifies the table that this primitive routes to
func (vf *VindexFunc) KeyspaceTableNames() []*KeyspaceTableName {
	return []*KeyspaceTableName{
		&KeyspaceTableName{},
	}
}

// Execute performs a non-streaming exec.
func (vf *VindexFunc) Execute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return vf.mapVindex(vcursor, bindVars)
}

// StreamExecute performs a streaming exec.
func (vf *VindexFunc) StreamExecute(vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	r, err := vf.mapVindex(vcursor, bindVars)
	if err != nil {
		return err
	}
	if err := callback(&sqltypes.Result{Fields: r.Fields}); err != nil {
		return err
	}
	return callback(&sqltypes.Result{Rows: r.Rows})
}

// GetFields fetches the field info.
func (vf *VindexFunc) GetFields(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{Fields: vf.Fields}, nil
}

func (vf *VindexFunc) mapVindex(vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	k, err := vf.Value.ResolveValue(bindVars)
	if err != nil {
		return nil, err
	}
	vkey, err := sqltypes.Cast(k, sqltypes.VarBinary)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{
		Fields: vf.Fields,
	}

	destinations, err := vf.Vindex.Map(vcursor, []sqltypes.Value{k})
	if err != nil {
		return nil, err
	}
	switch d := destinations[0].(type) {
	case key.DestinationKeyRange:
		if d.KeyRange != nil {
			result.Rows = append(result.Rows, vf.buildRow(vkey, nil, d.KeyRange))
			result.RowsAffected = 1
		}
	case key.DestinationKeyspaceID:
		if len(d) > 0 {
			result.Rows = [][]sqltypes.Value{
				vf.buildRow(vkey, d, nil),
			}
			result.RowsAffected = 1
		}
	case key.DestinationKeyspaceIDs:
		for _, ksid := range d {
			result.Rows = append(result.Rows, vf.buildRow(vkey, ksid, nil))
		}
		result.RowsAffected = uint64(len(d))
	case key.DestinationNone:
		// Nothing to do.
	default:
		panic("unexpected")
	}
	return result, nil
}

func (vf *VindexFunc) buildRow(id sqltypes.Value, ksid []byte, kr *topodatapb.KeyRange) []sqltypes.Value {
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
		default:
			panic("BUG: unexpected column number")
		}
	}
	return row
}
