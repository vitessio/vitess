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

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/vtgate/vindexes"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
)

var _ Primitive = (*VindexFunc)(nil)

// VindexFunc is a primitive that performs vindex functions.
type VindexFunc struct {
	Opcode VindexOpcode
	Vindex vindexes.Vindex
	Value  sqltypes.PlanValue
}

// MarshalJSON serializes the VindexFunc into a JSON representation.
// It's used for testing and diagnostics.
func (vf *VindexFunc) MarshalJSON() ([]byte, error) {
	v := struct {
		Opcode VindexOpcode
		Vindex string
		Value  sqltypes.PlanValue
	}{
		Opcode: vf.Opcode,
		Vindex: vf.Vindex.String(),
		Value:  vf.Value,
	}
	return json.Marshal(v)
}

// VindexOpcode is the opcode for a VindexFunc.
type VindexOpcode int

// These are opcode values for VindexFunc.
const (
	VindexMap = VindexOpcode(iota)
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

// Execute performs a non-streaming exec.
func (vf *VindexFunc) Execute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return vf.mapVindex(vcursor, bindVars, joinVars)
}

// StreamExecute performs a streaming exec.
func (vf *VindexFunc) StreamExecute(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	r, err := vf.mapVindex(vcursor, bindVars, joinVars)
	if err != nil {
		return err
	}
	if err := callback(&sqltypes.Result{Fields: r.Fields}); err != nil {
		return err
	}
	return callback(&sqltypes.Result{Rows: r.Rows})
}

// GetFields fetches the field info.
func (vf *VindexFunc) GetFields(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	r, err := vf.mapVindex(vcursor, bindVars, joinVars)
	if err != nil {
		return nil, err
	}
	return &sqltypes.Result{Fields: r.Fields}, nil
}

func (vf *VindexFunc) mapVindex(vcursor VCursor, bindVars, joinVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	bindVars = combineVars(bindVars, joinVars)
	key, err := vf.Value.ResolveValue(bindVars)
	if err != nil {
		return nil, err
	}
	result := &sqltypes.Result{
		Fields: []*querypb.Field{{
			Name: "id",
			Type: key.Type(),
		}, {
			Name: "keyspace_id",
			Type: querypb.Type_VARBINARY,
		}},
	}

	switch mapper := vf.Vindex.(type) {
	case vindexes.Unique:
		ksids, err := mapper.Map(vcursor, []sqltypes.Value{key})
		if err != nil {
			return nil, err
		}
		// TODO(sougou): change this to nil check after upsert code is
		// merged to master.
		if len(ksids[0]) != 0 {
			result.Rows = [][]sqltypes.Value{{
				key, sqltypes.MakeTrusted(sqltypes.VarBinary, ksids[0]),
			}}
			result.RowsAffected = 1
		}
	case vindexes.NonUnique:
		ksidss, err := mapper.Map(vcursor, []sqltypes.Value{key})
		if err != nil {
			return nil, err
		}
		for _, ksid := range ksidss[0] {
			result.Rows = append(result.Rows, []sqltypes.Value{key, sqltypes.MakeTrusted(sqltypes.VarBinary, ksid)})
		}
		result.RowsAffected = uint64(len(ksidss[0]))
	default:
		panic("unexpected")
	}
	return result, nil
}
