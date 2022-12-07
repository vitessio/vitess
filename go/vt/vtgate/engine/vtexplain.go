/*
Copyright 2022 The Vitess Authors.

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
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type (
	ExecuteEntry struct {
		ID         int
		Keyspace   string
		Shard      string
		TabletType topodatapb.TabletType
		Cell       string
		Query      string
	}
	VTExplain struct {
		Input Primitive
	}
)

var _ Primitive = (*VTExplain)(nil)

// RouteType implements the Primitive interface
func (v *VTExplain) RouteType() string {
	return v.Input.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (v *VTExplain) GetKeyspaceName() string {
	return v.Input.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (v *VTExplain) GetTableName() string {
	return v.Input.GetTableName()
}

// GetFields implements the Primitive interface
func (v *VTExplain) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return v.Input.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (v *VTExplain) NeedsTransaction() bool {
	return v.Input.NeedsTransaction()
}

// TryExecute implements the Primitive interface
func (v *VTExplain) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	vcursor.Session().VtExplainLogging()
	_, err := vcursor.ExecutePrimitive(ctx, v.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	result := convertToVTExplainResult(vcursor.Session().GetVTExplainLogs())
	return result, nil
}

// TryStreamExecute implements the Primitive interface
func (v *VTExplain) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	vcursor.Session().VtExplainLogging()
	err := vcursor.StreamExecutePrimitive(ctx, v.Input, bindVars, wantfields, func(result *sqltypes.Result) error {
		return nil
	})
	if err != nil {
		return err
	}
	result := convertToVTExplainResult(vcursor.Session().GetVTExplainLogs())
	return callback(result)
}

func convertToVTExplainResult(logs []ExecuteEntry) *sqltypes.Result {
	fields := []*querypb.Field{{
		Name: "#", Type: sqltypes.Int32,
	}, {
		Name: "keyspace", Type: sqltypes.VarChar,
	}, {
		Name: "shard", Type: sqltypes.VarChar,
	}, {
		Name: "query", Type: sqltypes.VarChar,
	}}
	qr := &sqltypes.Result{
		Fields: fields,
	}
	for _, line := range logs {
		qr.Rows = append(qr.Rows, sqltypes.Row{
			sqltypes.NewInt32(int32(line.ID)),
			sqltypes.NewVarChar(line.Keyspace),
			sqltypes.NewVarChar(line.Shard),
			sqltypes.NewVarChar(line.Query),
		})
	}
	return qr
}

// Inputs implements the Primitive interface
func (v *VTExplain) Inputs() []Primitive {
	return []Primitive{v.Input}
}

func (v *VTExplain) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "VTEXPLAIN",
	}
}
