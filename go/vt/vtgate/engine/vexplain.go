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
	"encoding/json"
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
)

type (
	ExecuteEntry struct {
		ID        int
		Target    *querypb.Target
		Gateway   srvtopo.Gateway
		Query     string
		FiredFrom Primitive
	}

	VExplain struct {
		Input Primitive
		Type  sqlparser.VExplainType
	}
)

var _ Primitive = (*VExplain)(nil)

// RouteType implements the Primitive interface
func (v *VExplain) RouteType() string {
	return v.Input.RouteType()
}

// GetKeyspaceName implements the Primitive interface
func (v *VExplain) GetKeyspaceName() string {
	return v.Input.GetKeyspaceName()
}

// GetTableName implements the Primitive interface
func (v *VExplain) GetTableName() string {
	return v.Input.GetTableName()
}

// GetFields implements the Primitive interface
func (v *VExplain) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return v.Input.GetFields(ctx, vcursor, bindVars)
}

// NeedsTransaction implements the Primitive interface
func (v *VExplain) NeedsTransaction() bool {
	return v.Input.NeedsTransaction()
}

// TryExecute implements the Primitive interface
func (v *VExplain) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	vcursor.Session().VExplainLogging()
	_, err := vcursor.ExecutePrimitive(ctx, v.Input, bindVars, wantfields)
	if err != nil {
		return nil, err
	}
	return v.convertToResult(ctx, vcursor)
}

// TryStreamExecute implements the Primitive interface
func (v *VExplain) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	vcursor.Session().VExplainLogging()
	err := vcursor.StreamExecutePrimitive(ctx, v.Input, bindVars, wantfields, func(result *sqltypes.Result) error {
		return nil
	})
	if err != nil {
		return err
	}
	result, err := v.convertToResult(ctx, vcursor)
	if err != nil {
		return err
	}
	return callback(result)
}

func (v *VExplain) convertToResult(ctx context.Context, vcursor VCursor) (*sqltypes.Result, error) {
	switch v.Type {
	case sqlparser.QueriesVExplainType:
		result := convertToVExplainQueriesResult(vcursor.Session().GetVExplainLogs())
		return result, nil
	case sqlparser.AllVExplainType:
		return v.convertToVExplainAllResult(ctx, vcursor)
	default:
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "Unknown type of VExplain plan")
	}
}

func (v *VExplain) convertToVExplainAllResult(ctx context.Context, vcursor VCursor) (*sqltypes.Result, error) {
	logEntries := vcursor.Session().GetVExplainLogs()
	explainResults := make(map[Primitive]string)
	for _, entry := range logEntries {
		if entry.Target == nil || entry.Gateway == nil || entry.FiredFrom == nil {
			continue
		}
		if explainResults[entry.FiredFrom] != "" {
			continue
		}
		explainQuery := fmt.Sprintf("explain format = json %v", entry.Query)
		// We rely on the parser to see if the query we have is explainable or not
		// If we get an error in parsing then we can't execute explain on the given query, and we skip it
		_, err := sqlparser.Parse(explainQuery)
		if err != nil {
			continue
		}
		// Explain statement should now succeed
		res, err := vcursor.ExecuteStandalone(ctx, nil, explainQuery, nil, &srvtopo.ResolvedShard{
			Target:  entry.Target,
			Gateway: entry.Gateway,
		})
		if err != nil {
			return nil, err
		}
		explainResults[entry.FiredFrom] = res.Rows[0][0].ToString()
	}

	planDescription := primitiveToPlanDescriptionWithSQLResults(v.Input, explainResults)
	resultBytes, err := json.MarshalIndent(planDescription, "", "\t")
	if err != nil {
		return nil, err
	}

	result := string(resultBytes)
	fields := []*querypb.Field{
		{
			Name: "VExplain", Type: sqltypes.VarChar,
		},
	}
	rows := []sqltypes.Row{
		{
			sqltypes.NewVarChar(result),
		},
	}
	qr := &sqltypes.Result{
		Fields: fields,
		Rows:   rows,
	}
	return qr, nil
}

// primitiveToPlanDescriptionWithSQLResults transforms a primitive tree into a corresponding PlanDescription tree
// and adds the given res ...
func primitiveToPlanDescriptionWithSQLResults(in Primitive, res map[Primitive]string) PrimitiveDescription {
	this := in.description()

	if v, found := res[in]; found {
		this.Other["mysql_explain_json"] = json.RawMessage(v)
	}

	for _, input := range in.Inputs() {
		this.Inputs = append(this.Inputs, primitiveToPlanDescriptionWithSQLResults(input, res))
	}

	if len(in.Inputs()) == 0 {
		this.Inputs = []PrimitiveDescription{}
	}

	return this
}

func convertToVExplainQueriesResult(logs []ExecuteEntry) *sqltypes.Result {
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
			sqltypes.NewVarChar(line.Target.Keyspace),
			sqltypes.NewVarChar(line.Target.Shard),
			sqltypes.NewVarChar(line.Query),
		})
	}
	return qr
}

// Inputs implements the Primitive interface
func (v *VExplain) Inputs() []Primitive {
	return []Primitive{v.Input}
}

func (v *VExplain) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "VEXPLAIN",
		Other:        map[string]any{"Type": v.Type.ToString()},
	}
}
