/*
Copyright 2024 The Vitess Authors.

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

	"vitess.io/vitess/go/mysql/collations"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*Trace)(nil)

type Trace struct {
	identifiablePrimitive
	Inner Primitive
}

type RowsReceived []int

func (t *Trace) RouteType() string {
	return t.Inner.RouteType()
}

func (t *Trace) GetKeyspaceName() string {
	return t.Inner.GetKeyspaceName()
}

func (t *Trace) GetTableName() string {
	return t.Inner.GetTableName()
}

func getFields() []*querypb.Field {
	return []*querypb.Field{{
		Name:    "Trace",
		Type:    sqltypes.VarChar,
		Charset: uint32(collations.SystemCollation.Collation),
		Flags:   uint32(querypb.MySqlFlag_NOT_NULL_FLAG),
	}}
}

func (t *Trace) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return &sqltypes.Result{Fields: getFields()}, nil
}

func (t *Trace) NeedsTransaction() bool {
	return t.Inner.NeedsTransaction()
}

func preWalk(desc PrimitiveDescription, f func(PrimitiveDescription)) {
	f(desc)
	for _, input := range desc.Inputs {
		preWalk(input, f)
	}
}

func (t *Trace) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	getOpStats := vcursor.StartPrimitiveTrace()
	_, err := t.Inner.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return nil, err
	}

	return t.getExplainTraceOutput(getOpStats)
}

func (t *Trace) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	getOpsStats := vcursor.StartPrimitiveTrace()
	noop := func(result *sqltypes.Result) error { return nil }
	err := t.Inner.TryStreamExecute(ctx, vcursor, bindVars, wantfields, noop)
	if err != nil {
		return err
	}

	res, err := t.getExplainTraceOutput(getOpsStats)
	if err != nil {
		return err
	}

	return callback(res)
}

func (t *Trace) getExplainTraceOutput(getOpStats func() map[int]RowsReceived) (*sqltypes.Result, error) {
	description := PrimitiveToPlanDescription(t.Inner)
	statsMap := getOpStats()

	// let's add the stats to the description
	preWalk(description, func(desc PrimitiveDescription) {
		stats, found := statsMap[int(desc.ID)]
		if !found {
			return
		}
		desc.Stats = stats
	})

	output, err := json.MarshalIndent(description, "", "\t")
	if err != nil {
		return nil, err
	}

	return &sqltypes.Result{
		Fields: getFields(),
		Rows: []sqltypes.Row{{
			sqltypes.NewVarChar(string(output)),
		}},
	}, nil
}

func (t *Trace) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{t.Inner}, nil
}

func (t *Trace) description() PrimitiveDescription {
	return PrimitiveDescription{
		OperatorType: "Trace",
	}
}
