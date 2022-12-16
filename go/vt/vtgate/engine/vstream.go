/*
Copyright 2021 The Vitess Authors.

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
	"fmt"
	"io"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

var _ Primitive = (*VStream)(nil)

// VStream is an operator for streaming specific keyspace, destination
type VStream struct {
	Keyspace          *vindexes.Keyspace
	TargetDestination key.Destination
	TableName         string
	Position          string
	Limit             int

	noTxNeeded
	noInputs
}

// RouteType implements the Primitive interface
func (v *VStream) RouteType() string {
	return "VStream"
}

// GetKeyspaceName implements the Primitive interface
func (v *VStream) GetKeyspaceName() string {
	return v.Keyspace.Name
}

// GetTableName implements the Primitive interface
func (v *VStream) GetTableName() string {
	return v.TableName
}

// TryExecute implements the Primitive interface
func (v *VStream) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("TryExecute is not supported for VStream")
}

// TryStreamExecute implements the Primitive interface
func (v *VStream) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	rss, _, err := vcursor.ResolveDestinations(ctx, v.Keyspace.Name, nil, []key.Destination{v.TargetDestination})
	if err != nil {
		return err
	}
	filter := &binlogdatapb.Filter{
		Rules: []*binlogdatapb.Rule{{
			Match:  v.TableName,
			Filter: fmt.Sprintf("select * from %s", v.TableName),
		}},
	}
	var lastFields []*querypb.Field
	numRows := 0
	totalRows := 0
	send := func(evs []*binlogdatapb.VEvent) error {
		result := &sqltypes.Result{
			Fields: nil,
			Rows:   [][]sqltypes.Value{},
		}
		for _, ev := range evs {
			if totalRows+numRows >= v.Limit {
				break
			}
			switch ev.Type {
			case binlogdatapb.VEventType_FIELD:
				lastFields = []*querypb.Field{{
					Name: "op",
					Type: querypb.Type_VARCHAR,
				}}
				lastFields = append(lastFields, ev.FieldEvent.Fields...)
			case binlogdatapb.VEventType_ROW:
				result.Fields = lastFields
				eventFields := lastFields[1:]
				for _, change := range ev.RowEvent.RowChanges {
					newVals := addRowChangeIndicatorColumn(change, eventFields)
					result.Rows = append(result.Rows, newVals)
					numRows++
					if totalRows+numRows >= v.Limit {
						break
					}
				}
			default:
			}
		}
		if numRows > 0 {
			err := callback(result)
			if err != nil {
				return err
			}
			totalRows += numRows
			numRows = 0
			if v.Limit > 0 && totalRows >= v.Limit {
				return io.EOF
			}
		}
		return nil
	}

	return vcursor.VStream(ctx, rss, filter, v.Position, send)
}

// for demo purposes we prefix the row with a column with a single char +/*/- to indicate why the row changed
// + => insert, - => delete, * => update. This will be removed/improved as we iterate over this functionality
const (
	RowChangeInsert string = "+"
	RowChangeDelete string = "-"
	RowChangeUpdate string = "*"
)

func addRowChangeIndicatorColumn(change *binlogdatapb.RowChange, eventFields []*querypb.Field) []sqltypes.Value {
	op := ""
	var vals []sqltypes.Value
	if change.After != nil && change.Before == nil {
		op = RowChangeInsert
		vals = sqltypes.MakeRowTrusted(eventFields, change.After)
	} else if change.After != nil && change.Before != nil {
		op = RowChangeUpdate
		vals = sqltypes.MakeRowTrusted(eventFields, change.After)
	} else {
		op = RowChangeDelete
		vals = sqltypes.MakeRowTrusted(eventFields, change.Before)
	}
	newVals := append([]sqltypes.Value{sqltypes.NewVarChar(op)}, vals...)
	return newVals
}

// GetFields implements the Primitive interface
func (v *VStream) GetFields(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.VT13001("GetFields is not supported for VStream")
}

func (v *VStream) description() PrimitiveDescription {
	other := map[string]any{
		"Table":    v.TableName,
		"Limit":    v.Limit,
		"Position": v.Position,
	}
	return PrimitiveDescription{
		OperatorType:      "VStream",
		Keyspace:          v.Keyspace,
		TargetDestination: v.TargetDestination,
		Other:             other,
	}
}
