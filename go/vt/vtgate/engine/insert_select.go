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
	"encoding/json"
	"fmt"
	"strconv"
	"sync"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*InsertSelect)(nil)

type (
	// InsertSelect represents the instructions to perform an insert operation with input rows from a select.
	InsertSelect struct {
		InsertCommon

		// Input is a select query plan to retrieve results for inserting data.
		Input Primitive

		// VindexValueOffset stores the offset for each column in the ColumnVindex
		// that will appear in the result set of the select query.
		VindexValueOffset [][]int
	}
)

// newInsertSelect creates a new InsertSelect.
func newInsertSelect(
	ignore bool,
	keyspace *vindexes.Keyspace,
	table *vindexes.Table,
	prefix string,
	suffix sqlparser.OnDup,
	vv [][]int,
	input Primitive,
) *InsertSelect {
	ins := &InsertSelect{
		InsertCommon: InsertCommon{
			Ignore:   ignore,
			Keyspace: keyspace,
			Prefix:   prefix,
			Suffix:   suffix,
		},
		Input:             input,
		VindexValueOffset: vv,
	}
	if table != nil {
		ins.TableName = table.Name.String()
		for _, colVindex := range table.ColumnVindexes {
			if colVindex.IsPartialVindex() {
				continue
			}
			ins.ColVindexes = append(ins.ColVindexes, colVindex)
		}
	}
	return ins
}

func (ins *InsertSelect) Inputs() ([]Primitive, []map[string]any) {
	return []Primitive{ins.Input}, nil
}

// RouteType returns a description of the query routing type used by the primitive
func (ins *InsertSelect) RouteType() string {
	return "InsertSelect"
}

// TryExecute performs a non-streaming exec.
func (ins *InsertSelect) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, ins.QueryTimeout)
	defer cancelFunc()

	if ins.Keyspace.Sharded {
		return ins.execInsertSharded(ctx, vcursor, bindVars)
	}
	return ins.execInsertUnsharded(ctx, vcursor, bindVars)
}

// TryStreamExecute performs a streaming exec.
func (ins *InsertSelect) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if ins.ForceNonStreaming {
		res, err := ins.TryExecute(ctx, vcursor, bindVars, wantfields)
		if err != nil {
			return err
		}
		return callback(res)
	}
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, ins.QueryTimeout)
	defer cancelFunc()

	sharded := ins.Keyspace.Sharded
	output := &sqltypes.Result{}
	err := ins.execSelectStreaming(ctx, vcursor, bindVars, func(irr insertRowsResult) error {
		if len(irr.rows) == 0 {
			return nil
		}

		var qr *sqltypes.Result
		var err error
		if sharded {
			qr, err = ins.insertIntoShardedTable(ctx, vcursor, bindVars, irr)
		} else {
			qr, err = ins.insertIntoUnshardedTable(ctx, vcursor, bindVars, irr)
		}
		if err != nil {
			return err
		}

		output.RowsAffected += qr.RowsAffected
		// InsertID needs to be updated to the least insertID value in sqltypes.Result
		if output.InsertID == 0 || output.InsertID > qr.InsertID {
			output.InsertID = qr.InsertID
		}
		return nil
	})
	if err != nil {
		return err
	}
	return callback(output)
}

func (ins *InsertSelect) execInsertUnsharded(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	irr, err := ins.execSelect(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	if len(irr.rows) == 0 {
		return &sqltypes.Result{}, nil
	}
	return ins.insertIntoUnshardedTable(ctx, vcursor, bindVars, irr)
}

func (ins *InsertSelect) insertIntoUnshardedTable(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, irr insertRowsResult) (*sqltypes.Result, error) {
	query := ins.getInsertUnshardedQuery(irr.rows, bindVars)
	return ins.executeUnshardedTableQuery(ctx, vcursor, ins, bindVars, query, irr.insertID)
}

func (ins *InsertSelect) getInsertUnshardedQuery(rows []sqltypes.Row, bindVars map[string]*querypb.BindVariable) string {
	var mids sqlparser.Values
	for r, inputRow := range rows {
		row := sqlparser.ValTuple{}
		for c, value := range inputRow {
			bvName := insertVarOffset(r, c)
			bindVars[bvName] = sqltypes.ValueBindVariable(value)
			row = append(row, sqlparser.NewArgument(bvName))
		}
		mids = append(mids, row)
	}
	return ins.Prefix + sqlparser.String(mids) + sqlparser.String(ins.Suffix)
}

func (ins *InsertSelect) insertIntoShardedTable(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	irr insertRowsResult,
) (*sqltypes.Result, error) {
	rss, queries, err := ins.getInsertShardedQueries(ctx, vcursor, bindVars, irr.rows)
	if err != nil {
		return nil, err
	}

	qr, err := ins.executeInsertQueries(ctx, vcursor, rss, queries, irr.insertID)
	if err != nil {
		return nil, err
	}
	qr.InsertID = uint64(irr.insertID)
	return qr, nil
}

func (ins *InsertSelect) executeInsertQueries(
	ctx context.Context,
	vcursor VCursor,
	rss []*srvtopo.ResolvedShard,
	queries []*querypb.BoundQuery,
	insertID uint64,
) (*sqltypes.Result, error) {
	autocommit := (len(rss) == 1 || ins.MultiShardAutocommit) && vcursor.AutocommitApproval()
	err := allowOnlyPrimary(rss...)
	if err != nil {
		return nil, err
	}
	result, errs := vcursor.ExecuteMultiShard(ctx, ins, rss, queries, true /* rollbackOnError */, autocommit)
	if errs != nil {
		return nil, vterrors.Aggregate(errs)
	}

	if insertID != 0 {
		result.InsertID = insertID
	}
	return result, nil
}

func (ins *InsertSelect) getInsertShardedQueries(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	rows []sqltypes.Row,
) ([]*srvtopo.ResolvedShard, []*querypb.BoundQuery, error) {
	vindexRowsValues, err := ins.buildVindexRowsValues(rows)
	if err != nil {
		return nil, nil, err
	}

	keyspaceIDs, err := ins.processVindexes(ctx, vcursor, vindexRowsValues)
	if err != nil {
		return nil, nil, err
	}

	var indexes []*querypb.Value
	var destinations []key.Destination
	for i, ksid := range keyspaceIDs {
		if ksid != nil {
			indexes = append(indexes, &querypb.Value{
				Value: strconv.AppendInt(nil, int64(i), 10),
			})
			destinations = append(destinations, key.DestinationKeyspaceID(ksid))
		}
	}
	if len(destinations) == 0 {
		// In this case, all we have is nil KeyspaceIds, we don't do
		// anything at all.
		return nil, nil, nil
	}

	rss, indexesPerRss, err := vcursor.ResolveDestinations(ctx, ins.Keyspace.Name, indexes, destinations)
	if err != nil {
		return nil, nil, err
	}

	queries := make([]*querypb.BoundQuery, len(rss))
	for i := range rss {
		bvs := sqltypes.CopyBindVariables(bindVars) // we don't want to create one huge bindvars for all values
		var mids sqlparser.Values
		for _, indexValue := range indexesPerRss[i] {
			index, _ := strconv.Atoi(string(indexValue.Value))
			if keyspaceIDs[index] != nil {
				row := sqlparser.ValTuple{}
				for colOffset, value := range rows[index] {
					bvName := insertVarOffset(index, colOffset)
					bvs[bvName] = sqltypes.ValueBindVariable(value)
					row = append(row, sqlparser.NewArgument(bvName))
				}
				mids = append(mids, row)
			}
		}
		rewritten := ins.Prefix + sqlparser.String(mids) + sqlparser.String(ins.Suffix)
		queries[i] = &querypb.BoundQuery{
			Sql:           rewritten,
			BindVariables: bvs,
		}
	}

	return rss, queries, nil
}

func (ins *InsertSelect) buildVindexRowsValues(rows []sqltypes.Row) ([][]sqltypes.Row, error) {
	colVindexes := ins.ColVindexes
	if len(colVindexes) != len(ins.VindexValueOffset) {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex value offsets and vindex info do not match")
	}

	// Here we go over the incoming rows and extract values for the vindexes we need to update
	vindexRowsValues := make([][]sqltypes.Row, len(colVindexes))
	for _, inputRow := range rows {
		for colIdx := range colVindexes {
			offsets := ins.VindexValueOffset[colIdx]
			row := make(sqltypes.Row, 0, len(offsets))
			for _, offset := range offsets {
				if offset == -1 { // value not provided from select query
					row = append(row, sqltypes.NULL)
					continue
				}
				row = append(row, inputRow[offset])
			}
			vindexRowsValues[colIdx] = append(vindexRowsValues[colIdx], row)
		}
	}
	return vindexRowsValues, nil
}

func (ins *InsertSelect) execInsertSharded(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	result, err := ins.execSelect(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	if len(result.rows) == 0 {
		return &sqltypes.Result{}, nil
	}

	return ins.insertIntoShardedTable(ctx, vcursor, bindVars, result)
}

func (ins *InsertSelect) description() PrimitiveDescription {
	other := ins.commonDesc()
	other["TableName"] = ins.GetTableName()

	if len(ins.VindexValueOffset) > 0 {
		valuesOffsets := map[string]string{}
		for idx, ints := range ins.VindexValueOffset {
			if len(ins.ColVindexes) < idx {
				panic("ins.ColVindexes and ins.VindexValueOffset do not line up")
			}
			vindex := ins.ColVindexes[idx]
			marshal, _ := json.Marshal(ints)
			valuesOffsets[vindex.Name] = string(marshal)
		}
		other["VindexOffsetFromSelect"] = valuesOffsets
	}

	return PrimitiveDescription{
		OperatorType:     "Insert",
		Keyspace:         ins.Keyspace,
		Variant:          "Select",
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}

func (ic *InsertCommon) commonDesc() map[string]any {
	other := map[string]any{
		"MultiShardAutocommit": ic.MultiShardAutocommit,
		"QueryTimeout":         ic.QueryTimeout,
		"InsertIgnore":         ic.Ignore,
		"InputAsNonStreaming":  ic.ForceNonStreaming,
		"NoAutoCommit":         ic.PreventAutoCommit,
	}

	if ic.Generate != nil {
		if ic.Generate.Values == nil {
			other["AutoIncrement"] = fmt.Sprintf("%s:Offset(%d)", ic.Generate.Query, ic.Generate.Offset)
		} else {
			other["AutoIncrement"] = fmt.Sprintf("%s:Values::%s", ic.Generate.Query, sqlparser.String(ic.Generate.Values))
		}
	}
	return other
}

func insertVarOffset(rowNum, colOffset int) string {
	return fmt.Sprintf("_c%d_%d", rowNum, colOffset)
}

type insertRowsResult struct {
	rows     []sqltypes.Row
	insertID uint64
}

func (ins *InsertSelect) execSelect(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (insertRowsResult, error) {
	res, err := vcursor.ExecutePrimitive(ctx, ins.Input, bindVars, false)
	if err != nil || len(res.Rows) == 0 {
		return insertRowsResult{}, err
	}

	insertID, err := ins.processGenerateFromSelect(ctx, vcursor, ins, res.Rows)
	if err != nil {
		return insertRowsResult{}, err
	}

	return insertRowsResult{
		rows:     res.Rows,
		insertID: uint64(insertID),
	}, nil
}

func (ins *InsertSelect) execSelectStreaming(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	callback func(irr insertRowsResult) error,
) error {
	var mu sync.Mutex
	return vcursor.StreamExecutePrimitiveStandalone(ctx, ins.Input, bindVars, false, func(result *sqltypes.Result) error {
		if len(result.Rows) == 0 {
			return nil
		}

		// should process only one chunk at a time.
		// as parallel chunk insert will try to use the same transaction in the vttablet
		// this will cause transaction in use error out with "transaction in use" error.
		mu.Lock()
		defer mu.Unlock()

		insertID, err := ins.processGenerateFromSelect(ctx, vcursor, ins, result.Rows)
		if err != nil {
			return err
		}

		return callback(insertRowsResult{
			rows:     result.Rows,
			insertID: uint64(insertID),
		})
	})
}
