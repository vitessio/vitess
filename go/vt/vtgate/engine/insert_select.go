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
	"strings"
	"sync"

	"vitess.io/vitess/go/slice"
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
	// InsertSelect represents the instructions to perform an insert operation.
	InsertSelect struct {

		// Keyspace specifies the keyspace to send the query to.
		Keyspace *vindexes.Keyspace

		// Ignore is for INSERT IGNORE and INSERT...ON DUPLICATE KEY constructs
		// for sharded cases.
		Ignore bool

		// TableName is the name of the table on which row will be inserted.
		TableName string

		// Query specifies the query to be executed.
		// For InsertSharded plans, this value is unused,
		// and Prefix, Mid and Suffix are used instead.
		Query string

		InsertRows *InsertRows

		// VindexValueOffset stores the offset for each column in the ColumnVindex
		// that will appear in the result set of the select query.
		VindexValueOffset [][]int

		// ColVindexes are the vindexes that will use the VindexValues
		ColVindexes []*vindexes.ColumnVindex

		// Prefix, Mid and Suffix are for sharded insert plans.
		Prefix string
		Mid    sqlparser.Values
		Suffix string

		// Option to override the standard behavior and allow a multi-shard insert
		// to use single round trip autocommit.
		//
		// This is a clear violation of the SQL semantics since it means the statement
		// is not atomic in the presence of PK conflicts on one shard and not another.
		// However, some application use cases would prefer that the statement partially
		// succeed in order to get the performance benefits of autocommit.
		MultiShardAutocommit bool

		// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
		QueryTimeout int

		// ForceNonStreaming is true when the insert table and select table are same.
		// This will avoid locking by the select table.
		ForceNonStreaming bool

		PreventAutoCommit bool

		// Insert needs tx handling
		txNeeded
	}
)

func (ins *InsertSelect) Inputs() ([]Primitive, []map[string]any) {
	return ins.InsertRows.Inputs()
}

// NewInsertSelect creates a new InsertSelect.
func NewInsertSelect(
	ignore bool,
	keyspace *vindexes.Keyspace,
	table *vindexes.Table,
	prefix string,
	mid sqlparser.Values,
	suffix string,
) *InsertSelect {
	ins := &InsertSelect{
		Ignore:     ignore,
		Keyspace:   keyspace,
		InsertRows: NewInsertRows(nil),
		Prefix:     prefix,
		Mid:        mid,
		Suffix:     suffix,
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

// RouteType returns a description of the query routing type used by the primitive
func (ins *InsertSelect) RouteType() string {
	return "InsertSelect"
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (ins *InsertSelect) GetKeyspaceName() string {
	return ins.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (ins *InsertSelect) GetTableName() string {
	return ins.TableName
}

// TryExecute performs a non-streaming exec.
func (ins *InsertSelect) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, ins.QueryTimeout)
	defer cancelFunc()

	if ins.Keyspace.Sharded {
		return ins.execInsertFromSelect(ctx, vcursor, bindVars)
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
	var mu sync.Mutex
	output := &sqltypes.Result{}
	err := ins.InsertRows.execSelectStreaming(ctx, vcursor, bindVars, func(irr insertRowsResult) error {
		if len(irr.rows) == 0 {
			return nil
		}

		// should process only one chunk at a time.
		// as parallel chunk insert will try to use the same transaction in the vttablet
		// this will cause transaction in use error.
		mu.Lock()
		defer mu.Unlock()

		var insertID int64
		var qr *sqltypes.Result
		var err error
		if sharded {
			insertID, qr, err = ins.insertIntoShardedTableFromSelect(ctx, vcursor, bindVars, irr)
		} else {
			insertID, qr, err = ins.insertIntoUnshardedTable(ctx, vcursor, bindVars, irr.rows)
		}
		if err != nil {
			return err
		}

		output.RowsAffected += qr.RowsAffected
		// InsertID needs to be updated to the least insertID value in sqltypes.Result
		if output.InsertID == 0 || output.InsertID > uint64(insertID) {
			output.InsertID = uint64(insertID)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return callback(output)
}

// GetFields fetches the field info.
func (ins *InsertSelect) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable code for %q", ins.Query)
}

func (ins *InsertSelect) execInsertUnsharded(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	query := ins.Query
	if ins.InsertRows.hasSelectInput() {
		result, err := ins.InsertRows.execSelect(ctx, vcursor, bindVars)
		if err != nil {
			return nil, err
		}
		if len(result.rows) == 0 {
			return &sqltypes.Result{}, nil
		}
		query = ins.getInsertQueryFromSelectForUnsharded(result.rows, bindVars)
	}

	_, qr, err := ins.executeUnshardedTableQuery(ctx, vcursor, bindVars, query)
	return qr, err
}

func (ins *InsertSelect) getInsertQueryFromSelectForUnsharded(rows []sqltypes.Row, bindVars map[string]*querypb.BindVariable) string {
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
	return ins.Prefix + sqlparser.String(mids) + ins.Suffix
}

func (ins *InsertSelect) insertIntoShardedTableFromSelect(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	irr insertRowsResult,
) (int64, *sqltypes.Result, error) {
	rss, queries, err := ins.getInsertSelectQueries(ctx, vcursor, bindVars, irr.rows)
	if err != nil {
		return 0, nil, err
	}

	qr, err := ins.executeInsertQueries(ctx, vcursor, rss, queries, irr.insertID)
	if err != nil {
		return 0, nil, err
	}
	return irr.insertID, qr, nil
}

func (ins *InsertSelect) executeInsertQueries(
	ctx context.Context,
	vcursor VCursor,
	rss []*srvtopo.ResolvedShard,
	queries []*querypb.BoundQuery,
	insertID int64,
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
		result.InsertID = uint64(insertID)
	}
	return result, nil
}

func (ins *InsertSelect) getInsertSelectQueries(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	rows []sqltypes.Row,
) ([]*srvtopo.ResolvedShard, []*querypb.BoundQuery, error) {
	colVindexes := ins.ColVindexes
	if len(colVindexes) != len(ins.VindexValueOffset) {
		return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vindex value offsets and vindex info do not match")
	}

	// Here we go over the incoming rows and extract values for the vindexes we need to update
	shardingCols := make([][]sqltypes.Row, len(colVindexes))
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
			shardingCols[colIdx] = append(shardingCols[colIdx], row)
		}
	}

	keyspaceIDs, err := ins.processPrimary(ctx, vcursor, shardingCols[0], colVindexes[0])
	if err != nil {
		return nil, nil, err
	}

	for vIdx := 1; vIdx < len(colVindexes); vIdx++ {
		colVindex := colVindexes[vIdx]
		var err error
		if colVindex.Owned {
			err = ins.processOwned(ctx, vcursor, shardingCols[vIdx], colVindex, keyspaceIDs)
		} else {
			err = ins.processUnowned(ctx, vcursor, shardingCols[vIdx], colVindex, keyspaceIDs)
		}
		if err != nil {
			return nil, nil, err
		}
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
		rewritten := ins.Prefix + sqlparser.String(mids) + ins.Suffix
		queries[i] = &querypb.BoundQuery{
			Sql:           rewritten,
			BindVariables: bvs,
		}
	}

	return rss, queries, nil
}

func (ins *InsertSelect) execInsertFromSelect(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	result, err := ins.InsertRows.execSelect(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	if len(result.rows) == 0 {
		return &sqltypes.Result{}, nil
	}

	_, qr, err := ins.insertIntoShardedTableFromSelect(ctx, vcursor, bindVars, result)
	return qr, err
}

// processPrimary maps the primary vindex values to the keyspace ids.
func (ins *InsertSelect) processPrimary(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex) ([]ksID, error) {
	destinations, err := vindexes.Map(ctx, colVindex.Vindex, vcursor, vindexColumnsKeys)
	if err != nil {
		return nil, err
	}

	keyspaceIDs := make([]ksID, len(destinations))
	for i, destination := range destinations {
		switch d := destination.(type) {
		case key.DestinationKeyspaceID:
			// This is a single keyspace id, we're good.
			keyspaceIDs[i] = d
		case key.DestinationNone:
			// No valid keyspace id, we may return an error.
			if !ins.Ignore {
				return nil, fmt.Errorf("could not map %v to a keyspace id", vindexColumnsKeys[i])
			}
		default:
			return nil, fmt.Errorf("could not map %v to a unique keyspace id: %v", vindexColumnsKeys[i], destination)
		}
	}

	return keyspaceIDs, nil
}

// processOwned creates vindex entries for the values of an owned column.
func (ins *InsertSelect) processOwned(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex, ksids []ksID) error {
	if !ins.Ignore {
		return colVindex.Vindex.(vindexes.Lookup).Create(ctx, vcursor, vindexColumnsKeys, ksids, false /* ignoreMode */)
	}

	// InsertIgnore
	var createIndexes []int
	var createKeys []sqltypes.Row
	var createKsids []ksID

	for rowNum, rowColumnKeys := range vindexColumnsKeys {
		if ksids[rowNum] == nil {
			continue
		}
		createIndexes = append(createIndexes, rowNum)
		createKeys = append(createKeys, rowColumnKeys)
		createKsids = append(createKsids, ksids[rowNum])
	}
	if createKeys == nil {
		return nil
	}

	err := colVindex.Vindex.(vindexes.Lookup).Create(ctx, vcursor, createKeys, createKsids, true)
	if err != nil {
		return err
	}
	// After creation, verify that the keys map to the keyspace ids. If not, remove
	// those that don't map.
	verified, err := vindexes.Verify(ctx, colVindex.Vindex, vcursor, createKeys, createKsids)
	if err != nil {
		return err
	}
	for i, v := range verified {
		if !v {
			ksids[createIndexes[i]] = nil
		}
	}
	return nil
}

// processUnowned either reverse maps or validates the values for an unowned column.
func (ins *InsertSelect) processUnowned(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex, ksids []ksID) error {
	var reverseIndexes []int
	var reverseKsids []ksID

	var verifyIndexes []int
	var verifyKeys []sqltypes.Row
	var verifyKsids []ksID

	// Check if this VIndex is reversible or not.
	reversibleVindex, isReversible := colVindex.Vindex.(vindexes.Reversible)

	for rowNum, rowColumnKeys := range vindexColumnsKeys {
		// If we weren't able to determine a keyspace id from the primary VIndex, skip this row
		if ksids[rowNum] == nil {
			continue
		}

		if rowColumnKeys[0].IsNull() {
			// If the value of the column is `NULL`, but this is a reversible VIndex,
			// we will try to generate the value from the keyspace id generated by the primary VIndex.
			if isReversible {
				reverseIndexes = append(reverseIndexes, rowNum)
				reverseKsids = append(reverseKsids, ksids[rowNum])
			}

			// Otherwise, don't do anything. Whether `NULL` is a valid value for this column will be
			// handled by MySQL.
		} else {
			// If a value for this column was specified, the keyspace id values from the
			// secondary VIndex need to be verified against the keyspace id from the primary VIndex
			verifyIndexes = append(verifyIndexes, rowNum)
			verifyKeys = append(verifyKeys, rowColumnKeys)
			verifyKsids = append(verifyKsids, ksids[rowNum])
		}
	}

	// Reverse map values for secondary VIndex columns from the primary VIndex's keyspace id.
	if reverseKsids != nil {
		reverseKeys, err := reversibleVindex.ReverseMap(vcursor, reverseKsids)
		if err != nil {
			return err
		}

		for i, reverseKey := range reverseKeys {
			// Fill the first column with the reverse-mapped value.
			vindexColumnsKeys[reverseIndexes[i]][0] = reverseKey
		}
	}

	// Verify that the keyspace ids generated by the primary and secondary VIndexes match
	if verifyIndexes != nil {
		// If values were supplied, we validate against keyspace id.
		verified, err := vindexes.Verify(ctx, colVindex.Vindex, vcursor, verifyKeys, verifyKsids)
		if err != nil {
			return err
		}

		var mismatchVindexKeys []sqltypes.Row
		for i, v := range verified {
			rowNum := verifyIndexes[i]
			if !v {
				if !ins.Ignore {
					mismatchVindexKeys = append(mismatchVindexKeys, vindexColumnsKeys[rowNum])
					continue
				}

				// Skip the whole row if this is a `INSERT IGNORE` or `INSERT ... ON DUPLICATE KEY ...` statement
				// but the keyspace ids didn't match.
				ksids[verifyIndexes[i]] = nil
			}
		}

		if mismatchVindexKeys != nil {
			return fmt.Errorf("values %v for column %v does not map to keyspace ids", mismatchVindexKeys, colVindex.Columns)
		}
	}

	return nil
}

func (ins *InsertSelect) description() PrimitiveDescription {
	other := map[string]any{
		"Query":                ins.Query,
		"TableName":            ins.GetTableName(),
		"MultiShardAutocommit": ins.MultiShardAutocommit,
		"QueryTimeout":         ins.QueryTimeout,
		"InsertIgnore":         ins.Ignore,
		"InputAsNonStreaming":  ins.ForceNonStreaming,
		"NoAutoCommit":         ins.PreventAutoCommit,
	}

	if ins.InsertRows.Generate != nil {
		if ins.InsertRows.Generate.Values == nil {
			other["AutoIncrement"] = fmt.Sprintf("%s:Offset(%d)", ins.InsertRows.Generate.Query, ins.InsertRows.Generate.Offset)
		} else {
			other["AutoIncrement"] = fmt.Sprintf("%s:Values::%s", ins.InsertRows.Generate.Query, sqlparser.String(ins.InsertRows.Generate.Values))
		}
	}

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
	if len(ins.Mid) > 0 {
		mids := slice.Map(ins.Mid, func(from sqlparser.ValTuple) string {
			return sqlparser.String(from)
		})
		shardQuery := fmt.Sprintf("%s%s%s", ins.Prefix, strings.Join(mids, ", "), ins.Suffix)
		if shardQuery != ins.Query {
			other["ShardedQuery"] = shardQuery
		}
	}
	return PrimitiveDescription{
		OperatorType:     "Insert",
		Keyspace:         ins.Keyspace,
		Variant:          "Select",
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}

func (ins *InsertSelect) insertIntoUnshardedTable(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, rows []sqltypes.Row) (int64, *sqltypes.Result, error) {
	query := ins.getInsertQueryFromSelectForUnsharded(rows, bindVars)
	return ins.executeUnshardedTableQuery(ctx, vcursor, bindVars, query)
}

func (ins *InsertSelect) executeUnshardedTableQuery(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, query string) (int64, *sqltypes.Result, error) {
	insertID, err := ins.InsertRows.processGenerateFromValues(ctx, vcursor, bindVars)
	if err != nil {
		return 0, nil, err
	}

	rss, _, err := vcursor.ResolveDestinations(ctx, ins.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return 0, nil, err
	}
	if len(rss) != 1 {
		return 0, nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}
	err = allowOnlyPrimary(rss...)
	if err != nil {
		return 0, nil, err
	}
	qr, err := execShard(ctx, ins, vcursor, query, bindVars, rss[0], true, !ins.PreventAutoCommit /* canAutocommit */)
	if err != nil {
		return 0, nil, err
	}

	// If processGenerateFromValues generated new values, it supersedes
	// any ids that MySQL might have generated. If both generated
	// values, we don't return an error because this behavior
	// is required to support migration.
	if insertID != 0 {
		qr.InsertID = uint64(insertID)
	} else {
		insertID = int64(qr.InsertID)
	}
	return insertID, qr, nil
}
