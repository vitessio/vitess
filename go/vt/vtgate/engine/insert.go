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
	"strconv"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/sqlparser"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/vindexes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var _ Primitive = (*Insert)(nil)

type (
	// Insert represents the instructions to perform an insert operation.
	Insert struct {
		// Opcode is the execution opcode.
		Opcode InsertOpcode

		// Ignore is for INSERT IGNORE and INSERT...ON DUPLICATE KEY constructs
		// for sharded cases.
		Ignore bool

		// Keyspace specifies the keyspace to send the query to.
		Keyspace *vindexes.Keyspace

		// Query specifies the query to be executed.
		// For InsertSharded plans, this value is unused,
		// and Prefix, Mid and Suffix are used instead.
		Query string

		// VindexValues specifies values for all the vindex columns.
		// This is a three-dimensional data structure:
		// Insert.Values[i] represents the values to be inserted for the i'th colvindex (i < len(Insert.Table.ColumnVindexes))
		// Insert.Values[i].Values[j] represents values for the j'th column of the given colVindex (j < len(colVindex[i].Columns)
		// Insert.Values[i].Values[j].Values[k] represents the value pulled from row k for that column: (k < len(ins.rows))
		VindexValues [][][]evalengine.Expr

		// ColVindexes are the vindexes that will use the VindexValues
		ColVindexes []*vindexes.ColumnVindex

		// Table specifies the table for the insert.
		Table *vindexes.Table

		// Generate is only set for inserts where a sequence must be generated.
		Generate *Generate

		// Prefix, Mid and Suffix are for sharded insert plans.
		Prefix string
		Mid    []string
		Suffix string

		// Option to override the standard behavior and allow a multi-shard insert
		// to use single round trip autocommit.
		//
		// This is a clear violation of the SQL semantics since it means the statement
		// is not atomic in the presence of PK conflicts on one shard and not another.
		// However some application use cases would prefer that the statement partially
		// succeed in order to get the performance benefits of autocommit.
		MultiShardAutocommit bool

		// QueryTimeout contains the optional timeout (in milliseconds) to apply to this query
		QueryTimeout int

		// VindexValueOffset stores the offset for each column in the ColumnVindex
		// that will appear in the result set of the select query.
		VindexValueOffset [][]int

		// Input is a select query plan to retrieve results for inserting data.
		Input Primitive `json:",omitempty"`

		// ForceNonStreaming is true when the insert table and select table are same.
		// This will avoid locking by the select table.
		ForceNonStreaming bool

		// Insert needs tx handling
		txNeeded
	}

	ksID = []byte
)

func (ins *Insert) Inputs() []Primitive {
	if ins.Input == nil {
		return nil
	}
	return []Primitive{ins.Input}
}

// NewQueryInsert creates an Insert with a query string.
func NewQueryInsert(opcode InsertOpcode, keyspace *vindexes.Keyspace, query string) *Insert {
	return &Insert{
		Opcode:   opcode,
		Keyspace: keyspace,
		Query:    query,
	}
}

// NewSimpleInsert creates an Insert for a Table.
func NewSimpleInsert(opcode InsertOpcode, table *vindexes.Table, keyspace *vindexes.Keyspace) *Insert {
	return &Insert{
		Opcode:   opcode,
		Table:    table,
		Keyspace: keyspace,
	}
}

// NewInsert creates a new Insert.
func NewInsert(
	opcode InsertOpcode,
	ignore bool,
	keyspace *vindexes.Keyspace,
	vindexValues [][][]evalengine.Expr,
	table *vindexes.Table,
	prefix string,
	mid []string,
	suffix string,
) *Insert {
	return &Insert{
		Opcode:       opcode,
		Ignore:       ignore,
		Keyspace:     keyspace,
		VindexValues: vindexValues,
		Table:        table,
		Prefix:       prefix,
		Mid:          mid,
		Suffix:       suffix,
	}
}

// Generate represents the instruction to generate
// a value from a sequence.
type Generate struct {
	Keyspace *vindexes.Keyspace
	Query    string
	// Values are the supplied values for the column, which
	// will be stored as a list within the expression. New
	// values will be generated based on how many were not
	// supplied (NULL).
	Values evalengine.Expr
	// Insert using Select, offset for auto increment column
	Offset int
}

// InsertOpcode is a number representing the opcode
// for the Insert primitive.
type InsertOpcode int

const (
	// InsertUnsharded is for routing an insert statement
	// to an unsharded keyspace.
	InsertUnsharded = InsertOpcode(iota)
	// InsertSharded is for routing an insert statement
	// to individual shards. Requires: A list of Values, one
	// for each ColVindex. If the table has an Autoinc column,
	// A Generate subplan must be created.
	InsertSharded
	// InsertSelect is for routing an insert statement
	// based on rows returned from the select statement.
	InsertSelect
)

var insName = map[InsertOpcode]string{
	InsertUnsharded: "InsertUnsharded",
	InsertSharded:   "InsertSharded",
	InsertSelect:    "InsertSelect",
}

// String returns the opcode
func (code InsertOpcode) String() string {
	return strings.ReplaceAll(insName[code], "Insert", "")
}

// MarshalJSON serializes the InsertOpcode as a JSON string.
// It's used for testing and diagnostics.
func (code InsertOpcode) MarshalJSON() ([]byte, error) {
	return json.Marshal(insName[code])
}

// RouteType returns a description of the query routing type used by the primitive
func (ins *Insert) RouteType() string {
	return insName[ins.Opcode]
}

// GetKeyspaceName specifies the Keyspace that this primitive routes to.
func (ins *Insert) GetKeyspaceName() string {
	return ins.Keyspace.Name
}

// GetTableName specifies the table that this primitive routes to.
func (ins *Insert) GetTableName() string {
	if ins.Table != nil {
		return ins.Table.Name.String()
	}
	return ""
}

// TryExecute performs a non-streaming exec.
func (ins *Insert) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, ins.QueryTimeout)
	defer cancelFunc()

	switch ins.Opcode {
	case InsertUnsharded:
		return ins.execInsertUnsharded(ctx, vcursor, bindVars)
	case InsertSharded:
		return ins.execInsertSharded(ctx, vcursor, bindVars)
	case InsertSelect:
		return ins.execInsertFromSelect(ctx, vcursor, bindVars)
	default:
		// Unreachable.
		return nil, fmt.Errorf("unsupported query route: %v", ins)
	}
}

// TryStreamExecute performs a streaming exec.
func (ins *Insert) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	if ins.Input == nil || ins.ForceNonStreaming {
		res, err := ins.TryExecute(ctx, vcursor, bindVars, wantfields)
		if err != nil {
			return err
		}
		return callback(res)
	}
	if ins.QueryTimeout != 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(ins.QueryTimeout)*time.Millisecond)
		defer cancel()
	}

	unsharded := ins.Opcode == InsertUnsharded
	var mu sync.Mutex
	output := &sqltypes.Result{}

	err := vcursor.StreamExecutePrimitiveStandalone(ctx, ins.Input, bindVars, false, func(result *sqltypes.Result) error {
		if len(result.Rows) == 0 {
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
		if unsharded {
			insertID, qr, err = ins.insertIntoUnshardedTable(ctx, vcursor, bindVars, result)
		} else {
			insertID, qr, err = ins.insertIntoShardedTable(ctx, vcursor, bindVars, result)
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

func (ins *Insert) insertIntoShardedTable(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, result *sqltypes.Result) (int64, *sqltypes.Result, error) {
	insertID, err := ins.processGenerateFromRows(ctx, vcursor, result.Rows)
	if err != nil {
		return 0, nil, err
	}

	rss, queries, err := ins.getInsertSelectQueries(ctx, vcursor, bindVars, result.Rows)
	if err != nil {
		return 0, nil, err
	}

	qr, err := ins.executeInsertQueries(ctx, vcursor, rss, queries, insertID)
	if err != nil {
		return 0, nil, err
	}
	return insertID, qr, nil
}

// GetFields fetches the field info.
func (ins *Insert) GetFields(context.Context, VCursor, map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] unreachable code for %q", ins.Query)
}

func (ins *Insert) execInsertUnsharded(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	query := ins.Query
	if ins.Input != nil {
		result, err := vcursor.ExecutePrimitive(ctx, ins.Input, bindVars, false)
		if err != nil {
			return nil, err
		}
		if len(result.Rows) == 0 {
			return &sqltypes.Result{}, nil
		}
		query = ins.getInsertQueryForUnsharded(result, bindVars)
	}

	_, qr, err := ins.executeUnshardedTableQuery(ctx, vcursor, bindVars, query)
	return qr, err
}

func (ins *Insert) getInsertQueryForUnsharded(result *sqltypes.Result, bindVars map[string]*querypb.BindVariable) string {
	var mids sqlparser.Values
	for r, inputRow := range result.Rows {
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

func (ins *Insert) execInsertSharded(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	insertID, err := ins.processGenerateFromValues(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	rss, queries, err := ins.getInsertShardedRoute(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	return ins.executeInsertQueries(ctx, vcursor, rss, queries, insertID)
}

func (ins *Insert) executeInsertQueries(
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

func (ins *Insert) getInsertSelectQueries(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
	rows []sqltypes.Row,
) ([]*srvtopo.ResolvedShard, []*querypb.BoundQuery, error) {
	colVindexes := ins.ColVindexes
	if colVindexes == nil {
		colVindexes = ins.Table.ColumnVindexes
	}

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

func (ins *Insert) execInsertFromSelect(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	// run the SELECT query
	if ins.Input == nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "something went wrong planning INSERT SELECT")
	}

	result, err := vcursor.ExecutePrimitive(ctx, ins.Input, bindVars, false)
	if err != nil {
		return nil, err
	}
	if len(result.Rows) == 0 {
		return &sqltypes.Result{}, nil
	}

	_, qr, err := ins.insertIntoShardedTable(ctx, vcursor, bindVars, result)
	return qr, err
}

// shouldGenerate determines if a sequence value should be generated for a given value
func shouldGenerate(v sqltypes.Value) bool {
	if v.IsNull() {
		return true
	}

	// Unless the NO_AUTO_VALUE_ON_ZERO sql mode is active in mysql, it also
	// treats 0 as a value that should generate a new sequence.
	n, err := evalengine.ToUint64(v)
	if err == nil && n == 0 {
		return true
	}

	return false
}

// processGenerateFromValues generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (ins *Insert) processGenerateFromValues(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (insertID int64, err error) {
	if ins.Generate == nil {
		return 0, nil
	}

	// Scan input values to compute the number of values to generate, and
	// keep track of where they should be filled.
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	resolved, err := env.Evaluate(ins.Generate.Values)
	if err != nil {
		return 0, err
	}
	count := int64(0)
	values := resolved.TupleValues()
	for _, val := range values {
		if shouldGenerate(val) {
			count++
		}
	}

	// If generation is needed, generate the requested number of values (as one call).
	if count != 0 {
		rss, _, err := vcursor.ResolveDestinations(ctx, ins.Generate.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
		if err != nil {
			return 0, err
		}
		if len(rss) != 1 {
			return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "auto sequence generation can happen through single shard only, it is getting routed to %d shards", len(rss))
		}
		bindVars := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(count)}
		qr, err := vcursor.ExecuteStandalone(ctx, ins, ins.Generate.Query, bindVars, rss[0])
		if err != nil {
			return 0, err
		}
		// If no rows are returned, it's an internal error, and the code
		// must panic, which will be caught and reported.
		insertID, err = evalengine.ToInt64(qr.Rows[0][0])
		if err != nil {
			return 0, err
		}
	}

	// Fill the holes where no value was supplied.
	cur := insertID
	for i, v := range values {
		if shouldGenerate(v) {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.Int64BindVariable(cur)
			cur++
		} else {
			bindVars[SeqVarName+strconv.Itoa(i)] = sqltypes.ValueBindVariable(v)
		}
	}
	return insertID, nil
}

// processGenerateFromRows generates new values using a sequence if necessary.
// If no value was generated, it returns 0. Values are generated only
// for cases where none are supplied.
func (ins *Insert) processGenerateFromRows(
	ctx context.Context,
	vcursor VCursor,
	rows []sqltypes.Row,
) (insertID int64, err error) {
	if ins.Generate == nil {
		return 0, nil
	}
	var count int64
	offset := ins.Generate.Offset
	genColPresent := offset < len(rows[0])
	if genColPresent {
		for _, val := range rows {
			if val[offset].IsNull() {
				count++
			}
		}
	} else {
		count = int64(len(rows))
	}

	if count == 0 {
		return 0, nil
	}

	// If generation is needed, generate the requested number of values (as one call).
	rss, _, err := vcursor.ResolveDestinations(ctx, ins.Generate.Keyspace.Name, nil, []key.Destination{key.DestinationAnyShard{}})
	if err != nil {
		return 0, err
	}
	if len(rss) != 1 {
		return 0, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "auto sequence generation can happen through single shard only, it is getting routed to %d shards", len(rss))
	}
	bindVars := map[string]*querypb.BindVariable{"n": sqltypes.Int64BindVariable(count)}
	qr, err := vcursor.ExecuteStandalone(ctx, ins, ins.Generate.Query, bindVars, rss[0])
	if err != nil {
		return 0, err
	}
	// If no rows are returned, it's an internal error, and the code
	// must panic, which will be caught and reported.
	insertID, err = evalengine.ToInt64(qr.Rows[0][0])
	if err != nil {
		return 0, err
	}

	used := insertID
	for idx, val := range rows {
		if genColPresent {
			if val[offset].IsNull() {
				val[offset] = sqltypes.NewInt64(used)
				used++
			}
		} else {
			rows[idx] = append(val, sqltypes.NewInt64(used))
			used++
		}
	}

	return insertID, nil
}

// getInsertShardedRoute performs all the vindex related work
// and returns a map of shard to queries.
// Using the primary vindex, it computes the target keyspace ids.
// For owned vindexes, it creates entries.
// For unowned vindexes with no input values, it reverse maps.
// For unowned vindexes with values, it validates.
// If it's an IGNORE or ON DUPLICATE key insert, it drops unroutable rows.
func (ins *Insert) getInsertShardedRoute(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) ([]*srvtopo.ResolvedShard, []*querypb.BoundQuery, error) {
	// vindexRowsValues builds the values of all vindex columns.
	// the 3-d structure indexes are colVindex, row, col. Note that
	// ins.Values indexes are colVindex, col, row. So, the conversion
	// involves a transpose.
	// The reason we need to transpose is that all the Vindex APIs
	// require inputs in that format.
	vindexRowsValues := make([][]sqltypes.Row, len(ins.VindexValues))
	rowCount := 0
	env := evalengine.EnvWithBindVars(bindVars, vcursor.ConnCollation())
	colVindexes := ins.ColVindexes
	if colVindexes == nil {
		colVindexes = ins.Table.ColumnVindexes
	}
	for vIdx, vColValues := range ins.VindexValues {
		if len(vColValues) != len(colVindexes[vIdx].Columns) {
			return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] supplied vindex column values don't match vschema: %v %v", vColValues, colVindexes[vIdx].Columns)
		}
		for colIdx, colValues := range vColValues {
			rowsResolvedValues := make(sqltypes.Row, 0, len(colValues))
			for _, colValue := range colValues {
				result, err := env.Evaluate(colValue)
				if err != nil {
					return nil, nil, err
				}
				rowsResolvedValues = append(rowsResolvedValues, result.Value())
			}
			// This is the first iteration: allocate for transpose.
			if colIdx == 0 {
				if len(rowsResolvedValues) == 0 {
					return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] rowcount is zero for inserts: %v", rowsResolvedValues)
				}
				if rowCount == 0 {
					rowCount = len(rowsResolvedValues)
				}
				if rowCount != len(rowsResolvedValues) {
					return nil, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] uneven row values for inserts: %d %d", rowCount, len(rowsResolvedValues))
				}
				vindexRowsValues[vIdx] = make([]sqltypes.Row, rowCount)
			}
			// Perform the transpose.
			for rowNum, colVal := range rowsResolvedValues {
				vindexRowsValues[vIdx][rowNum] = append(vindexRowsValues[vIdx][rowNum], colVal)
			}
		}
	}

	// The output from the following 'process' functions is a list of
	// keyspace ids. For regular inserts, a failure to find a route
	// results in an error. For 'ignore' type inserts, the keyspace
	// id is returned as nil, which is used later to drop the corresponding rows.
	if len(vindexRowsValues) == 0 || len(colVindexes) == 0 {
		return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.RequiresPrimaryKey, vterrors.PrimaryVindexNotSet, ins.Table.Name)
	}
	keyspaceIDs, err := ins.processPrimary(ctx, vcursor, vindexRowsValues[0], colVindexes[0])
	if err != nil {
		return nil, nil, err
	}

	for vIdx := 1; vIdx < len(colVindexes); vIdx++ {
		colVindex := colVindexes[vIdx]
		var err error
		if colVindex.Owned {
			err = ins.processOwned(ctx, vcursor, vindexRowsValues[vIdx], colVindex, keyspaceIDs)
		} else {
			err = ins.processUnowned(ctx, vcursor, vindexRowsValues[vIdx], colVindex, keyspaceIDs)
		}
		if err != nil {
			return nil, nil, err
		}
	}

	// Build 3-d bindvars. Skip rows with nil keyspace ids in case
	// we're executing an insert ignore.
	for vIdx, colVindex := range colVindexes {
		for rowNum, rowColumnKeys := range vindexRowsValues[vIdx] {
			if keyspaceIDs[rowNum] == nil {
				// InsertIgnore: skip the row.
				continue
			}
			for colIdx, vindexKey := range rowColumnKeys {
				col := colVindex.Columns[colIdx]
				name := InsertVarName(col, rowNum)
				bindVars[name] = sqltypes.ValueBindVariable(vindexKey)
			}
		}
	}

	// We need to know the keyspace ids and the Mids associated with
	// each RSS.  So we pass the ksid indexes in as ids, and get them back
	// as values. We also skip nil KeyspaceIds, no need to resolve them.
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
		var mids []string
		for _, indexValue := range indexesPerRss[i] {
			index, _ := strconv.ParseInt(string(indexValue.Value), 0, 64)
			if keyspaceIDs[index] != nil {
				mids = append(mids, ins.Mid[index])
			}
		}
		rewritten := ins.Prefix + strings.Join(mids, ",") + ins.Suffix
		queries[i] = &querypb.BoundQuery{
			Sql:           rewritten,
			BindVariables: bindVars,
		}
	}

	return rss, queries, nil
}

// processPrimary maps the primary vindex values to the keyspace ids.
func (ins *Insert) processPrimary(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex) ([]ksID, error) {
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
func (ins *Insert) processOwned(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex, ksids []ksID) error {
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
func (ins *Insert) processUnowned(ctx context.Context, vcursor VCursor, vindexColumnsKeys []sqltypes.Row, colVindex *vindexes.ColumnVindex, ksids []ksID) error {
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

// InsertVarName returns a name for the bind var for this column. This method is used by the planner and engine,
// to make sure they both produce the same names
func InsertVarName(col sqlparser.IdentifierCI, rowNum int) string {
	return fmt.Sprintf("_%s_%d", col.CompliantName(), rowNum)
}

func insertVarOffset(rowNum, colOffset int) string {
	return fmt.Sprintf("_c%d_%d", rowNum, colOffset)
}

func (ins *Insert) description() PrimitiveDescription {
	other := map[string]any{
		"Query":                ins.Query,
		"TableName":            ins.GetTableName(),
		"MultiShardAutocommit": ins.MultiShardAutocommit,
		"QueryTimeout":         ins.QueryTimeout,
	}

	if len(ins.VindexValues) > 0 {
		valuesOffsets := map[string]string{}
		for idx, ints := range ins.VindexValues {
			if len(ins.ColVindexes) < idx {
				panic("ins.ColVindexes and ins.VindexValueOffset do not line up")
			}
			vindex := ins.ColVindexes[idx]
			var res []string
			for _, exprs := range ints {
				var this []string
				for _, expr := range exprs {
					this = append(this, evalengine.FormatExpr(expr))
				}
				res = append(res, strings.Join(this, ", "))
			}

			valuesOffsets[vindex.Name] = strings.Join(res, ", ")
		}
		other["VindexValues"] = valuesOffsets
	}

	if ins.Generate != nil && ins.Generate.Values == nil {
		other["AutoIncrement"] = fmt.Sprintf("%s:%d", ins.Generate.Keyspace.Name, ins.Generate.Offset)
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
	if ins.Ignore {
		other["InsertIgnore"] = true
	}
	return PrimitiveDescription{
		OperatorType:     "Insert",
		Keyspace:         ins.Keyspace,
		Variant:          ins.Opcode.String(),
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}

func (ins *Insert) insertIntoUnshardedTable(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, result *sqltypes.Result) (int64, *sqltypes.Result, error) {
	query := ins.getInsertQueryForUnsharded(result, bindVars)
	return ins.executeUnshardedTableQuery(ctx, vcursor, bindVars, query)
}

func (ins *Insert) executeUnshardedTableQuery(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, query string) (int64, *sqltypes.Result, error) {
	insertID, err := ins.processGenerateFromValues(ctx, vcursor, bindVars)
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
	qr, err := execShard(ctx, ins, vcursor, query, bindVars, rss[0], true, true /* canAutocommit */)
	if err != nil {
		return 0, nil, err
	}

	// If processGenerateFromValues generated new values, it supercedes
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
