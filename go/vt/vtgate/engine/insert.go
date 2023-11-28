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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ Primitive = (*Insert)(nil)

// Insert represents the instructions to perform an insert operation.
type Insert struct {
	*InsertCommon

	// Query specifies the query to be executed.
	// For InsertSharded plans, this value is unused,
	// and Prefix, Mid and Suffix are used instead.
	Query string

	InsertRows *InsertRows

	// VindexValues specifies values for all the vindex columns.
	// This is a three-dimensional data structure:
	// Insert.Values[i] represents the values to be inserted for the i'th colvindex (i < len(Insert.Table.ColumnVindexes))
	// Insert.Values[i].Values[j] represents values for the j'th column of the given colVindex (j < len(colVindex[i].Columns)
	// Insert.Values[i].Values[j].Values[k] represents the value pulled from row k for that column: (k < len(ins.rows))
	VindexValues [][][]evalengine.Expr

	// Prefix, Mid and Suffix are for sharded insert plans.
	Prefix string
	Mid    sqlparser.Values
	Suffix string

	noInputs
}

// NewQueryInsert creates an Insert with a query string.
func NewQueryInsert(opcode InsertOpcode, keyspace *vindexes.Keyspace, query string) *Insert {
	return &Insert{
		InsertCommon: &InsertCommon{
			Opcode:   opcode,
			Keyspace: keyspace,
		},
		Query:      query,
		InsertRows: NewInsertRows(nil),
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
	mid sqlparser.Values,
	suffix string,
) *Insert {
	ins := &Insert{
		InsertCommon: &InsertCommon{
			Opcode:   opcode,
			Keyspace: keyspace,
			Ignore:   ignore,
		},
		InsertRows:   NewInsertRows(nil),
		VindexValues: vindexValues,
		Prefix:       prefix,
		Mid:          mid,
		Suffix:       suffix,
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
)

var insName = map[InsertOpcode]string{
	InsertUnsharded: "InsertUnsharded",
	InsertSharded:   "InsertSharded",
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

// TryExecute performs a non-streaming exec.
func (ins *Insert) TryExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, _ bool) (*sqltypes.Result, error) {
	ctx, cancelFunc := addQueryTimeout(ctx, vcursor, ins.QueryTimeout)
	defer cancelFunc()

	switch ins.Opcode {
	case InsertUnsharded:
		return ins.execInsertUnsharded(ctx, vcursor, bindVars)
	case InsertSharded:
		return ins.insertIntoShardedTableFromValues(ctx, vcursor, bindVars)
	default:
		return nil, vterrors.VT13001("unexpected query route: %v", ins.Opcode)
	}
}

// TryStreamExecute performs a streaming exec.
func (ins *Insert) TryStreamExecute(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable, wantfields bool, callback func(*sqltypes.Result) error) error {
	res, err := ins.TryExecute(ctx, vcursor, bindVars, wantfields)
	if err != nil {
		return err
	}
	return callback(res)
}

func (ins *Insert) execInsertUnsharded(ctx context.Context, vcursor VCursor, bindVars map[string]*querypb.BindVariable) (*sqltypes.Result, error) {
	insertID, err := ins.InsertRows.processGenerateFromValues(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}

	rss, _, err := vcursor.ResolveDestinations(ctx, ins.Keyspace.Name, nil, []key.Destination{key.DestinationAllShards{}})
	if err != nil {
		return nil, err
	}
	if len(rss) != 1 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "Keyspace does not have exactly one shard: %v", rss)
	}
	if err = allowOnlyPrimary(rss...); err != nil {
		return nil, err
	}
	qr, err := execShard(ctx, ins, vcursor, ins.Query, bindVars, rss[0], true, !ins.PreventAutoCommit /* canAutocommit */)
	if err != nil {
		return nil, err
	}

	// If processGenerateFromValues generated new values, it supersedes
	// any ids that MySQL might have generated. If both generated
	// values, we don't return an error because this behavior
	// is required to support migration.
	if insertID != 0 {
		qr.InsertID = uint64(insertID)
	}
	return qr, nil

}

func (ins *Insert) insertIntoShardedTableFromValues(
	ctx context.Context,
	vcursor VCursor,
	bindVars map[string]*querypb.BindVariable,
) (*sqltypes.Result, error) {
	insertID, err := ins.InsertRows.processGenerateFromValues(ctx, vcursor, bindVars)
	if err != nil {
		return nil, err
	}
	rss, queries, err := ins.getInsertQueriesFromValues(ctx, vcursor, bindVars)
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

// getInsertQueriesFromValues performs all the vindex related work
// and returns a map of shard to queries.
// Using the primary vindex, it computes the target keyspace ids.
// For owned vindexes, it creates entries.
// For unowned vindexes with no input values, it reverse maps.
// For unowned vindexes with values, it validates.
// If it's an IGNORE or ON DUPLICATE key insert, it drops unroutable rows.
func (ins *Insert) getInsertQueriesFromValues(
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
	env := evalengine.NewExpressionEnv(ctx, bindVars, vcursor)
	colVindexes := ins.ColVindexes
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
				rowsResolvedValues = append(rowsResolvedValues, result.Value(vcursor.ConnCollation()))
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
		return nil, nil, vterrors.NewErrorf(vtrpcpb.Code_FAILED_PRECONDITION, vterrors.RequiresPrimaryKey, vterrors.PrimaryVindexNotSet, ins.TableName)
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
		shardBindVars := map[string]*querypb.BindVariable{}
		var mids []string
		for _, indexValue := range indexesPerRss[i] {
			index, _ := strconv.ParseInt(string(indexValue.Value), 0, 64)
			if keyspaceIDs[index] != nil {
				mids = append(mids, sqlparser.String(ins.Mid[index]))
				for _, expr := range ins.Mid[index] {
					err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
						if arg, ok := node.(*sqlparser.Argument); ok {
							bv, exists := bindVars[arg.Name]
							if !exists {
								return false, vterrors.VT03026(arg.Name)
							}
							shardBindVars[arg.Name] = bv
						}
						return true, nil
					}, expr, nil)
					if err != nil {
						return nil, nil, err
					}
				}
			}
		}
		rewritten := ins.Prefix + strings.Join(mids, ",") + ins.Suffix
		queries[i] = &querypb.BoundQuery{
			Sql:           rewritten,
			BindVariables: shardBindVars,
		}
	}

	return rss, queries, nil
}

func (ins *Insert) description() PrimitiveDescription {
	other := ins.commonDesc()
	other["Query"] = ins.Query
	other["TableName"] = ins.GetTableName()
	ins.InsertRows.describe(other)

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
					this = append(this, sqlparser.String(expr))
				}
				res = append(res, strings.Join(this, ", "))
			}

			valuesOffsets[vindex.Name] = strings.Join(res, ", ")
		}
		other["VindexValues"] = valuesOffsets
	}

	return PrimitiveDescription{
		OperatorType:     "Insert",
		Keyspace:         ins.Keyspace,
		Variant:          ins.Opcode.String(),
		TargetTabletType: topodatapb.TabletType_PRIMARY,
		Other:            other,
	}
}

// InsertVarName returns a name for the bind var for this column. This method is used by the planner and engine,
// to make sure they both produce the same names
func InsertVarName(col sqlparser.IdentifierCI, rowNum int) string {
	return fmt.Sprintf("_%s_%d", col.CompliantName(), rowNum)
}
