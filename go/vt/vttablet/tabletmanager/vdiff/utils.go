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

package vdiff

import (
	"context"
	"errors"
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// newMergeSorter creates an engine.MergeSort based on the shard streamers and pk columns
func newMergeSorter(participants map[string]*shardStreamer, comparePKs []compareColInfo, collationEnv *collations.Environment) *engine.MergeSort {
	prims := make([]engine.StreamExecutor, 0, len(participants))
	for _, participant := range participants {
		prims = append(prims, participant)
	}
	ob := make([]evalengine.OrderByParams, len(comparePKs))
	for i, cpk := range comparePKs {
		weightStringCol := -1
		// if the collation is nil or unknown, use binary collation to compare as bytes
		var collation collations.ID = collations.CollationBinaryID
		if cpk.collation != collations.Unknown {
			collation = cpk.collation
		}
		ob[i] = evalengine.OrderByParams{Col: cpk.colIndex, WeightStringCol: weightStringCol, Type: evalengine.NewType(sqltypes.Unknown, collation), CollationEnv: collationEnv}
	}
	return &engine.MergeSort{
		Primitives: prims,
		OrderBy:    ob,
	}
}

// -----------------------------------------------------------------
// Utility functions

func encodeString(in string) string {
	return sqltypes.EncodeStringSQL(in)
}

func pkColsToGroupByParams(pkCols []int, collationEnv *collations.Environment) []*engine.GroupByParams {
	res := make([]*engine.GroupByParams, 0, len(pkCols))
	for _, col := range pkCols {
		res = append(res, &engine.GroupByParams{KeyCol: col, WeightStringCol: -1, CollationEnv: collationEnv})
	}
	return res
}

// errWithoutQueryEcho returns err with the echoed failing statement removed from
// its SQL error. A *sqlerror.SQLError keeps the human message, errno and sqlstate
// separate from the echoed query, and only that query echo is unbounded -- it can
// be arbitrarily large when the statement embeds a big payload such as a VDiff
// report. The echo is dropped for any *sqlerror.SQLError that carries one,
// regardless of the errno; it is applied at the report-write sites so the failure
// can be recorded without the recording statement (last_error / vdiff_log) itself
// exceeding max_allowed_packet -- the case (errno 1153) that motivates it. Non-SQL
// errors, and SQL errors without a query echo, are returned unchanged.
func errWithoutQueryEcho(err error) error {
	sqlErr, ok := errors.AsType[*sqlerror.SQLError](err)
	if !ok || sqlErr.Query == "" {
		return err
	}
	// Clear the echoed query on a copy so SQLError.Error() emits only the message,
	// errno and sqlstate (it appends " during query: ..." only when Query is set).
	redacted := *sqlErr
	redacted.Query = ""
	return &redacted
}

func insertVDiffLog(ctx context.Context, dbClient binlogplayer.DBClient, vdiffID int64, message string) {
	query := "insert into _vt.vdiff_log(vdiff_id, message) values (%d, %s)"
	query = fmt.Sprintf(query, vdiffID, encodeString(message))
	if _, err := dbClient.ExecuteFetch(query, 1); err != nil {
		log.Error(fmt.Sprintf("Error inserting into _vt.vdiff_log: %v", err))
	}
}

// copyNonKeyRangeExpressions copies all expressions from the input WHERE clause
// to the output WHERE clause except for any in_keyrange() expressions.
func copyNonKeyRangeExpressions(where *sqlparser.Where) *sqlparser.Where {
	if where == nil {
		return nil
	}
	exprs := sqlparser.SplitAndExpression(nil, where.Expr)
	newWhere := &sqlparser.Where{}
	for _, expr := range exprs {
		switch expr := expr.(type) {
		case *sqlparser.FuncExpr:
			if expr.Name.EqualString("in_keyrange") {
				continue
			}
		}
		newWhere.Expr = sqlparser.AndExpressions(newWhere.Expr, expr)
	}
	return newWhere
}
