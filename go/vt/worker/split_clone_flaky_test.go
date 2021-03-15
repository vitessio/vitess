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

package worker

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"testing"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"context"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/vttablet/queryservice/fakes"
)

var (
	errReadOnly = errors.New("the MariaDB server is running with the --read-only option so it cannot execute this statement (errno 1290) during query: ")

	errStreamingQueryTimeout = errors.New("vttablet: generic::unknown: error: the query was killed either because it timed out or was canceled: (errno 2013) (sqlstate HY000) during query: ")
)

// testQueryService is a local QueryService implementation to support the tests.
type testQueryService struct {
	t *testing.T

	// target is used in the log output.
	target *querypb.Target
	*fakes.StreamHealthQueryService
	shardIndex int
	shardCount int
	alias      string
	// omitKeyspaceID is true when the returned rows should not contain the
	// "keyspace_id" column.
	omitKeyspaceID bool
	fields         []*querypb.Field
	rows           [][]sqltypes.Value

	// mu guards the fields in this group.
	// It is necessary because multiple Go routines will read from the same
	// tablet.
	mu sync.Mutex
	// forceError is set to true for a given int64 primary key value if
	// testQueryService should return an error instead of the actual row.
	forceError map[int64]int
	// errorCallback is run once after the first error is returned.
	errorCallback func()
}

func newTestQueryService(t *testing.T, target *querypb.Target, shqs *fakes.StreamHealthQueryService, shardIndex, shardCount int, alias string, omitKeyspaceID bool) *testQueryService {
	fields := v2Fields
	if omitKeyspaceID {
		fields = v3Fields
	}
	return &testQueryService{
		t:              t,
		target:         target,
		shardIndex:     shardIndex,
		shardCount:     shardCount,
		alias:          alias,
		omitKeyspaceID: omitKeyspaceID,
		fields:         fields,
		forceError:     make(map[int64]int),

		StreamHealthQueryService: shqs,
	}
}

func (sq *testQueryService) StreamExecute(ctx context.Context, target *querypb.Target, sql string, bindVariables map[string]*querypb.BindVariable, transactionID int64, options *querypb.ExecuteOptions, callback func(reply *sqltypes.Result) error) error {
	// Custom parsing of the query we expect.
	// Example: SELECT `id`, `msg`, `keyspace_id` FROM table1 WHERE id>=180 AND id<190 ORDER BY id
	min := math.MinInt32
	max := math.MaxInt32
	var err error
	parts := strings.Split(sql, " ")
	for _, part := range parts {
		if strings.HasPrefix(part, "`id`>=") {
			// Chunk start.
			min, err = strconv.Atoi(part[6:])
			if err != nil {
				return err
			}
		} else if strings.HasPrefix(part, "`id`>") {
			// Chunk start after restart.
			min, err = strconv.Atoi(part[5:])
			if err != nil {
				return err
			}
			// Increment by one to fulfill ">" instead of ">=".
			min++
		} else if strings.HasPrefix(part, "`id`<") {
			// Chunk end.
			max, err = strconv.Atoi(part[5:])
			if err != nil {
				return err
			}
		}
	}
	sq.t.Logf("testQueryService: %v,%v/%v/%v: got query: %v with min %v max (exclusive) %v", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, sql, min, max)

	if sq.forceErrorOnce(int64(min)) {
		sq.t.Logf("testQueryService: %v,%v/%v/%v: sending error for id: %v before sending the fields", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, min)
		return errStreamingQueryTimeout
	}

	// Send the headers.
	if err := callback(&sqltypes.Result{Fields: sq.fields}); err != nil {
		return err
	}

	// Send the values.
	rowsAffected := 0
	for _, row := range sq.rows {
		v, _ := evalengine.ToNative(row[0])
		primaryKey := v.(int64)

		if primaryKey >= int64(min) && primaryKey < int64(max) {
			if sq.forceErrorOnce(primaryKey) {
				sq.t.Logf("testQueryService: %v,%v/%v/%v: sending error for id: %v row: %v", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, primaryKey, row)
				return errStreamingQueryTimeout
			}

			if err := callback(&sqltypes.Result{
				Rows: [][]sqltypes.Value{row},
			}); err != nil {
				return err
			}
			// Uncomment the next line during debugging when needed.
			//			sq.t.Logf("testQueryService: %v,%v/%v/%v: sent row for id: %v row: %v", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, primaryKey, row)
			rowsAffected++
		}
	}

	if rowsAffected == 0 {
		sq.t.Logf("testQueryService: %v,%v/%v/%v: no rows were sent (%v are available)", sq.alias, sq.target.Keyspace, sq.target.Shard, sq.target.TabletType, len(sq.rows))
	}
	return nil
}

// addGeneratedRows will add from-to generated rows. The rows (their primary
// key) will be in the range [from, to).
func (sq *testQueryService) addGeneratedRows(from, to int) {
	var rows [][]sqltypes.Value
	// ksids has keyspace ids which are covered by the shard key ranges -40 and 40-80.
	ksids := []uint64{0x2000000000000000, 0x6000000000000000}

	for id := from; id < to; id++ {
		// Only return the rows which are covered by this shard.
		shardIndex := id % 2
		if sq.shardCount == 1 || shardIndex == sq.shardIndex {
			idValue := sqltypes.NewInt64(int64(id))

			row := []sqltypes.Value{
				idValue,
				sqltypes.NewVarBinary(fmt.Sprintf("Text for %v", id)),
			}
			if !sq.omitKeyspaceID {
				row = append(row, sqltypes.NewVarBinary(fmt.Sprintf("%v", ksids[shardIndex])))
			}
			rows = append(rows, row)
		}
	}

	if sq.rows == nil {
		sq.rows = rows
	} else {
		sq.rows = append(sq.rows, rows...)
	}
}

func (sq *testQueryService) forceErrorOnce(primaryKey int64) bool {
	sq.mu.Lock()
	defer sq.mu.Unlock()

	force := sq.forceError[primaryKey] > 0
	if force {
		sq.forceError[primaryKey]--
		if sq.errorCallback != nil {
			sq.errorCallback()
			sq.errorCallback = nil
		}
	}
	return force
}

var v2Fields = []*querypb.Field{
	{
		Name: "id",
		Type: sqltypes.Int64,
	},
	{
		Name: "msg",
		Type: sqltypes.VarChar,
	},
	{
		Name: "keyspace_id",
		Type: sqltypes.Int64,
	},
}

// v3Fields is identical to v2Fields but lacks the "keyspace_id" column.
var v3Fields = []*querypb.Field{
	{
		Name: "id",
		Type: sqltypes.Int64,
	},
	{
		Name: "msg",
		Type: sqltypes.VarChar,
	},
}
