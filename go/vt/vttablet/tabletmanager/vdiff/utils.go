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
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"

	"vitess.io/vitess/go/sqltypes"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// newMergeSorter creates an engine.MergeSort based on the shard streamers and pk columns.
func newMergeSorter(participants map[string]*shardStreamer, comparePKs []compareColInfo) *engine.MergeSort {
	prims := make([]engine.StreamExecutor, 0, len(participants))
	for _, participant := range participants {
		prims = append(prims, participant)
	}
	ob := make([]engine.OrderByParams, len(comparePKs))
	for i, cpk := range comparePKs {
		weightStringCol := -1
		// if the collation is nil or unknown, use binary collation to compare as bytes
		if cpk.collation == nil {
			ob[i] = engine.OrderByParams{Col: cpk.colIndex, WeightStringCol: weightStringCol, CollationID: collations.CollationBinaryID}
		} else {
			ob[i] = engine.OrderByParams{Col: cpk.colIndex, WeightStringCol: weightStringCol, CollationID: cpk.collation.ID()}
		}
	}
	return &engine.MergeSort{
		Primitives: prims,
		OrderBy:    ob,
	}
}

//-----------------------------------------------------------------
// Utility functions

func encodeString(in string) string {
	var buf strings.Builder
	sqltypes.NewVarChar(in).EncodeSQL(&buf)
	return buf.String()
}

func pkColsToGroupByParams(pkCols []int) []*engine.GroupByParams {
	var res []*engine.GroupByParams
	for _, col := range pkCols {
		res = append(res, &engine.GroupByParams{KeyCol: col, WeightStringCol: -1})
	}
	return res
}

func insertVDiffLog(ctx context.Context, dbClient binlogplayer.DBClient, vdiffID int64, message string) {
	query := "insert into _vt.vdiff_log(vdiff_id, message) values (%d, %s)"
	query = fmt.Sprintf(query, vdiffID, encodeString(message))
	if _, err := withDDL.Exec(ctx, query, dbClient.ExecuteFetch, dbClient.ExecuteFetch); err != nil {
		log.Error("Error inserting into _vt.vdiff_log: %w", err)
	}
}

// getTableLastPK gets the lastPK protobuf message for a given vdiff and table using the
// given database client
func getTableLastPK(dbClient binlogplayer.DBClient, vdiffID int64, tableName string) (*querypb.QueryResult, error) {
	if err := dbClient.Connect(); err != nil {
		return nil, err
	}
	defer dbClient.Close()

	query := fmt.Sprintf(sqlGetVDiffTable, vdiffID, encodeString(tableName))
	qr, err := dbClient.ExecuteFetch(query, 1)
	if err != nil {
		return nil, err
	}
	if len(qr.Rows) == 1 {
		var lastpk []byte
		if lastpk, err = qr.Named().Row().ToBytes("lastpk"); err != nil {
			return nil, err
		}
		if len(lastpk) != 0 {
			var lastpkpb querypb.QueryResult
			if err := prototext.Unmarshal(lastpk, &lastpkpb); err != nil {
				return nil, err
			}
			return &lastpkpb, nil
		}
	}
	return nil, nil
}

func stringListContains(lst []string, item string) bool {
	contains := false
	for _, t := range lst {
		if t == item {
			contains = true
			break
		}
	}
	return contains
}
