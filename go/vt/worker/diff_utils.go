// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"strings"

	log "github.com/golang/glog"
	mproto "github.com/youtube/vitess/go/mysql/proto"
	rpc "github.com/youtube/vitess/go/rpcplus"
	"github.com/youtube/vitess/go/rpcwrap/bsonrpc"
	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/key"
	"github.com/youtube/vitess/go/vt/mysqlctl"
	tproto "github.com/youtube/vitess/go/vt/tabletserver/proto"
	"github.com/youtube/vitess/go/vt/topo"
)

// QueryResultReader will stream rows towards the output channel.
type QueryResultReader struct {
	Output chan *mproto.QueryResult
	Fields []mproto.Field
	client *rpc.Client
	call   *rpc.Call
}

// orderedColumns returns the list of columns:
// - first the primary key columns in the right order
// - then the rest of the columns
func orderedColumns(tableDefinition *mysqlctl.TableDefinition) []string {
	result := make([]string, 0, len(tableDefinition.Columns))
	result = append(result, tableDefinition.PrimaryKeyColumns...)
	for _, column := range tableDefinition.Columns {
		found := false
		for _, primaryKey := range tableDefinition.PrimaryKeyColumns {
			if primaryKey == column {
				found = true
				break
			}
		}
		if !found {
			result = append(result, column)
		}
	}
	return result
}

// FullTableScan returns a QueryResultReader that gets all the rows from a table
// that match the supplied KeyRange, ordered by Primary Key. The returned
// columns are ordered with the Primary Key columns in front.
func FullTableScan(ts topo.Server, tabletAlias topo.TabletAlias, tableDefinition *mysqlctl.TableDefinition, keyRange key.KeyRange) (*QueryResultReader, error) {
	tablet, err := ts.GetTablet(tabletAlias)
	if err != nil {
		return nil, err
	}

	addr := fmt.Sprintf("%v:%v", tablet.IPAddr, tablet.Portmap["vt"])
	rpcClient, err := bsonrpc.DialHTTP("tcp", addr, 0, nil)
	if err != nil {
		return nil, err
	}

	var sessionInfo tproto.SessionInfo
	if err := rpcClient.Call("SqlQuery.GetSessionId", tproto.SessionParams{Keyspace: tablet.Keyspace, Shard: tablet.Shard}, &sessionInfo); err != nil {
		return nil, err
	}

	where := ""
	if keyRange.Start != key.MinKey {
		if keyRange.End != key.MaxKey {
			// have start & end
			where = fmt.Sprintf("WHERE HEX(keyspace_id) >= '%v' AND HEX(keyspace_id) < '%v' ", keyRange.Start.Hex(), keyRange.End.Hex())
		} else {
			// have start only
			where = fmt.Sprintf("WHERE HEX(keyspace_id) >= '%v' ", keyRange.Start.Hex())
		}
	} else {
		if keyRange.End != key.MaxKey {
			// have end only
			where = fmt.Sprintf("WHERE HEX(keyspace_id) < '%v' ", keyRange.End.Hex())
		}
	}

	sql := fmt.Sprintf("SELECT %v FROM %v %vORDER BY (%v)", strings.Join(orderedColumns(tableDefinition), ", "), tableDefinition.Name, where, strings.Join(tableDefinition.PrimaryKeyColumns, ", "))
	log.Infof("SQL query for %v/%v: %v", tabletAlias, tableDefinition.Name, sql)
	req := &tproto.Query{
		Sql:           sql,
		BindVariables: make(map[string]interface{}),
		TransactionId: 0,
		SessionId:     sessionInfo.SessionId,
	}
	sr := make(chan *mproto.QueryResult, 1000)
	call := rpcClient.StreamGo("SqlQuery.StreamExecute", req, sr)

	// read the columns, or grab the error
	cols, ok := <-sr
	if !ok {
		return nil, fmt.Errorf("Cannot read Fields for query: %v", sql)
	}

	return &QueryResultReader{
		Output: sr,
		Fields: cols.Fields,
		client: rpcClient,
		call:   call,
	}, nil
}

func (qrr *QueryResultReader) Error() error {
	return qrr.call.Error
}

func (qrr *QueryResultReader) Close() error {
	return qrr.client.Close()
}

// RowReader returns individual rows from a QueryResultReader
type RowReader struct {
	queryResultReader *QueryResultReader
	currentResult     *mproto.QueryResult
	currentIndex      int
}

// NewRowReader returns a RowReader based on the QueryResultReader
func NewRowReader(queryResultReader *QueryResultReader) *RowReader {
	return &RowReader{
		queryResultReader: queryResultReader,
	}
}

// Next will return:
// (row, nil) for the next row
// (nil, nil) for EOF
// (nil, error) if an error occured
func (rr *RowReader) Next() ([]sqltypes.Value, error) {
	if rr.currentResult == nil || rr.currentIndex == len(rr.currentResult.Rows) {
		var ok bool
		rr.currentResult, ok = <-rr.queryResultReader.Output
		if !ok {
			if err := rr.queryResultReader.Error(); err != nil {
				return nil, err
			}
			return nil, nil
		}
		rr.currentIndex = 0
	}
	result := rr.currentResult.Rows[rr.currentIndex]
	rr.currentIndex++
	return result, nil
}

// Fields returns the types for the rows
func (rr *RowReader) Fields() []mproto.Field {
	return rr.queryResultReader.Fields
}

// Drain will empty the RowReader and return how many rows we got
func (rr *RowReader) Drain() (int, error) {
	count := 0
	for {
		row, err := rr.Next()
		if err != nil {
			return 0, err
		}
		if row == nil {
			return count, nil
		}
		count++
	}
}

// RowDiffer will consume rows on both sides, and compare them.
// It assumes left and right are sorted by ascending primary key.
type RowDiffer struct {
	left         *RowReader
	right        *RowReader
	pkFieldCount int
}

// NewRowDiffer returns a new RowDiffer
func NewRowDiffer(left, right *QueryResultReader, tableDefinition *mysqlctl.TableDefinition) *RowDiffer {
	return &RowDiffer{
		left:         NewRowReader(left),
		right:        NewRowReader(right),
		pkFieldCount: len(tableDefinition.PrimaryKeyColumns),
	}
}

// DiffReport has the stats for a diff job
type DiffReport struct {
	processedRows  int
	matchingRows   int
	mismatchedRows int
	extraRowsLeft  int
	extraRowsRight int
}

// HasDifferences returns true if the diff job recorded any difference
func (dr *DiffReport) HasDifferences() bool {
	return dr.mismatchedRows > 0 || dr.extraRowsLeft > 0 || dr.extraRowsRight > 0
}

// Go runs the diff. If there is no error, it will drain both sides.
// If an error occurs, it will just return it and stop.
func (rd *RowDiffer) Go() (dr DiffReport, err error) {

	var left []sqltypes.Value
	var right []sqltypes.Value
	advanceLeft := true
	advanceRight := true
	for {
		if advanceLeft {
			left, err = rd.left.Next()
			if err != nil {
				return
			}
			advanceLeft = false
		}
		if advanceRight {
			right, err = rd.right.Next()
			if err != nil {
				return
			}
			advanceRight = false
		}
		dr.processedRows++
		if left == nil {
			// no more rows from the left
			if right == nil {
				// no more rows from right either, we're done
				return
			}

			// drain right, update count
			if count, err := rd.right.Drain(); err != nil {
				return dr, err
			} else {
				dr.extraRowsRight += 1 + count
			}
			return
		}
		if right == nil {
			// no more rows from the right
			// we know we have rows from left, drain, update count
			if count, err := rd.left.Drain(); err != nil {
				return dr, err
			} else {
				dr.extraRowsLeft += 1 + count
			}
			return
		}

		// we have both left and right, compare
		f := RowsEqual(left, right)
		if f == -1 {
			// rows are the same, next
			dr.matchingRows++
			advanceLeft = true
			advanceRight = true
			continue
		}

		if f >= rd.pkFieldCount {
			// rows have the same primary key, only content is different
			if dr.mismatchedRows < 10 {
				log.Errorf("Different content %v in same PK: %v != %v", dr.mismatchedRows, left, right)
			}
			dr.mismatchedRows++
			advanceLeft = true
			advanceRight = true
			continue
		}

		// have to find the 'smallest' raw and advance it
		c, err := CompareRows(rd.left.Fields(), rd.pkFieldCount, left, right)
		if err != nil {
			return dr, err
		}
		if c < 0 {
			if dr.extraRowsLeft < 10 {
				log.Errorf("Extra row %v on left: %v", dr.extraRowsLeft, left)
			}
			dr.extraRowsLeft++
			advanceLeft = true
			continue
		} else if c > 0 {
			if dr.extraRowsRight < 10 {
				log.Errorf("Extra row %v on right: %v", dr.extraRowsRight, right)
			}
			dr.extraRowsRight++
			advanceRight = true
			continue
		}

		// After looking at primary keys more carefully,
		// they're the same. Logging a regular difference
		// then, and advancing both.
		if dr.mismatchedRows < 10 {
			log.Errorf("Different content %v in same PK: %v != %v", dr.mismatchedRows, left, right)
		}
		dr.mismatchedRows++
		advanceLeft = true
		advanceRight = true
	}
}

// RowsEqual returns the index of the first different fields, or -1 if
// both rows are the same
func RowsEqual(left, right []sqltypes.Value) int {
	for i, l := range left {
		if !bytes.Equal(l.Raw(), right[i].Raw()) {
			return i
		}
	}
	return -1
}

// CompareRows returns:
// -1 if left is smaller than right
// 0 if left and right are equal
// +1 if left is bigger than right
func CompareRows(fields []mproto.Field, compareCount int, left, right []sqltypes.Value) (int, error) {
	for i := 0; i < compareCount; i++ {
		fieldType := fields[i].Type
		lv, err := mproto.Convert(fieldType, left[i])
		if err != nil {
			return 0, err
		}
		rv, err := mproto.Convert(fieldType, right[i])
		if err != nil {
			return 0, err
		}
		switch l := lv.(type) {
		case int64:
			r := rv.(int64)
			if l < r {
				return -1, nil
			} else if l > r {
				return 1, nil
			}
		case float64:
			r := rv.(float64)
			if l < r {
				return -1, nil
			} else if l > r {
				return 1, nil
			}
		case []byte:
			r := rv.([]byte)
			return bytes.Compare(l, r), nil
		default:
			return 0, fmt.Errorf("Unsuported type %T returned by mysql.proto.Convert", l)
		}
	}
	return 0, nil
}
