// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"fmt"
	"strings"

	log "github.com/golang/glog"
	"github.com/youtube/vitess/go/mysql"
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
// that match the supplied KeyRange
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
	log.Infof("SQL query for %v: %v", tabletAlias, sql)
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
	log.Infof("Read columns %v for table %v", cols, tableDefinition.Name)

	return &QueryResultReader{
		Output: sr,
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
		log.Infof("Read one batch: %v", rr.currentResult)
	}
	result := rr.currentResult.Rows[rr.currentIndex]
	rr.currentIndex++
	return result, nil
}

// FieldType returns the type of the i-th column
func (rr *RowReader) FieldType(i int) int64 {
	return rr.currentResult.Fields[i].Type
}

type RowDiffer struct {
	left         *RowReader
	right        *RowReader
	pkFieldCount int
}

func NewRowDiffer(left, right *QueryResultReader, tableDefinition *mysqlctl.TableDefinition) *RowDiffer {
	return &RowDiffer{
		left:         NewRowReader(left),
		right:        NewRowReader(right),
		pkFieldCount: len(tableDefinition.PrimaryKeyColumns),
	}
}

func (rd *RowDiffer) Go() error {

	matchingRows := 0
	mismatchedRows := 0
	extraRowsLeft := 0
	extraRowsRight := 0

	var err error
	var left []sqltypes.Value
	var right []sqltypes.Value
	advanceLeft := true
	advanceRight := true
	for {
		if advanceLeft {
			left, err = rd.left.Next()
			if err != nil {
				return err
			}
			log.Infof("Read left row: %v", left)
			advanceLeft = false
		}
		if advanceRight {
			right, err = rd.right.Next()
			if err != nil {
				return err
			}
			log.Infof("Read right row: %v", right)
			advanceRight = false
		}
		if left == nil {
			// no more rows from the left
			if right == nil {
				// no more rows from right either, we're done
				return nil
			}

			// drain right, return count
			return fmt.Errorf("more rows on right")
		}
		if right == nil {
			// no more rows from the right
			// we know we have rows from left, drain, return count
			return fmt.Errorf("more rows on left")
		}

		// we have both left and right, compare
		f := RowsEqual(left, right)
		log.Infof("Compare returned %v", f)
		if f == -1 {
			// rows are the same, next
			log.Errorf("Same row: %v == %v", left, right)
			matchingRows++
			advanceLeft = true
			advanceRight = true
			continue
		}

		if f >= rd.pkFieldCount {
			// rows have the same primary key, only content is different
			log.Errorf("Different content in same PK: %v != %v", left, right)
			mismatchedRows++
			advanceLeft = true
			advanceRight = true
			continue
		}

		// have to find the 'smallest' raw and advance it
		for i := 0; i < rd.pkFieldCount; i++ {
			// note this is correct, but can be seriously optimized
			fieldType := rd.left.FieldType(i)
			lv := mysql.BuildValue(left[i].Raw(), uint32(fieldType))
			rv := mysql.BuildValue(right[i].Raw(), uint32(fieldType))

			if lv.IsNumeric() {
				ln, err := lv.ParseInt64()
				if err != nil {
					return err
				}
				rn, err := rv.ParseInt64()
				if err != nil {
					return err
				}
				if ln < rn {
					log.Errorf("Extra row on left: %v", left)
					extraRowLeft++
					advanceLeft = true
					break
				} else if ln > rn {
					log.Errorf("Extra row on right: %v", right)
					extraRowRight++
					advanceRight = true
					break
				}
			} else if lv.IsString() {
				c := bytes.Compare(left[i].Raw(), right[i].Raw())
				if c < 0 {
					log.Errorf("Extra row on left: %v", left)
					extraRowLeft++
					advanceLeft = true
					break
				} else if c > 0 {
					log.Errorf("Extra row on right: %v", right)
					extraRowRight++
					advanceRight = true
					break
				}
			} else {
				return fmt.Errorf("Fractional types not supported in primary keys for diffs")
			}
		}
	}

	return nil
}

// return the index of the first different fields, or -1 if both rows are the same
func RowsEqual(left, right []sqltypes.Value) int {
	for i, l := range left {
		if !bytes.Equal(l.Raw(), right[i].Raw()) {
			return i
		}
	}
	return -1
}
