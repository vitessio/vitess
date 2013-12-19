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
func FullTableScan(ts topo.Server, tabletAlias topo.TabletAlias, tableDefinition *mysqlctl.TableDefinition) (*QueryResultReader, error) {
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

	sql := fmt.Sprintf("SELECT %v FROM %v ORDER BY (%v)", strings.Join(orderedColumns(tableDefinition), ", "), tableDefinition.Name, strings.Join(tableDefinition.PrimaryKeyColumns, ", "))
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
			advanceLeft = true
			advanceRight = true
			continue
		}

		if f >= rd.pkFieldCount {
			// rows have the same primary key, only content is different
			log.Errorf("Different content in same PK: %v != %v", left, right)
			advanceLeft = true
			advanceRight = true
			continue
		}

		// have to find the 'smallest' raw and advance it
		// let's say the PK is an int64... for now
		for i := 0; i < rd.pkFieldCount; i++ {
			li, lerr := left[i].ParseInt64()
			ri, rerr := right[i].ParseInt64()
			log.Infof("Parse li=%v and ri=%v: %v %v", li, ri, lerr, rerr)
			if lerr != nil {
				return lerr
			}
			if rerr != nil {
				return rerr
			}
			if li < ri {
				log.Errorf("Extra row on left: %v", left)
				advanceLeft = true
				break
			} else if li > ri {
				log.Errorf("Extra row on right: %v", right)
				advanceRight = true
				break
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
