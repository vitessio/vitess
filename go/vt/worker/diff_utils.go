// Copyright 2013, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package worker

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/logutil"
	"github.com/youtube/vitess/go/vt/tabletserver/tabletconn"
	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/topo/topoproto"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
)

// QueryResultReader will stream rows towards the output channel.
type QueryResultReader struct {
	Output      <-chan *sqltypes.Result
	Fields      []*querypb.Field
	conn        tabletconn.TabletConn
	clientErrFn func() error
}

// NewQueryResultReaderForTablet creates a new QueryResultReader for
// the provided tablet / sql query
func NewQueryResultReaderForTablet(ctx context.Context, ts topo.Server, tabletAlias *topodatapb.TabletAlias, sql string) (*QueryResultReader, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	tablet, err := ts.GetTablet(shortCtx, tabletAlias)
	cancel()
	if err != nil {
		return nil, err
	}

	endPoint, err := topo.TabletEndPoint(tablet.Tablet)
	if err != nil {
		return nil, err
	}

	// use sessionId for now
	conn, err := tabletconn.GetDialer()(ctx, endPoint, tablet.Keyspace, tablet.Shard, topodatapb.TabletType_UNKNOWN, *remoteActionsTimeout)
	if err != nil {
		return nil, err
	}

	sr, clientErrFn, err := conn.StreamExecute(ctx, sql, make(map[string]interface{}), 0)
	if err != nil {
		return nil, err
	}

	// read the columns, or grab the error
	cols, ok := <-sr
	if !ok {
		return nil, fmt.Errorf("Cannot read Fields for query '%v': %v", sql, clientErrFn())
	}

	return &QueryResultReader{
		Output:      sr,
		Fields:      cols.Fields,
		conn:        conn,
		clientErrFn: clientErrFn,
	}, nil
}

// orderedColumns returns the list of columns:
// - first the primary key columns in the right order
// - then the rest of the columns
func orderedColumns(tableDefinition *tabletmanagerdatapb.TableDefinition) []string {
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

// uint64FromKeyspaceID returns a 64 bits hex number as a string
// (in the form of 0x0123456789abcdef) from the provided keyspaceId
func uint64FromKeyspaceID(keyspaceID []byte) string {
	hex := hex.EncodeToString(keyspaceID)
	return "0x" + hex + strings.Repeat("0", 16-len(hex))
}

// TableScan returns a QueryResultReader that gets all the rows from a
// table, ordered by Primary Key. The returned columns are ordered
// with the Primary Key columns in front.
func TableScan(ctx context.Context, log logutil.Logger, ts topo.Server, tabletAlias *topodatapb.TabletAlias, tableDefinition *tabletmanagerdatapb.TableDefinition) (*QueryResultReader, error) {
	sql := fmt.Sprintf("SELECT %v FROM %v ORDER BY %v", strings.Join(orderedColumns(tableDefinition), ", "), tableDefinition.Name, strings.Join(tableDefinition.PrimaryKeyColumns, ", "))
	log.Infof("SQL query for %v/%v: %v", topoproto.TabletAliasString(tabletAlias), tableDefinition.Name, sql)
	return NewQueryResultReaderForTablet(ctx, ts, tabletAlias, sql)
}

// TableScanByKeyRange returns a QueryResultReader that gets all the
// rows from a table that match the supplied KeyRange, ordered by
// Primary Key. The returned columns are ordered with the Primary Key
// columns in front.
func TableScanByKeyRange(ctx context.Context, log logutil.Logger, ts topo.Server, tabletAlias *topodatapb.TabletAlias, tableDefinition *tabletmanagerdatapb.TableDefinition, keyRange *topodatapb.KeyRange, shardingColumnName string, shardingColumnType topodatapb.KeyspaceIdType) (*QueryResultReader, error) {
	where := ""
	if keyRange != nil {
		switch shardingColumnType {
		case topodatapb.KeyspaceIdType_UINT64:
			if len(keyRange.Start) > 0 {
				if len(keyRange.End) > 0 {
					// have start & end
					where = fmt.Sprintf("WHERE %v >= %v AND %v < %v ", shardingColumnName, uint64FromKeyspaceID(keyRange.Start), shardingColumnName, uint64FromKeyspaceID(keyRange.End))
				} else {
					// have start only
					where = fmt.Sprintf("WHERE %v >= %v ", shardingColumnName, uint64FromKeyspaceID(keyRange.Start))
				}
			} else {
				if len(keyRange.End) > 0 {
					// have end only
					where = fmt.Sprintf("WHERE %v < %v ", shardingColumnName, uint64FromKeyspaceID(keyRange.End))
				}
			}
		case topodatapb.KeyspaceIdType_BYTES:
			if len(keyRange.Start) > 0 {
				if len(keyRange.End) > 0 {
					// have start & end
					where = fmt.Sprintf("WHERE HEX(%v) >= '%v' AND HEX(%v) < '%v' ", shardingColumnName, hex.EncodeToString(keyRange.Start), shardingColumnName, hex.EncodeToString(keyRange.End))
				} else {
					// have start only
					where = fmt.Sprintf("WHERE HEX(%v) >= '%v' ", shardingColumnName, hex.EncodeToString(keyRange.Start))
				}
			} else {
				if len(keyRange.End) > 0 {
					// have end only
					where = fmt.Sprintf("WHERE HEX(%v) < '%v' ", shardingColumnName, hex.EncodeToString(keyRange.End))
				}
			}
		default:
			return nil, fmt.Errorf("Unsupported ShardingColumnType: %v", shardingColumnType)
		}
	}

	sql := fmt.Sprintf("SELECT %v FROM %v %vORDER BY %v", strings.Join(orderedColumns(tableDefinition), ", "), tableDefinition.Name, where, strings.Join(tableDefinition.PrimaryKeyColumns, ", "))
	log.Infof("SQL query for %v/%v: %v", topoproto.TabletAliasString(tabletAlias), tableDefinition.Name, sql)
	return NewQueryResultReaderForTablet(ctx, ts, tabletAlias, sql)
}

func (qrr *QueryResultReader) Error() error {
	return qrr.clientErrFn()
}

// Close closes the connection to the tablet.
func (qrr *QueryResultReader) Close() {
	qrr.conn.Close()
}

// RowReader returns individual rows from a QueryResultReader
type RowReader struct {
	queryResultReader *QueryResultReader
	currentResult     *sqltypes.Result
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
// (nil, error) if an error occurred
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
func (rr *RowReader) Fields() []*querypb.Field {
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

// DiffReport has the stats for a diff job
type DiffReport struct {
	// general stats
	processedRows int

	// stats about the diff
	matchingRows   int
	mismatchedRows int
	extraRowsLeft  int
	extraRowsRight int

	// QPS variables and stats
	startingTime  time.Time
	processingQPS int
}

// HasDifferences returns true if the diff job recorded any difference
func (dr *DiffReport) HasDifferences() bool {
	return dr.mismatchedRows > 0 || dr.extraRowsLeft > 0 || dr.extraRowsRight > 0
}

// ComputeQPS fills in processingQPS
func (dr *DiffReport) ComputeQPS() {
	if dr.processedRows > 0 {
		dr.processingQPS = int(time.Duration(dr.processedRows) * time.Second / time.Now().Sub(dr.startingTime))
	}
}

func (dr *DiffReport) String() string {
	return fmt.Sprintf("DiffReport{%v processed, %v matching, %v mismatched, %v extra left, %v extra right, %v q/s}", dr.processedRows, dr.matchingRows, dr.mismatchedRows, dr.extraRowsLeft, dr.extraRowsRight, dr.processingQPS)
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
// TODO: This can panic if types for left and right don't match.
func CompareRows(fields []*querypb.Field, compareCount int, left, right []sqltypes.Value) (int, error) {
	for i := 0; i < compareCount; i++ {
		lv := left[i].ToNative()
		rv := right[i].ToNative()
		switch l := lv.(type) {
		case int64:
			r := rv.(int64)
			if l < r {
				return -1, nil
			} else if l > r {
				return 1, nil
			}
		case uint64:
			r := rv.(uint64)
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

// RowDiffer will consume rows on both sides, and compare them.
// It assumes left and right are sorted by ascending primary key.
// it will record errors if extra rows exist on either side.
type RowDiffer struct {
	left         *RowReader
	right        *RowReader
	pkFieldCount int
}

// NewRowDiffer returns a new RowDiffer
func NewRowDiffer(left, right *QueryResultReader, tableDefinition *tabletmanagerdatapb.TableDefinition) (*RowDiffer, error) {
	if len(left.Fields) != len(right.Fields) {
		return nil, fmt.Errorf("Cannot diff inputs with different types")
	}
	for i, field := range left.Fields {
		if field.Type != right.Fields[i].Type {
			return nil, fmt.Errorf("Cannot diff inputs with different types: field %v types are %v and %v", i, field.Type, right.Fields[i].Type)
		}
	}
	return &RowDiffer{
		left:         NewRowReader(left),
		right:        NewRowReader(right),
		pkFieldCount: len(tableDefinition.PrimaryKeyColumns),
	}, nil
}

// Go runs the diff. If there is no error, it will drain both sides.
// If an error occurs, it will just return it and stop.
func (rd *RowDiffer) Go(log logutil.Logger) (dr DiffReport, err error) {

	dr.startingTime = time.Now()
	defer dr.ComputeQPS()

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

// RowSubsetDiffer will consume rows on both sides, and compare them.
// It assumes superset and subset are sorted by ascending primary key.
// It will record errors in DiffReport.extraRowsRight if extra rows
// exist on the subset side, and DiffReport.extraRowsLeft will
// always be zero.
type RowSubsetDiffer struct {
	superset     *RowReader
	subset       *RowReader
	pkFieldCount int
}

// NewRowSubsetDiffer returns a new RowSubsetDiffer
func NewRowSubsetDiffer(superset, subset *QueryResultReader, pkFieldCount int) (*RowSubsetDiffer, error) {
	if len(superset.Fields) != len(subset.Fields) {
		return nil, fmt.Errorf("Cannot diff inputs with different types")
	}
	for i, field := range superset.Fields {
		if field.Type != subset.Fields[i].Type {
			return nil, fmt.Errorf("Cannot diff inputs with different types: field %v types are %v and %v", i, field.Type, subset.Fields[i].Type)
		}
	}
	return &RowSubsetDiffer{
		superset:     NewRowReader(superset),
		subset:       NewRowReader(subset),
		pkFieldCount: pkFieldCount,
	}, nil
}

// Go runs the diff. If there is no error, it will drain both sides.
// If an error occurs, it will just return it and stop.
func (rd *RowSubsetDiffer) Go(log logutil.Logger) (dr DiffReport, err error) {

	dr.startingTime = time.Now()
	defer dr.ComputeQPS()

	var superset []sqltypes.Value
	var subset []sqltypes.Value
	advanceSuperset := true
	advanceSubset := true
	for {
		if advanceSuperset {
			superset, err = rd.superset.Next()
			if err != nil {
				return
			}
			advanceSuperset = false
		}
		if advanceSubset {
			subset, err = rd.subset.Next()
			if err != nil {
				return
			}
			advanceSubset = false
		}
		dr.processedRows++
		if superset == nil {
			// no more rows from the superset
			if subset == nil {
				// no more rows from subset either, we're done
				return
			}

			// drain subset, update count
			if count, err := rd.subset.Drain(); err != nil {
				return dr, err
			} else {
				dr.extraRowsRight += 1 + count
			}
			return
		}
		if subset == nil {
			// no more rows from the subset
			// we know we have rows from superset, drain
			if _, err := rd.superset.Drain(); err != nil {
				return dr, err
			}
			return
		}

		// we have both superset and subset, compare
		f := RowsEqual(superset, subset)
		if f == -1 {
			// rows are the same, next
			dr.matchingRows++
			advanceSuperset = true
			advanceSubset = true
			continue
		}

		if f >= rd.pkFieldCount {
			// rows have the same primary key, only content is different
			if dr.mismatchedRows < 10 {
				log.Errorf("Different content %v in same PK: %v != %v", dr.mismatchedRows, superset, subset)
			}
			dr.mismatchedRows++
			advanceSuperset = true
			advanceSubset = true
			continue
		}

		// have to find the 'smallest' raw and advance it
		c, err := CompareRows(rd.superset.Fields(), rd.pkFieldCount, superset, subset)
		if err != nil {
			return dr, err
		}
		if c < 0 {
			advanceSuperset = true
			continue
		} else if c > 0 {
			if dr.extraRowsRight < 10 {
				log.Errorf("Extra row %v on subset: %v", dr.extraRowsRight, subset)
			}
			dr.extraRowsRight++
			advanceSubset = true
			continue
		}

		// After looking at primary keys more carefully,
		// they're the same. Logging a regular difference
		// then, and advancing both.
		if dr.mismatchedRows < 10 {
			log.Errorf("Different content %v in same PK: %v != %v", dr.mismatchedRows, superset, subset)
		}
		dr.mismatchedRows++
		advanceSuperset = true
		advanceSubset = true
	}
}
