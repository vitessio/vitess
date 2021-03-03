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
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tmclient"
	"vitess.io/vitess/go/vt/wrangler"

	"context"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/topo"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
	"vitess.io/vitess/go/vt/vttablet/tabletconn"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// QueryResultReader will stream rows towards the output channel.
// TODO(mberlin): Delete this in favor of RestartableResultReader once
// we are confident that the new SplitClone code produces the same diff results
// as the old diff code.
type QueryResultReader struct {
	output sqltypes.ResultStream
	fields []*querypb.Field
	conn   queryservice.QueryService
}

// NewQueryResultReaderForTablet creates a new QueryResultReader for
// the provided tablet / sql query
func NewQueryResultReaderForTablet(ctx context.Context, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, sql string) (*QueryResultReader, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	tablet, err := ts.GetTablet(shortCtx, tabletAlias)
	cancel()
	if err != nil {
		return nil, err
	}

	conn, err := tabletconn.GetDialer()(tablet.Tablet, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}

	stream := queryservice.ExecuteWithStreamer(ctx, conn, &querypb.Target{
		Keyspace:   tablet.Tablet.Keyspace,
		Shard:      tablet.Tablet.Shard,
		TabletType: tablet.Tablet.Type,
	}, sql, make(map[string]*querypb.BindVariable), nil)

	// read the columns, or grab the error
	cols, err := stream.Recv()
	if err != nil {
		return nil, vterrors.Wrapf(err, "Cannot read Fields for query '%v'", sql)
	}

	return &QueryResultReader{
		output: stream,
		fields: cols.Fields,
		conn:   conn,
	}, nil
}

// NewTransactionalQueryResultReaderForTablet creates a new QueryResultReader for
// the provided tablet / sql query, and runs it in an existing transaction
func NewTransactionalQueryResultReaderForTablet(ctx context.Context, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, sql string, txID int64) (*QueryResultReader, error) {
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	tablet, err := ts.GetTablet(shortCtx, tabletAlias)
	cancel()
	if err != nil {
		return nil, err
	}

	conn, err := tabletconn.GetDialer()(tablet.Tablet, grpcclient.FailFast(false))
	if err != nil {
		return nil, err
	}

	stream := queryservice.ExecuteWithTransactionalStreamer(ctx, conn, &querypb.Target{
		Keyspace:   tablet.Tablet.Keyspace,
		Shard:      tablet.Tablet.Shard,
		TabletType: tablet.Tablet.Type,
	}, sql, make(map[string]*querypb.BindVariable), txID, nil)

	// read the columns, or grab the error
	cols, err := stream.Recv()
	if err != nil {
		return nil, vterrors.Wrapf(err, "cannot read Fields for query '%v'", sql)
	}

	return &QueryResultReader{
		output: stream,
		fields: cols.Fields,
		conn:   conn,
	}, nil
}

// Next returns the next result on the stream. It implements ResultReader.
func (qrr *QueryResultReader) Next() (*sqltypes.Result, error) {
	return qrr.output.Recv()
}

// Fields returns the field data. It implements ResultReader.
func (qrr *QueryResultReader) Fields() []*querypb.Field {
	return qrr.fields
}

// Close closes the connection to the tablet.
func (qrr *QueryResultReader) Close(ctx context.Context) {
	qrr.conn.Close(ctx)
}

// v3KeyRangeFilter is a sqltypes.ResultStream implementation that filters
// the underlying results to match the keyrange using a v3 resolver.
type v3KeyRangeFilter struct {
	input    sqltypes.ResultStream
	resolver *v3Resolver
	keyRange *topodatapb.KeyRange
}

// Recv is part of sqltypes.ResultStream interface.
func (f *v3KeyRangeFilter) Recv() (*sqltypes.Result, error) {
	r, err := f.input.Recv()
	if err != nil {
		return nil, err
	}

	rows := make([][]sqltypes.Value, 0, len(r.Rows))
	for _, row := range r.Rows {
		ksid, err := f.resolver.keyspaceID(row)
		if err != nil {
			return nil, err
		}

		if key.KeyRangeContains(f.keyRange, ksid) {
			rows = append(rows, row)
		}
	}
	r.Rows = rows
	return r, nil
}

// reorderColumnsPrimaryKeyFirst returns a copy of "td" with the only difference
// that the Columns field is reordered such that the primary key columns come
// first. See orderedColumns() for details on the ordering.
func reorderColumnsPrimaryKeyFirst(td *tabletmanagerdatapb.TableDefinition) *tabletmanagerdatapb.TableDefinition {
	reorderedTd := proto.Clone(td).(*tabletmanagerdatapb.TableDefinition)
	reorderedTd.Columns = orderedColumns(td)
	return reorderedTd
}

// orderedColumns returns the list of columns:
// - first the primary key columns in the right order
// - then the rest of the columns
// Within the partition of non primary key columns, the order is not changed
// in comparison to the original order of "td.Columns".
func orderedColumns(td *tabletmanagerdatapb.TableDefinition) []string {
	return orderedColumnsHelper(td, true)
}

// orderedColumnsWithoutPrimaryKeyColumns is identical to orderedColumns but
// leaves the primary key columns out.
func orderedColumnsWithoutPrimaryKeyColumns(td *tabletmanagerdatapb.TableDefinition) []string {
	return orderedColumnsHelper(td, false)
}

func orderedColumnsHelper(td *tabletmanagerdatapb.TableDefinition, includePrimaryKey bool) []string {
	var result []string
	if includePrimaryKey {
		result = append(result, td.PrimaryKeyColumns...)
	}
	for _, column := range td.Columns {
		found := false
		for _, primaryKey := range td.PrimaryKeyColumns {
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

// uint64FromKeyspaceID returns the 64 bit number representation
// of the keyspaceID.
func uint64FromKeyspaceID(keyspaceID []byte) uint64 {
	// Copy into 8 bytes because keyspaceID could be shorter.
	bits := make([]byte, 8)
	copy(bits, keyspaceID)
	return binary.BigEndian.Uint64(bits)
}

// TableScan returns a QueryResultReader that gets all the rows from a
// table, ordered by Primary Key. The returned columns are ordered
// with the Primary Key columns in front.
func TableScan(ctx context.Context, log logutil.Logger, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, td *tabletmanagerdatapb.TableDefinition) (*QueryResultReader, error) {
	sql := fmt.Sprintf("SELECT %v FROM %v", strings.Join(escapeAll(orderedColumns(td)), ", "), sqlescape.EscapeID(td.Name))
	if len(td.PrimaryKeyColumns) > 0 {
		sql += fmt.Sprintf(" ORDER BY %v", strings.Join(escapeAll(td.PrimaryKeyColumns), ", "))
	}
	log.Infof("SQL query for %v/%v: %v", topoproto.TabletAliasString(tabletAlias), td.Name, sql)
	return NewQueryResultReaderForTablet(ctx, ts, tabletAlias, sql)
}

// TransactionalTableScan does the same thing as TableScan, but runs inside a transaction
func TransactionalTableScan(ctx context.Context, log logutil.Logger, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, txID int64, td *tabletmanagerdatapb.TableDefinition) (*QueryResultReader, error) {
	sql := fmt.Sprintf("SELECT %v FROM %v", strings.Join(escapeAll(orderedColumns(td)), ", "), sqlescape.EscapeID(td.Name))
	if len(td.PrimaryKeyColumns) > 0 {
		sql += fmt.Sprintf(" ORDER BY %v", strings.Join(escapeAll(td.PrimaryKeyColumns), ", "))
	}
	log.Infof("SQL query for %v/%v: %v", topoproto.TabletAliasString(tabletAlias), td.Name, sql)
	return NewTransactionalQueryResultReaderForTablet(ctx, ts, tabletAlias, sql, txID)
}

// CreateTargetFrom is a helper function
func CreateTargetFrom(tablet *topodatapb.Tablet) *query.Target {
	return &query.Target{
		Cell:       tablet.Alias.Cell,
		Keyspace:   tablet.Keyspace,
		Shard:      tablet.Shard,
		TabletType: tablet.Type,
	}
}

// TableScanByKeyRange returns a QueryResultReader that gets all the
// rows from a table that match the supplied KeyRange, ordered by
// Primary Key. The returned columns are ordered with the Primary Key
// columns in front.
// If keyspaceSchema is passed in, we go into v3 mode, and we ask for all
// source data, and filter here. Otherwise we stick with v2 mode, where we can
// ask the source tablet to do the filtering.
func TableScanByKeyRange(ctx context.Context, log logutil.Logger, ts *topo.Server, tabletAlias *topodatapb.TabletAlias, td *tabletmanagerdatapb.TableDefinition, keyRange *topodatapb.KeyRange, keyspaceSchema *vindexes.KeyspaceSchema, shardingColumnName string, shardingColumnType topodatapb.KeyspaceIdType) (*QueryResultReader, error) {
	if keyspaceSchema != nil {
		// switch to v3 mode.
		keyResolver, err := newV3ResolverFromColumnList(keyspaceSchema, td.Name, orderedColumns(td))
		if err != nil {
			return nil, vterrors.Wrapf(err, "cannot resolve v3 sharding keys for table %v", td.Name)
		}

		// full table scan
		scan, err := TableScan(ctx, log, ts, tabletAlias, td)
		if err != nil {
			return nil, err
		}

		// with extra filter
		scan.output = &v3KeyRangeFilter{
			input:    scan.output,
			resolver: keyResolver.(*v3Resolver),
			keyRange: keyRange,
		}
		return scan, nil
	}

	// in v2 mode, we can do the filtering at the source
	where := ""
	switch shardingColumnType {
	case topodatapb.KeyspaceIdType_UINT64:
		if len(keyRange.Start) > 0 {
			if len(keyRange.End) > 0 {
				// have start & end
				where = fmt.Sprintf("WHERE %v >= %v AND %v < %v", sqlescape.EscapeID(shardingColumnName), uint64FromKeyspaceID(keyRange.Start), sqlescape.EscapeID(shardingColumnName), uint64FromKeyspaceID(keyRange.End))
			} else {
				// have start only
				where = fmt.Sprintf("WHERE %v >= %v", sqlescape.EscapeID(shardingColumnName), uint64FromKeyspaceID(keyRange.Start))
			}
		} else {
			if len(keyRange.End) > 0 {
				// have end only
				where = fmt.Sprintf("WHERE %v < %v", sqlescape.EscapeID(shardingColumnName), uint64FromKeyspaceID(keyRange.End))
			}
		}
	case topodatapb.KeyspaceIdType_BYTES:
		if len(keyRange.Start) > 0 {
			if len(keyRange.End) > 0 {
				// have start & end
				where = fmt.Sprintf("WHERE HEX(%v) >= '%v' AND HEX(%v) < '%v'", sqlescape.EscapeID(shardingColumnName), hex.EncodeToString(keyRange.Start), sqlescape.EscapeID(shardingColumnName), hex.EncodeToString(keyRange.End))
			} else {
				// have start only
				where = fmt.Sprintf("WHERE HEX(%v) >= '%v'", sqlescape.EscapeID(shardingColumnName), hex.EncodeToString(keyRange.Start))
			}
		} else {
			if len(keyRange.End) > 0 {
				// have end only
				where = fmt.Sprintf("WHERE HEX(%v) < '%v'", sqlescape.EscapeID(shardingColumnName), hex.EncodeToString(keyRange.End))
			}
		}
	default:
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "unsupported ShardingColumnType: %v", shardingColumnType)
	}

	sql := fmt.Sprintf("SELECT %v FROM %v %v", strings.Join(escapeAll(orderedColumns(td)), ", "), sqlescape.EscapeID(td.Name), where)
	if len(td.PrimaryKeyColumns) > 0 {
		sql += fmt.Sprintf(" ORDER BY %v", strings.Join(escapeAll(td.PrimaryKeyColumns), ", "))
	}
	log.Infof("SQL query for %v/%v: %v", topoproto.TabletAliasString(tabletAlias), td.Name, sql)
	return NewQueryResultReaderForTablet(ctx, ts, tabletAlias, sql)
}

// ErrStoppedRowReader is returned by RowReader.Next() when
// StopAfterCurrentResult() and it finished the current result.
var ErrStoppedRowReader = vterrors.New(vtrpc.Code_ABORTED, "RowReader won't advance to the next Result because StopAfterCurrentResult() was called")

// RowReader returns individual rows from a ResultReader.
type RowReader struct {
	resultReader           ResultReader
	currentResult          *sqltypes.Result
	currentIndex           int
	stopAfterCurrentResult bool
}

// NewRowReader returns a RowReader based on the QueryResultReader
func NewRowReader(resultReader ResultReader) *RowReader {
	return &RowReader{
		resultReader: resultReader,
	}
}

// Next will return:
// (row, nil) for the next row
// (nil, nil) for EOF
// (nil, error) if an error occurred
func (rr *RowReader) Next() ([]sqltypes.Value, error) {
	for rr.currentResult == nil || rr.currentIndex == len(rr.currentResult.Rows) {
		if rr.stopAfterCurrentResult {
			return nil, ErrStoppedRowReader
		}

		var err error
		rr.currentResult, err = rr.resultReader.Next()
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			return nil, nil
		}
		rr.currentIndex = 0
	}
	row := rr.currentResult.Rows[rr.currentIndex]
	rr.currentIndex++
	return row, nil
}

// Fields returns the types for the rows
func (rr *RowReader) Fields() []*querypb.Field {
	return rr.resultReader.Fields()
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

// StopAfterCurrentResult tells RowReader to keep returning rows in Next()
// until it has finished the current Result. Once there, Next() will always
// return the "StoppedRowReader" error.
// This is feature is necessary for an optimization where the underlying
// ResultReader is the last input in a merge and we want to switch from reading
// rows to reading Results.
func (rr *RowReader) StopAfterCurrentResult() {
	rr.stopAfterCurrentResult = true
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
		dr.processingQPS = int(time.Duration(dr.processedRows) * time.Second / time.Since(dr.startingTime))
	}
}

func (dr *DiffReport) String() string {
	return fmt.Sprintf("DiffReport{%v processed, %v matching, %v mismatched, %v extra left, %v extra right, %v q/s}", dr.processedRows, dr.matchingRows, dr.mismatchedRows, dr.extraRowsLeft, dr.extraRowsRight, dr.processingQPS)
}

// RowsEqual returns the index of the first different column, or -1 if
// both rows are the same.
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
// It compares only up to and including the first "compareCount" columns of each
// row.
// TODO: This can panic if types for left and right don't match.
func CompareRows(fields []*querypb.Field, compareCount int, left, right []sqltypes.Value) (int, error) {
	for i := 0; i < compareCount; i++ {
		lv, _ := evalengine.ToNative(left[i])
		rv, _ := evalengine.ToNative(right[i])
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
			return 0, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "unsupported type %T returned by mysql.proto.Convert", l)
		}
	}
	return 0, nil
}

// RowDiffer will consume rows on both sides, and compare them.
// It assumes left and right are sorted by ascending primary key.
// it will record errors if extra rows exist on either side.
type RowDiffer struct {
	left            *RowReader
	right           *RowReader
	tableDefinition *tabletmanagerdatapb.TableDefinition
}

// NewRowDiffer returns a new RowDiffer
func NewRowDiffer(left, right ResultReader, tableDefinition *tabletmanagerdatapb.TableDefinition) (*RowDiffer, error) {
	leftFields := left.Fields()
	rightFields := right.Fields()
	if len(leftFields) != len(rightFields) {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "[table=%v] Cannot diff inputs with different types", tableDefinition.Name)
	}
	for i, field := range leftFields {
		if field.Type != rightFields[i].Type {
			return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "[table=%v] Cannot diff inputs with different types: field %v types are %v and %v", tableDefinition.Name, i, field.Type, rightFields[i].Type)
		}
	}
	return &RowDiffer{
		left:            NewRowReader(left),
		right:           NewRowReader(right),
		tableDefinition: tableDefinition,
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
			log.Errorf("Draining extra row(s) found on the right starting with: %v", right)
			var count int
			if count, err = rd.right.Drain(); err != nil {
				return dr, err
			}
			dr.extraRowsRight += 1 + count
			return
		}
		if right == nil {
			// no more rows from the right
			// we know we have rows from left, drain, update count
			log.Errorf("Draining extra row(s) found on the left starting with: %v", left)
			var count int
			if count, err = rd.left.Drain(); err != nil {
				return dr, err
			}
			dr.extraRowsLeft += 1 + count
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

		if f >= len(rd.tableDefinition.PrimaryKeyColumns) {
			// rows have the same primary key, only content is different
			if dr.mismatchedRows < 10 {
				log.Errorf("[table=%v] Different content %v in same PK: %v != %v", rd.tableDefinition.Name, dr.mismatchedRows, left, right)
			}
			dr.mismatchedRows++
			advanceLeft = true
			advanceRight = true
			continue
		}

		// have to find the 'smallest' row and advance it
		c, err := CompareRows(rd.left.Fields(), len(rd.tableDefinition.PrimaryKeyColumns), left, right)
		if err != nil {
			return dr, err
		}
		if c < 0 {
			if dr.extraRowsLeft < 10 {
				log.Errorf("[table=%v] Extra row %v on left: %v", rd.tableDefinition.Name, dr.extraRowsLeft, left)
			}
			dr.extraRowsLeft++
			advanceLeft = true
			continue
		} else if c > 0 {
			if dr.extraRowsRight < 10 {
				log.Errorf("[table=%v] Extra row %v on right: %v", rd.tableDefinition.Name, dr.extraRowsRight, right)
			}
			dr.extraRowsRight++
			advanceRight = true
			continue
		}

		// After looking at primary keys more carefully,
		// they're the same. Logging a regular difference
		// then, and advancing both.
		if dr.mismatchedRows < 10 {
			log.Errorf("[table=%v] Different content %v in same PK: %v != %v", rd.tableDefinition.Name, dr.mismatchedRows, left, right)
		}
		dr.mismatchedRows++
		advanceLeft = true
		advanceRight = true
	}
}

// createTransactions returns an array of transactions that all share the same view of the data.
// It will check that no new transactions have been seen between the creation of the underlying transactions,
// to guarantee that all TransactionalTableScanner are pointing to the same point
func createTransactions(ctx context.Context, numberOfScanners int, wr *wrangler.Wrangler, cleaner *wrangler.Cleaner, queryService queryservice.QueryService, target *query.Target, tabletInfo *topodatapb.Tablet) ([]int64, error) {
	scanners := make([]int64, numberOfScanners)
	for i := 0; i < numberOfScanners; i++ {

		tx, _, err := queryService.Begin(ctx, target, &query.ExecuteOptions{
			// Make sure our tx is not killed by tx sniper
			Workload:             query.ExecuteOptions_DBA,
			TransactionIsolation: query.ExecuteOptions_CONSISTENT_SNAPSHOT_READ_ONLY,
		})
		if err != nil {
			return nil, vterrors.Wrapf(err, "could not open transaction on %v", topoproto.TabletAliasString(tabletInfo.Alias))
		}

		// Remember to rollback the transactions
		cleaner.Record("CloseTransaction", topoproto.TabletAliasString(tabletInfo.Alias), func(ctx context.Context, wr *wrangler.Wrangler) error {
			queryService, err := tabletconn.GetDialer()(tabletInfo, true)
			if err != nil {
				return err
			}
			_, err = queryService.Rollback(ctx, target, tx)
			return err
		})

		scanners[i] = tx
	}

	return scanners, nil
}

// TableScanner is a simple abstraction that allows a TableScanner user to remain impervious
// by the transactionality of the connection
type TableScanner interface {
	ScanTable(ctx context.Context, td *tabletmanagerdatapb.TableDefinition) (*QueryResultReader, error)
}

// TransactionalTableScanner works inside of a transaction set up with CONSISTENT SNAPSHOT
type TransactionalTableScanner struct {
	wr           *wrangler.Wrangler
	cleaner      *wrangler.Cleaner
	tabletAlias  *topodatapb.TabletAlias
	queryService queryservice.QueryService
	tx           int64
}

// ScanTable performs a full table scan, ordered by the primary keys, if any
func (tt TransactionalTableScanner) ScanTable(ctx context.Context, td *tabletmanagerdatapb.TableDefinition) (*QueryResultReader, error) {
	return TransactionalTableScan(ctx, tt.wr.Logger(), tt.wr.TopoServer(), tt.tabletAlias, tt.tx, td)
}

// NonTransactionalTableScanner just passes through the queries, and relies on paused replication traffic taking care of the consistent snapshot part
type NonTransactionalTableScanner struct {
	wr           *wrangler.Wrangler
	cleaner      *wrangler.Cleaner
	tabletAlias  *topodatapb.TabletAlias
	queryService queryservice.QueryService
}

// ScanTable performs a full table scan, ordered by the primary keys, if any
func (ntts NonTransactionalTableScanner) ScanTable(ctx context.Context, td *tabletmanagerdatapb.TableDefinition) (*QueryResultReader, error) {
	return TableScan(ctx, ntts.wr.Logger(), ntts.wr.TopoServer(), ntts.tabletAlias, td)
}

// CreateConsistentTableScanners will momentarily stop updates on the tablet, and then create connections that are all
// consistent snapshots of the same point in the transaction history
func CreateConsistentTableScanners(ctx context.Context, tablet *topo.TabletInfo, wr *wrangler.Wrangler, cleaner *wrangler.Cleaner, numberOfScanners int) ([]TableScanner, string, error) {
	txs, gtid, err := CreateConsistentTransactions(ctx, tablet, wr, cleaner, numberOfScanners)
	if err != nil {
		return nil, "", err
	}

	queryService, _ := tabletconn.GetDialer()(tablet.Tablet, true)
	defer queryService.Close(ctx)

	scanners := make([]TableScanner, numberOfScanners)
	for i, tx := range txs {
		scanners[i] = TransactionalTableScanner{
			wr:           wr,
			cleaner:      cleaner,
			tabletAlias:  tablet.Alias,
			queryService: queryService,
			tx:           tx,
		}
	}

	return scanners, gtid, nil
}

// CreateConsistentTransactions creates a number of consistent snapshot transactions,
// all starting from the same spot in the tx log
func CreateConsistentTransactions(ctx context.Context, tablet *topo.TabletInfo, wr *wrangler.Wrangler, cleaner *wrangler.Cleaner, numberOfScanners int) ([]int64, string, error) {
	if numberOfScanners < 1 {
		return nil, "", vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "need more than zero scanners: %d", numberOfScanners)
	}

	tm := tmclient.NewTabletManagerClient()
	defer tm.Close()

	// Lock all tables with a read lock to pause replication
	err := tm.LockTables(ctx, tablet.Tablet)
	if err != nil {
		return nil, "", vterrors.Wrapf(err, "could not lock tables on %v", topoproto.TabletAliasString(tablet.Tablet.Alias))
	}
	defer func() {
		tm := tmclient.NewTabletManagerClient()
		defer tm.Close()
		tm.UnlockTables(ctx, tablet.Tablet)
		wr.Logger().Infof("tables unlocked on %v", topoproto.TabletAliasString(tablet.Tablet.Alias))
	}()

	wr.Logger().Infof("tables locked on %v", topoproto.TabletAliasString(tablet.Tablet.Alias))
	target := CreateTargetFrom(tablet.Tablet)

	// Create transactions
	queryService, _ := tabletconn.GetDialer()(tablet.Tablet, true)
	defer queryService.Close(ctx)
	connections, err := createTransactions(ctx, numberOfScanners, wr, cleaner, queryService, target, tablet.Tablet)
	if err != nil {
		return nil, "", vterrors.Wrapf(err, "failed to create transactions on %v", topoproto.TabletAliasString(tablet.Tablet.Alias))
	}
	wr.Logger().Infof("transactions created on %v", topoproto.TabletAliasString(tablet.Tablet.Alias))
	executedGtid, err := tm.MasterPosition(ctx, tablet.Tablet)
	if err != nil {
		return nil, "", vterrors.Wrapf(err, "could not read executed GTID set on %v", topoproto.TabletAliasString(tablet.Tablet.Alias))
	}

	return connections, executedGtid, nil
}
