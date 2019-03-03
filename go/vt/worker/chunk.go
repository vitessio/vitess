/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package worker

import (
	"fmt"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqlescape"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/wrangler"

	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

var (
	completeChunk       = chunk{sqltypes.NULL, sqltypes.NULL, 1, 1}
	singleCompleteChunk = []chunk{completeChunk}
)

// chunk holds the information which subset of the table should be worked on.
// The subset is the range of rows in the range [start, end) where start and end
// both refer to the first column of the primary key.
// If the column is not numeric, both start and end will be sqltypes.NULL.
type chunk struct {
	start sqltypes.Value
	end   sqltypes.Value
	// number records the position of this chunk among all "total" chunks.
	// The lowest value is 1.
	number int
	// total is the total number of chunks this chunk belongs to.
	total int
}

// String returns a human-readable presentation of the chunk range.
func (c chunk) String() string {
	// Pad the chunk number such that all log messages align nicely.
	digits := digits(c.total)
	return fmt.Sprintf("%*d/%d", digits, c.number, c.total)
}

func digits(i int) int {
	digits := 1
	for {
		i /= 10
		if i == 0 {
			break
		}
		digits++
	}
	return digits
}

// generateChunks returns an array of chunks to use for splitting up a table
// into multiple data chunks. It only works for tables with a primary key
// whose first column is a numeric type.
func generateChunks(ctx context.Context, wr *wrangler.Wrangler, tablet *topodatapb.Tablet, td *tabletmanagerdatapb.TableDefinition, chunkCount, minRowsPerChunk int) ([]chunk, error) {
	if len(td.PrimaryKeyColumns) == 0 {
		// No explicit primary key. Cannot chunk the rows then.
		wr.Logger().Infof("table=%v: Not splitting the table into multiple chunks because it has no primary key columns. This will reduce the performance of the clone.", td.Name)
		return singleCompleteChunk, nil
	}
	if td.RowCount < 2*uint64(minRowsPerChunk) {
		// The automatic adjustment of "chunkCount" based on "minRowsPerChunk"
		// below would set "chunkCount" to less than 2 i.e. 1 or 0 chunks.
		// In practice in this case there should be exactly one chunk.
		// Return early in this case and notice the user about this.
		wr.Logger().Infof("table=%v: Not splitting the table into multiple chunks because it has only %d rows.", td.Name, td.RowCount)
		return singleCompleteChunk, nil
	}
	if chunkCount == 1 {
		return singleCompleteChunk, nil
	}

	// Get the MIN and MAX of the leading column of the primary key.
	query := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v.%v", sqlescape.EscapeID(td.PrimaryKeyColumns[0]), sqlescape.EscapeID(td.PrimaryKeyColumns[0]), sqlescape.EscapeID(topoproto.TabletDbName(tablet)), sqlescape.EscapeID(td.Name))
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	qr, err := wr.TabletManagerClient().ExecuteFetchAsApp(shortCtx, tablet, true, []byte(query), 1)
	cancel()
	if err != nil {
		return nil, vterrors.Wrapf(err, "tablet: %v, table: %v: cannot determine MIN and MAX of the first primary key column. ExecuteFetchAsApp", topoproto.TabletAliasString(tablet.Alias), td.Name)
	}
	if len(qr.Rows) != 1 {
		return nil, vterrors.Errorf(vtrpc.Code_FAILED_PRECONDITION, "tablet: %v, table: %v: cannot determine MIN and MAX of the first primary key column. Zero rows were returned", topoproto.TabletAliasString(tablet.Alias), td.Name)
	}

	result := sqltypes.Proto3ToResult(qr)
	min, _ := sqltypes.ToNative(result.Rows[0][0])
	max, _ := sqltypes.ToNative(result.Rows[0][1])

	if min == nil || max == nil {
		wr.Logger().Infof("table=%v: Not splitting the table into multiple chunks, min or max is NULL: %v", td.Name, qr.Rows[0])
		return singleCompleteChunk, nil
	}

	// Determine the average number of rows per chunk for the given chunkCount.
	avgRowsPerChunk := td.RowCount / uint64(chunkCount)
	if avgRowsPerChunk < uint64(minRowsPerChunk) {
		// Reduce the chunkCount to fulfill minRowsPerChunk.
		newChunkCount := td.RowCount / uint64(minRowsPerChunk)
		wr.Logger().Infof("table=%v: Reducing the number of chunks from the default %d to %d to make sure that each chunk has at least %d rows.", td.Name, chunkCount, newChunkCount, minRowsPerChunk)
		chunkCount = int(newChunkCount)
	}

	// TODO(mberlin): Write a unit test for this part of the function.
	var interval interface{}
	chunks := make([]chunk, chunkCount)
	switch min := min.(type) {
	case int64:
		max := max.(int64)
		interval = (max - min) / int64(chunkCount)
		if interval == 0 {
			wr.Logger().Infof("table=%v: Not splitting the table into multiple chunks, interval=0: %v to %v", td.Name, min, max)
			return singleCompleteChunk, nil
		}
	case uint64:
		max := max.(uint64)
		interval = (max - min) / uint64(chunkCount)
		if interval == 0 {
			wr.Logger().Infof("table=%v: Not splitting the table into multiple chunks, interval=0: %v to %v", td.Name, min, max)
			return singleCompleteChunk, nil
		}
	case float64:
		max := max.(float64)
		interval = (max - min) / float64(chunkCount)
		if interval == 0 {
			wr.Logger().Infof("table=%v: Not splitting the table into multiple chunks, interval=0: %v to %v", td.Name, min, max)
			return singleCompleteChunk, nil
		}
	default:
		wr.Logger().Infof("table=%v: Not splitting the table into multiple chunks, primary key not numeric.", td.Name)
		return singleCompleteChunk, nil
	}

	// Create chunks.
	start := min
	for i := 0; i < chunkCount; i++ {
		end := add(start, interval)
		chunk, err := toChunk(start, end, i+1, chunkCount)
		if err != nil {
			return nil, vterrors.Wrapf(err, "tablet: %v, table: %v", topoproto.TabletAliasString(tablet.Alias), td.Name)
		}
		chunks[i] = chunk
		start = end
	}

	// Clear out the MIN and MAX on the first and last chunk respectively
	// because other shards might have smaller or higher values than the one we
	// looked at.
	chunks[0].start = sqltypes.NULL
	chunks[chunkCount-1].end = sqltypes.NULL
	return chunks, nil
}

func add(start, interval interface{}) interface{} {
	switch start := start.(type) {
	case int64:
		return start + interval.(int64)
	case uint64:
		return start + interval.(uint64)
	case float64:
		return start + interval.(float64)
	default:
		panic(fmt.Sprintf("unsupported type %T for interval start: %v", start, start))
	}
}

func toChunk(start, end interface{}, number, total int) (chunk, error) {
	startValue, err := sqltypes.InterfaceToValue(start)
	if err != nil {
		return chunk{}, vterrors.Wrapf(err, "failed to convert calculated start value (%v) into internal sqltypes.Value", start)
	}
	endValue, err := sqltypes.InterfaceToValue(end)
	if err != nil {
		return chunk{}, vterrors.Wrapf(err, "failed to convert calculated end value (%v) into internal sqltypes.Value", end)
	}
	return chunk{startValue, endValue, number, total}, nil
}
