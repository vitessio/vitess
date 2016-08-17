package worker

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/sqltypes"
	"github.com/youtube/vitess/go/vt/topo/topoproto"
	"github.com/youtube/vitess/go/vt/wrangler"

	tabletmanagerdatapb "github.com/youtube/vitess/go/vt/proto/tabletmanagerdata"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
func generateChunks(ctx context.Context, wr *wrangler.Wrangler, tablet *topodatapb.Tablet, td *tabletmanagerdatapb.TableDefinition, minTableSizeForSplit uint64, chunkCount int) ([]chunk, error) {
	if len(td.PrimaryKeyColumns) == 0 {
		// No explicit primary key. Cannot chunk the rows then.
		wr.Logger().Infof("Not splitting table %v into multiple chunks because it has no primary key columns. This will reduce the performance of the clone.", td.Name)
		return singleCompleteChunk, nil
	}
	if td.DataLength < minTableSizeForSplit {
		// Table is too small to split up.
		return singleCompleteChunk, nil
	}
	if chunkCount == 1 {
		return singleCompleteChunk, nil
	}

	// Get the MIN and MAX of the leading column of the primary key.
	query := fmt.Sprintf("SELECT MIN(%v), MAX(%v) FROM %v.%v", escape(td.PrimaryKeyColumns[0]), escape(td.PrimaryKeyColumns[0]), escape(topoproto.TabletDbName(tablet)), escape(td.Name))
	shortCtx, cancel := context.WithTimeout(ctx, *remoteActionsTimeout)
	qr, err := wr.TabletManagerClient().ExecuteFetchAsApp(shortCtx, tablet, true, []byte(query), 1)
	cancel()
	if err != nil {
		return nil, fmt.Errorf("Cannot determine MIN and MAX of the first primary key column. ExecuteFetchAsApp: %v", err)
	}
	if len(qr.Rows) != 1 {
		return nil, fmt.Errorf("Cannot determine MIN and MAX of the first primary key column. Zero rows were returned for the following query: %v", query)
	}

	result := sqltypes.Proto3ToResult(qr)
	min := result.Rows[0][0].ToNative()
	max := result.Rows[0][1].ToNative()

	if min == nil || max == nil {
		wr.Logger().Infof("Not splitting table %v into multiple chunks, min or max is NULL: %v", td.Name, qr.Rows[0])
		return singleCompleteChunk, nil
	}

	// TODO(mberlin): Write a unit test for this part of the function.
	var interval interface{}
	chunks := make([]chunk, chunkCount)
	switch min := min.(type) {
	case int64:
		max := max.(int64)
		interval = (max - min) / int64(chunkCount)
		if interval == 0 {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v to %v", td.Name, min, max)
			return singleCompleteChunk, nil
		}
	case uint64:
		max := max.(uint64)
		interval = (max - min) / uint64(chunkCount)
		if interval == 0 {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v to %v", td.Name, min, max)
			return singleCompleteChunk, nil
		}
	case float64:
		max := max.(float64)
		interval = (max - min) / float64(chunkCount)
		if interval == 0 {
			wr.Logger().Infof("Not splitting table %v into multiple chunks, interval=0: %v to %v", td.Name, min, max)
			return singleCompleteChunk, nil
		}
	default:
		wr.Logger().Infof("Not splitting table %v into multiple chunks, primary key not numeric.", td.Name)
		return singleCompleteChunk, nil
	}

	// Create chunks.
	start := min
	for i := 0; i < chunkCount; i++ {
		end := add(start, interval)
		chunk, err := toChunk(start, end, i+1, chunkCount)
		if err != nil {
			return nil, err
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
		interval := interval.(int64)
		return start + interval
	case uint64:
		interval := interval.(uint64)
		return start + interval
	case float64:
		interval := interval.(float64)
		return start + interval
	default:
		panic(fmt.Sprintf("unsupported type %T for interval start: %v", start, start))
	}
}

func toChunk(start, end interface{}, number, total int) (chunk, error) {
	startValue, err := sqltypes.BuildValue(start)
	if err != nil {
		return chunk{}, fmt.Errorf("Failed to convert calculated start value (%v) into internal sqltypes.Value: %v", start, err)
	}
	endValue, err := sqltypes.BuildValue(end)
	if err != nil {
		return chunk{}, fmt.Errorf("Failed to convert calculated end value (%v) into internal sqltypes.Value: %v", end, err)
	}
	return chunk{startValue, endValue, number, total}, nil
}
