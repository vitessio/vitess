/*
Copyright 2017 Google Inc.

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

// This file has the logic for performing merge-sorts of scatter queries.

package engine

import (
	"container/heap"
	"io"

	"golang.org/x/net/context"

	"vitess.io/vitess/go/sqltypes"

	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/srvtopo"
)

// mergeSort performs a merge-sort of rows returned by a streaming scatter query.
// Each shard of the scatter query is treated as a stream. One row from each stream
// is added to the merge-sorter heap. Every time a value is pulled out of the heap,
// a new value is added to it from the stream that was the source of the value that
// was pulled out. Since the input streams are sorted the same way that the heap is
// sorted, this guarantees that the merged stream will also be sorted the same way.
func mergeSort(vcursor VCursor, query string, orderBy []OrderbyParams, rss []*srvtopo.ResolvedShard, bvs []map[string]*querypb.BindVariable, callback func(*sqltypes.Result) error) error {
	ctx, cancel := context.WithCancel(vcursor.Context())
	defer cancel()

	handles := make([]*streamHandle, len(rss))
	id := 0
	for i, rs := range rss {
		handles[id] = runOneStream(ctx, vcursor, query, rs, bvs[i])
		id++
	}

	// Fetch field info from just one stream.
	fields := <-handles[0].fields
	// If fields is nil, it means there was an error.
	if fields == nil {
		return handles[0].err
	}
	if err := callback(&sqltypes.Result{Fields: fields}); err != nil {
		return err
	}

	sh := &scatterHeap{
		rows:    make([]streamRow, 0, len(handles)),
		orderBy: orderBy,
	}

	// Prime the heap. One element must be pulled from
	// each stream.
	for i, handle := range handles {
		select {
		case row, ok := <-handle.row:
			if !ok {
				if handle.err != nil {
					return handle.err
				}
				// It's possible that a stream returns no rows.
				// If so, don't add anything to the heap.
				continue
			}
			sh.rows = append(sh.rows, streamRow{row: row, id: i})
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	heap.Init(sh)
	if sh.err != nil {
		return sh.err
	}

	// Iterate one row at a time:
	// Pop a row from the heap and send it out.
	// Then pull the next row from the stream the popped
	// row came from and push it into the heap.
	for len(sh.rows) != 0 {
		sr := heap.Pop(sh).(streamRow)
		if sh.err != nil {
			// Unreachable: This should never fail.
			return sh.err
		}
		if err := callback(&sqltypes.Result{Rows: [][]sqltypes.Value{sr.row}}); err != nil {
			return err
		}

		select {
		case row, ok := <-handles[sr.id].row:
			if !ok {
				if handles[sr.id].err != nil {
					return handles[sr.id].err
				}
				continue
			}
			sr.row = row
			heap.Push(sh, sr)
			if sh.err != nil {
				return sh.err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// streamHandle is the rendez-vous point between each stream and the merge-sorter.
// The fields channel is used by the stream to transmit the field info, which
// is the first packet. Following this, the stream sends each row to the row
// channel. At the end of the stream, fields and row are closed. If there
// was an error, err is set before the channels are closed. The mergeSort
// routine that pulls the rows out of each streamHandle can abort the stream
// by calling canceling the context.
type streamHandle struct {
	fields chan []*querypb.Field
	row    chan []sqltypes.Value
	err    error
}

// runOnestream starts a streaming query on one shard, and returns a streamHandle for it.
func runOneStream(ctx context.Context, vcursor VCursor, query string, rs *srvtopo.ResolvedShard, vars map[string]*querypb.BindVariable) *streamHandle {
	handle := &streamHandle{
		fields: make(chan []*querypb.Field, 1),
		row:    make(chan []sqltypes.Value, 10),
	}

	go func() {
		defer close(handle.fields)
		defer close(handle.row)

		handle.err = vcursor.StreamExecuteMulti(
			query,
			[]*srvtopo.ResolvedShard{rs},
			[]map[string]*querypb.BindVariable{vars},
			func(qr *sqltypes.Result) error {
				if len(qr.Fields) != 0 {
					select {
					case handle.fields <- qr.Fields:
					case <-ctx.Done():
						return io.EOF
					}
				}

				for _, row := range qr.Rows {
					select {
					case handle.row <- row:
					case <-ctx.Done():
						return io.EOF
					}
				}
				return nil
			},
		)
	}()

	return handle
}

// A streamRow represents a row identified by the stream
// it came from. It is used as an element in scatterHeap.
type streamRow struct {
	row []sqltypes.Value
	id  int
}

// scatterHeap is the heap that is used for merge-sorting.
// You can push streamRow elements into it. Popping an
// element will return the one with the lowest value
// as defined by the orderBy criteria. If a comparison
// yielded an error, err is set. This must be checked
// after every heap operation.
type scatterHeap struct {
	rows    []streamRow
	orderBy []OrderbyParams
	err     error
}

// Len satisfies sort.Interface and heap.Interface.
func (sh *scatterHeap) Len() int {
	return len(sh.rows)
}

// Less satisfies sort.Interface and heap.Interface.
func (sh *scatterHeap) Less(i, j int) bool {
	for _, order := range sh.orderBy {
		if sh.err != nil {
			return true
		}
		cmp, err := sqltypes.NullsafeCompare(sh.rows[i].row[order.Col], sh.rows[j].row[order.Col])
		if err != nil {
			sh.err = err
			return true
		}
		if cmp == 0 {
			continue
		}
		if order.Desc {
			cmp = -cmp
		}
		return cmp < 0
	}
	return true
}

// Swap satisfies sort.Interface and heap.Interface.
func (sh *scatterHeap) Swap(i, j int) {
	sh.rows[i], sh.rows[j] = sh.rows[j], sh.rows[i]
}

// Push satisfies heap.Interface.
func (sh *scatterHeap) Push(x interface{}) {
	sh.rows = append(sh.rows, x.(streamRow))
}

// Pop satisfies heap.Interface.
func (sh *scatterHeap) Pop() interface{} {
	n := len(sh.rows)
	x := sh.rows[n-1]
	sh.rows = sh.rows[:n-1]
	return x
}
