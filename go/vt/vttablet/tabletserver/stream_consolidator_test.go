/*
Copyright 2021 The Vitess Authors.

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

package tabletserver

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"

	"vitess.io/vitess/go/sqltypes"
)

type consolidationResult struct {
	err      error
	items    []*sqltypes.Result
	duration time.Duration
	count    int64
}

func nocleanup(_ *sqltypes.Result) error {
	return nil
}

type consolidationTest struct {
	cc *StreamConsolidator

	streamDelay     time.Duration
	streamItemDelay time.Duration
	streamItemCount int
	streamItems     []*sqltypes.Result

	leaderCallback func(StreamCallback) error
	leaderCalls    int

	results []*consolidationResult
}

func generateResultSizes(size, count int) (r []*sqltypes.Result) {
	for i := 0; i < count; i++ {
		rows, _ := sqltypes.NewValue(querypb.Type_BINARY, make([]byte, size))
		item := &sqltypes.Result{InsertID: uint64(i), Rows: [][]sqltypes.Value{
			{rows},
		}}

		r = append(r, item)
	}
	return r
}

func (ct *consolidationTest) leader(stream StreamCallback) error {
	ct.leaderCalls++
	if ct.leaderCallback != nil {
		return ct.leaderCallback(stream)
	}

	time.Sleep(ct.streamDelay)

	if ct.streamItems != nil {
		for _, item := range ct.streamItems {
			cpy := *item
			if err := stream(&cpy); err != nil {
				return err
			}
			time.Sleep(ct.streamItemDelay)
		}
	} else {
		for i := 0; i < ct.streamItemCount; i++ {
			item := &sqltypes.Result{InsertID: uint64(i)}
			if err := stream(item); err != nil {
				return err
			}
			time.Sleep(ct.streamItemDelay)
		}
	}

	return nil
}

func (ct *consolidationTest) waitForResults(worker int, count int64) {
	for {
		if atomic.LoadInt64(&ct.results[worker].count) > count {
			return
		}
		time.Sleep(1 * time.Millisecond)
	}
}

func (ct *consolidationTest) run(workers int, generateCallback func(int) (string, StreamCallback)) {
	if ct.results == nil {
		ct.results = make([]*consolidationResult, workers)
		for i := 0; i < workers; i++ {
			ct.results[i] = &consolidationResult{}
		}
	}

	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)

		go func(worker int) {
			defer wg.Done()
			logStats := tabletenv.NewLogStats(context.Background(), "StreamConsolidation")
			query, callback := generateCallback(worker)
			start := time.Now()
			err := ct.cc.Consolidate(logStats, query, func(result *sqltypes.Result) error {
				cr := ct.results[worker]
				cr.items = append(cr.items, result)
				atomic.AddInt64(&cr.count, 1)
				return callback(result)
			}, ct.leader)

			cr := ct.results[worker]
			cr.err = err
			cr.duration = time.Since(start)
		}(i)
	}

	wg.Wait()
}

func TestConsolidatorSimple(t *testing.T) {
	ct := consolidationTest{
		cc:              NewStreamConsolidator(128*1024, 2*1024, nocleanup),
		streamItemDelay: 10 * time.Millisecond,
		streamItemCount: 10,
	}

	ct.run(10, func(worker int) (string, StreamCallback) {
		t.Helper()
		var inserts uint64

		return "select1", func(result *sqltypes.Result) error {
			require.Equal(t, inserts, result.InsertID)
			inserts++
			return nil
		}
	})

	require.Equal(t, 1, ct.leaderCalls)

	for _, results := range ct.results {
		require.Len(t, results.items, ct.streamItemCount)
		require.NoError(t, results.err)
	}
}

func TestConsolidatorErrorPropagation(t *testing.T) {
	t.Run("from mysql", func(t *testing.T) {
		ct := consolidationTest{
			cc: NewStreamConsolidator(128*1024, 2*1024, nocleanup),
			leaderCallback: func(callback StreamCallback) error {
				time.Sleep(100 * time.Millisecond)
				return fmt.Errorf("mysqld error")
			},
		}

		ct.run(4, func(worker int) (string, StreamCallback) {
			return "select 1", func(result *sqltypes.Result) error {
				return nil
			}
		})

		for _, results := range ct.results {
			require.Error(t, results.err)
		}
	})

	t.Run("from leader", func(t *testing.T) {
		ct := consolidationTest{
			cc:              NewStreamConsolidator(128*1024, 2*1024, nocleanup),
			streamItemDelay: 10 * time.Millisecond,
			streamItemCount: 10,
		}

		ct.run(4, func(worker int) (string, StreamCallback) {
			if worker == 0 {
				var rows int
				return "select 1", func(result *sqltypes.Result) error {
					if rows > 5 {
						return fmt.Errorf("leader streaming client disconnected")
					}
					rows++
					return nil
				}
			}
			time.Sleep(10 * time.Millisecond)
			return "select 1", func(result *sqltypes.Result) error {
				return nil
			}
		})

		for worker, results := range ct.results {
			if worker == 0 {
				require.Error(t, results.err)
			} else {
				require.NoError(t, results.err)
				require.Len(t, results.items, 10)
			}
		}
	})

	t.Run("from followers", func(t *testing.T) {
		ct := consolidationTest{
			cc:              NewStreamConsolidator(128*1024, 2*1024, nocleanup),
			streamItemDelay: 10 * time.Millisecond,
			streamItemCount: 10,
		}

		ct.run(4, func(worker int) (string, StreamCallback) {
			if worker == 0 {
				return "select 1", func(result *sqltypes.Result) error {
					return nil
				}
			}
			time.Sleep(10 * time.Millisecond)
			return "select 1", func(result *sqltypes.Result) error {
				if worker == 3 && result.InsertID == 5 {
					return fmt.Errorf("follower stream disconnected")
				}
				return nil
			}
		})

		for worker, results := range ct.results {
			switch worker {
			case 0, 1, 2:
				require.NoError(t, results.err)
				require.Len(t, results.items, 10)
			case 3:
				require.Error(t, results.err)
			}
		}
	})
}

func TestConsolidatorDelayedListener(t *testing.T) {
	ct := consolidationTest{
		cc:              NewStreamConsolidator(128*1024, 2*1024, nocleanup),
		streamItemDelay: 1 * time.Millisecond,
		streamItemCount: 100,
	}

	ct.run(4, func(worker int) (string, StreamCallback) {
		switch worker {
		case 0, 1, 2:
			return "select 1", func(_ *sqltypes.Result) error {
				return nil
			}

		case 3:
			time.Sleep(10 * time.Millisecond)
			return "select 1", func(result *sqltypes.Result) error {
				time.Sleep(100 * time.Millisecond)
				return nil
			}
		default:
			panic("??")
		}
	})

	require.Equal(t, 1, ct.leaderCalls)

	for worker, results := range ct.results {
		if worker == 3 {
			require.Error(t, results.err)
		} else {
			if results.duration > 1*time.Second {
				t.Fatalf("worker %d took too long (%v)", worker, results.duration)
			}
			require.Len(t, results.items, ct.streamItemCount)
			require.NoError(t, results.err)
		}
	}
}

func TestConsolidatorMemoryLimits(t *testing.T) {
	t.Run("rows too large", func(t *testing.T) {
		ct := consolidationTest{
			cc:              NewStreamConsolidator(128*1024, 32, nocleanup),
			streamItemDelay: 1 * time.Millisecond,
			streamItemCount: 100,
		}

		ct.run(4, func(worker int) (string, StreamCallback) {
			time.Sleep(time.Duration(worker) * 10 * time.Millisecond)
			return "select 1", func(_ *sqltypes.Result) error {
				return nil
			}
		})

		require.Equal(t, 4, ct.leaderCalls)

		for _, results := range ct.results {
			require.Len(t, results.items, ct.streamItemCount)
			require.NoError(t, results.err)
		}
	})

	t.Run("two-phase consolidation (time)", func(t *testing.T) {
		ct := consolidationTest{
			cc:              NewStreamConsolidator(128*1024, 2*1024, nocleanup),
			streamItemDelay: 10 * time.Millisecond,
			streamItemCount: 10,
		}

		ct.run(10, func(worker int) (string, StreamCallback) {
			if worker > 4 {
				time.Sleep(110 * time.Millisecond)
			}
			return "select 1", func(_ *sqltypes.Result) error {
				return nil
			}
		})

		require.Equal(t, 2, ct.leaderCalls)

		for _, results := range ct.results {
			require.Len(t, results.items, ct.streamItemCount)
			require.NoError(t, results.err)
		}
	})

	t.Run("two-phase consolidation (memory)", func(t *testing.T) {
		const streamsInFirstBatch = 5
		results := generateResultSizes(100, 10)
		rsize := results[0].CachedSize(true)

		ct := consolidationTest{
			cc:              NewStreamConsolidator(128*1024, rsize*streamsInFirstBatch+1, nocleanup),
			streamItemDelay: 1 * time.Millisecond,
			streamItems:     results,
		}

		ct.run(10, func(worker int) (string, StreamCallback) {
			if worker > 4 {
				ct.waitForResults(0, streamsInFirstBatch)
			}
			return "select 1", func(_ *sqltypes.Result) error { return nil }
		})

		require.Equal(t, 2, ct.leaderCalls)

		for _, results := range ct.results {
			require.Len(t, results.items, 10)
			require.NoError(t, results.err)
		}
	})

	t.Run("multiple phase consolidation", func(t *testing.T) {
		results := generateResultSizes(100, 10)
		rsize := results[0].CachedSize(true)

		ct := consolidationTest{
			cc:              NewStreamConsolidator(128*1024, rsize*2+1, nocleanup),
			streamItemDelay: 10 * time.Millisecond,
			streamItems:     results,
		}

		ct.run(20, func(worker int) (string, StreamCallback) {
			switch {
			case worker >= 0 && worker <= 4:
			case worker >= 5 && worker <= 9:
				ct.waitForResults(0, 2)
			case worker >= 10 && worker <= 14:
				ct.waitForResults(5, 2)
			case worker >= 15 && worker <= 19:
				ct.waitForResults(10, 2)
			}
			return "select 1", func(_ *sqltypes.Result) error { return nil }
		})

		require.Equal(t, 4, ct.leaderCalls)

		for _, results := range ct.results {
			require.Len(t, results.items, 10)
			require.NoError(t, results.err)
		}
	})
}
