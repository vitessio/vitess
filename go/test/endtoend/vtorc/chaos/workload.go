/*
Copyright 2026 The Vitess Authors.

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

package chaos

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// WriteRecord is the client-side outcome of one INSERT.
type WriteRecord struct {
	ID     int64
	Worker int
	Start  time.Time
	End    time.Time
	Acked  bool // the INSERT (autocommit) returned success to the client
	Err    string
}

// Workload inserts rows with increasing ids through vtgate and records which were acknowledged.
// Only an INSERT that returned without error counts as acked; a timeout or connection error is
// "unknown" (it may or may not have committed) and is never counted as acked.
type Workload struct {
	db      *sql.DB
	next    atomic.Int64
	mu      sync.Mutex
	recs    []WriteRecord
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	log     *EventLog
	failing atomic.Bool
}

// StartWorkload starts `workers` writers each issuing one INSERT per `interval`.
func (c *Chaos) StartWorkload(workers int, interval time.Duration) *Workload {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/%s?timeout=1s&readTimeout=3s&writeTimeout=3s&interpolateParams=true",
		c.CI.VtgateProcess.MySQLServerPort, keyspaceName))
	if err != nil {
		c.t.Fatal(err)
	}
	db.SetMaxOpenConns(workers)
	db.SetMaxIdleConns(workers)
	ctx, cancel := context.WithCancel(context.Background())
	w := &Workload{db: db, cancel: cancel, log: c.Log}
	w.next.Store(1000000 * (time.Now().Unix() % 1000))
	for i := range workers {
		w.wg.Add(1)
		go w.run(ctx, i, interval)
	}
	return w
}

func (w *Workload) run(ctx context.Context, worker int, interval time.Duration) {
	defer w.wg.Done()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		id := w.next.Add(1)
		rec := WriteRecord{ID: id, Worker: worker, Start: time.Now()}
		// Per-statement deadline; the driver's readTimeout (3s) bounds it too.
		qctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		_, err := w.db.ExecContext(qctx, fmt.Sprintf("insert into %s (id, worker, src) values (?, ?, @@global.server_uuid)", tableName), id, worker)
		cancel()
		rec.End = time.Now()
		if err == nil {
			rec.Acked = true
			if w.failing.CompareAndSwap(true, false) {
				w.log.Add("writer", fmt.Sprintf("writes succeeding again (id %d)", id))
			}
		} else {
			rec.Err = err.Error()
			if w.failing.CompareAndSwap(false, true) {
				w.log.Add("writer", fmt.Sprintf("write failed (id %d): %v", id, err))
			}
		}
		w.mu.Lock()
		w.recs = append(w.recs, rec)
		w.mu.Unlock()
	}
}

// Stop stops the writers and waits for in-flight writes to finish (bounded by their timeouts).
func (w *Workload) Stop() {
	w.cancel()
	w.wg.Wait()
	w.db.Close()
}

func (w *Workload) Records() []WriteRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := append([]WriteRecord(nil), w.recs...)
	sort.Slice(r, func(i, j int) bool { return r[i].ID < r[j].ID })
	return r
}

// Stats summarizes the workload.
type WorkloadStats struct {
	Total, Acked, Failed int
	// LongestGap is the longest period without any acked write completing, and when it happened.
	LongestGap       time.Duration
	GapFrom, GapTo   time.Time
	FirstFailAfter   time.Time // first failed write started after the fault
	FirstAckAfterGap time.Time
}

func (w *Workload) Stats(fault time.Time) WorkloadStats {
	recs := w.Records()
	var st WorkloadStats
	var ends []time.Time
	for _, r := range recs {
		st.Total++
		if r.Acked {
			st.Acked++
			ends = append(ends, r.End)
		} else {
			st.Failed++
			if !fault.IsZero() && r.Start.After(fault) && (st.FirstFailAfter.IsZero() || r.Start.Before(st.FirstFailAfter)) {
				st.FirstFailAfter = r.Start
			}
		}
	}
	sort.Slice(ends, func(i, j int) bool { return ends[i].Before(ends[j]) })
	for i := 1; i < len(ends); i++ {
		if g := ends[i].Sub(ends[i-1]); g > st.LongestGap {
			st.LongestGap, st.GapFrom, st.GapTo = g, ends[i-1], ends[i]
		}
	}
	st.FirstAckAfterGap = st.GapTo
	return st
}
