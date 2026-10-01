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
	reads   []ReadRecord
	probes  []ProbeRecord
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	log     *EventLog
	failing atomic.Bool
	// stopped is when Stop was called: the end of the scenario for availability metrics.
	stopped time.Time
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
	w.mu.Lock()
	w.stopped = time.Now()
	w.mu.Unlock()
	w.cancel()
	w.wg.Wait()
	w.db.Close()
}

// ReadRecord is one primary read through vtgate, and the server that answered it.
type ReadRecord struct {
	Start, End time.Time
	UUID       string
	Err        string
}

// StartReader reads @@global.server_uuid through vtgate (from the PRIMARY, vtgate's default)
// every interval, to measure which mysqld vtgate routes primary traffic to.
func (w *Workload) StartReader(port int, interval time.Duration) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/%s?timeout=1s&readTimeout=2s&writeTimeout=2s&interpolateParams=true", port, keyspaceName))
	if err != nil {
		return
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithCancel(context.Background())
	prev := w.cancel
	w.cancel = func() { cancel(); prev() }
	w.wg.Go(func() {
		defer db.Close()
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			rec := ReadRecord{Start: time.Now()}
			qctx, qcancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := db.QueryRowContext(qctx, fmt.Sprintf("select @@global.server_uuid from %s limit 1", tableName)).Scan(&rec.UUID)
			qcancel()
			rec.End = time.Now()
			if err != nil {
				rec.Err = err.Error()
			}
			w.mu.Lock()
			w.reads = append(w.reads, rec)
			w.mu.Unlock()
		}
	})
}

// ProbeRecord is one transaction of the write probe: which mysqld vtgate routed it to, and how
// long its client waited for the outcome.
type ProbeRecord struct {
	Start time.Time
	// UUID is the server_uuid of the mysqld that answered the transaction's first statement,
	// empty if that statement failed.
	UUID string
	End  time.Time
	// Acked is set when the commit returned success.
	Acked bool
	Err   string
}

// StartWriteProbe starts a transaction through vtgate every interval, with at most maxInFlight
// outstanding. Each transaction reads @@global.server_uuid, which tells which mysqld vtgate routed
// it to, inserts a row and commits. Unlike the writers, whose driver timeouts cut a write off after
// 3s, a probe waits up to timeout: it measures how long a client blocks on a primary that cannot
// commit before it gets an error.
func (w *Workload) StartWriteProbe(port int, interval, timeout time.Duration, maxInFlight int) {
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(127.0.0.1:%d)/%s?timeout=1s&readTimeout=%s&writeTimeout=%s&interpolateParams=true",
		port, keyspaceName, timeout, timeout))
	if err != nil {
		return
	}
	db.SetMaxOpenConns(maxInFlight)
	db.SetMaxIdleConns(maxInFlight)
	ctx, cancel := context.WithCancel(context.Background())
	prev := w.cancel
	w.cancel = func() { cancel(); prev() }
	sem := make(chan struct{}, maxInFlight)
	w.wg.Go(func() {
		defer db.Close()
		var inFlight sync.WaitGroup
		defer inFlight.Wait()
		tick := time.NewTicker(interval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			select {
			case sem <- struct{}{}:
			default:
				continue // maxInFlight probes are still waiting
			}
			id := w.next.Add(1)
			inFlight.Go(func() {
				defer func() { <-sem }()
				rec := w.probe(db, id, timeout)
				w.mu.Lock()
				w.probes = append(w.probes, rec)
				w.mu.Unlock()
			})
		}
	})
}

func (w *Workload) probe(db *sql.DB, id int64, timeout time.Duration) ProbeRecord {
	rec := ProbeRecord{Start: time.Now()}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err == nil {
		err = tx.QueryRowContext(ctx, fmt.Sprintf("select @@global.server_uuid from %s limit 1", tableName)).Scan(&rec.UUID)
		if err == nil {
			_, err = tx.ExecContext(ctx, fmt.Sprintf("insert into %s (id, worker, src) values (?, -1, @@global.server_uuid)", tableName), id)
		}
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}
	rec.End = time.Now()
	if err != nil {
		rec.Err = err.Error()
	} else {
		rec.Acked = true
	}
	return rec
}

// Probes returns the write probe's transactions.
func (w *Workload) Probes() []ProbeRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := append([]ProbeRecord(nil), w.probes...)
	sort.Slice(r, func(i, j int) bool { return r[i].Start.Before(r[j].Start) })
	return r
}

// Reads returns the primary reads.
func (w *Workload) Reads() []ReadRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]ReadRecord(nil), w.reads...)
}

func (w *Workload) Records() []WriteRecord {
	w.mu.Lock()
	defer w.mu.Unlock()
	r := append([]WriteRecord(nil), w.recs...)
	sort.Slice(r, func(i, j int) bool { return r[i].ID < r[j].ID })
	return r
}

// outageThreshold is the shortest interval without an acknowledged write that counts as an outage
// in WorkloadStats.Unavailable. The writers issue about 100 writes per second in total, so a
// healthy primary acknowledges one every few tens of milliseconds.
const outageThreshold = time.Second

// Outage is an interval without any acknowledged write.
type Outage struct {
	From, To time.Time
	// Ongoing is set when the outage lasted until the writers stopped: To is then the time the
	// writers were stopped, not an acknowledged write.
	Ongoing bool
}

// Duration is the length of the outage.
func (o Outage) Duration() time.Duration { return o.To.Sub(o.From) }

// WorkloadStats summarizes the workload.
type WorkloadStats struct {
	Total, Acked, Failed int
	// LongestGap is the longest period without any acked write completing, and when it happened.
	// A period that lasted until the writers stopped counts (GapOngoing), measured up to Stopped.
	LongestGap       time.Duration
	GapFrom, GapTo   time.Time
	GapOngoing       bool
	FirstFailAfter   time.Time // first failed write started after the fault
	FirstAckAfterGap time.Time
	// Outages are the periods of at least outageThreshold without an acked write, including one
	// still ongoing when the writers stopped, and Unavailable is their total length.
	Outages     []Outage
	Unavailable time.Duration
	Stopped     time.Time
}

func (w *Workload) Stats(fault time.Time) WorkloadStats {
	w.mu.Lock()
	stopped := w.stopped
	w.mu.Unlock()
	return computeStats(w.Records(), fault, stopped)
}

// computeStats computes the workload statistics of recs. stopped is when the writers were stopped
// (zero if they were not): an interval without an acked write that was still going on then counts
// as a gap and an outage, ending at stopped.
func computeStats(recs []WriteRecord, fault, stopped time.Time) WorkloadStats {
	st := WorkloadStats{Stopped: stopped}
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
	var gaps []Outage
	for i := 1; i < len(ends); i++ {
		gaps = append(gaps, Outage{From: ends[i-1], To: ends[i]})
	}
	if !stopped.IsZero() {
		// The writers stopped during an outage: it lasted from the last acked write (or the
		// fault, if no write was ever acked) until the writers stopped.
		last := fault
		if len(ends) > 0 {
			last = ends[len(ends)-1]
		}
		if !last.IsZero() && stopped.After(last) {
			gaps = append(gaps, Outage{From: last, To: stopped, Ongoing: true})
		}
	}
	for _, g := range gaps {
		d := g.Duration()
		if d > st.LongestGap {
			st.LongestGap, st.GapFrom, st.GapTo, st.GapOngoing = d, g.From, g.To, g.Ongoing
		}
		if d >= outageThreshold {
			st.Outages = append(st.Outages, g)
			st.Unavailable += d
		}
	}
	if !st.GapOngoing {
		st.FirstAckAfterGap = st.GapTo
	}
	return st
}
