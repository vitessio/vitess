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

package vreplication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

type applyTxnPayload struct {
	// pos is the GTID position to record when committing this transaction.
	pos replication.Position
	// timestamp is the source binlog timestamp, used for lag calculation
	// and the time_updated column in _vt.vreplication.
	timestamp int64
	// mustSave forces an immediate position save (e.g., stop position reached
	// or time-based flush bound exceeded).
	mustSave bool
	// events holds the VEvents that make up this transaction's data.
	// For row transactions these are ROW/FIELD events; for commitOnly
	// transactions this is typically a single DDL, OTHER, or COMMIT event.
	events []*binlogdatapb.VEvent
	// rowOnly is true when the transaction contains only ROW and FIELD events
	// (no DDL, OTHER, or JOURNAL). Row-only transactions can have writesets
	// computed for parallel conflict detection. FIELD events are pure metadata
	// and do not affect conflict detection.
	rowOnly bool
	// commitOnly is true for transactions that are applied by the commitLoop
	// on the main connection rather than by a worker (DDL, OTHER, JOURNAL,
	// position-only saves). Workers forward these directly to commitCh
	// without applying events or waiting on txn.done.
	commitOnly bool
	// updatePosOnly is true for position-only saves (idle timeout flush).
	// The commitLoop calls updatePos without applying any events.
	updatePosOnly bool
	// query/commit/client are the DB connection functions for this transaction.
	// For worker transactions, these are set by the worker after applying events.
	// For commitOnly transactions, these point to the main vplayer connection.
	query  func(ctx context.Context, sql string) (*sqltypes.Result, error)
	commit func() error
	client *vdbClient
	// Pre-computed during scheduling so commitLoop doesn't need to scan
	// all events to find the last qualifying timestamp for lag calculation.
	// Zero means no qualifying event was found.
	lastEventTimestamp   int64
	lastEventCurrentTime int64
}

var (
	applyTxnPool = sync.Pool{
		New: func() any { return new(applyTxn) },
	}
	applyTxnPayloadPool = sync.Pool{
		New: func() any { return new(applyTxnPayload) },
	}
)

// acquireApplyTxn gets an applyTxn from the pool with a fresh done channel.
// A fresh channel is allocated each time (not reused from the pool) because
// the worker captures a reference to the done channel via pendingDone before
// the txn is returned to the pool. If the channel were reused, the worker's
// pendingDone and the new txn's done would alias the same channel, and the
// drain here would steal the signal intended for the worker.
func acquireApplyTxn() *applyTxn {
	txn := applyTxnPool.Get().(*applyTxn)
	txn.done = make(chan struct{}, 1)
	return txn
}

// acquireApplyTxnPayload gets an applyTxnPayload from the pool.
func acquireApplyTxnPayload() *applyTxnPayload {
	return applyTxnPayloadPool.Get().(*applyTxnPayload)
}

// releaseApplyTxn returns an applyTxn and its payload to their pools.
// Must only be called after commitLoop has fully processed the txn.
func releaseApplyTxn(txn *applyTxn) {
	if txn.payload != nil {
		p := txn.payload
		*p = applyTxnPayload{}
		applyTxnPayloadPool.Put(p)
	}
	// Zero out the txn completely (including done channel). acquireApplyTxn
	// always creates a fresh done channel, so there's nothing to preserve.
	*txn = applyTxn{}
	applyTxnPool.Put(txn)
}

// computeLastEventTimestamp scans events in reverse to find the last event
// with a non-zero timestamp that isn't a throttled heartbeat. Returns the
// timestamp and currentTime from that event, or (0, 0) if none qualifies.
func computeLastEventTimestamp(events []*binlogdatapb.VEvent) (timestamp, currentTime int64) {
	for i := len(events) - 1; i >= 0; i-- {
		ev := events[i]
		if ev.Timestamp == 0 {
			continue
		}
		if ev.Type == binlogdatapb.VEventType_HEARTBEAT && ev.Throttled {
			continue
		}
		return ev.Timestamp, ev.CurrentTime
	}
	return 0, 0
}

// applyEventsParallel is the top-level orchestrator for the parallel applier.
// It creates N worker goroutines and a commitLoop goroutine, then runs
// scheduleLoop on the calling goroutine. On exit, it tears down the pipeline
// in order: close scheduler → wait workers → close commitCh → wait commitLoop.
func (vp *vplayer) applyEventsParallel(ctx context.Context, relay *relayLog) error {
	workerCount := vp.vr.workflowConfig.ParallelReplicationWorkers
	if workerCount <= 1 {
		return vp.applyEvents(ctx, relay)
	}

	// Mirror the serial applier: reset lag stats to MaxInt64 when we exit,
	// signalling that replication is no longer running.
	defer vp.vr.stats.ReplicationLagSeconds.Store(math.MaxInt64)
	defer vp.vr.stats.VReplicationLagGauges.Set(vp.idStr, math.MaxInt64)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	scheduler := newApplyScheduler(ctx)
	// Buffer 4x worker count to decouple worker throughput from commit
	// latency. Workers block when commitCh is full, stalling the pipeline.
	commitCh := make(chan *applyTxn, workerCount*4)
	applyErr := make(chan error, 2)
	workerErr := make(chan error, workerCount)

	workers := make([]*applyWorker, 0, workerCount)
	// Register the defer BEFORE the creation loop so that if creating
	// worker N fails, workers 0..N-1 are still closed. Without this,
	// a partial creation failure would leak DB connections.
	defer func() {
		for _, worker := range workers {
			worker.close()
		}
	}()
	for range workerCount {
		worker, err := newApplyWorker(ctx, vp.vr)
		if err != nil {
			return err
		}
		workers = append(workers, worker)
	}

	// Query FK constraints from the target database so that we can
	// generate writeset keys that create conflicts between child and
	// parent table transactions, preventing FK constraint violations
	// during parallel apply.
	fkRefs, err := queryFKRefs(vp.vr.dbClient, vp.vr.dbClient.DBName())
	if err != nil {
		// Non-fatal: if we can't read FK info, parallel apply still works
		// but may hit FK violations (which will cause a retry with
		// forceGlobal serialization).
		log.Error("Parallel apply: failed to query FK refs", slog.String("db", vp.vr.dbClient.DBName()), slog.Any("error", err))
		fkRefs = nil
	}
	if len(fkRefs) > 0 {
		for table, refs := range fkRefs {
			for _, ref := range refs {
				log.Info("Parallel apply: FK ref", slog.String("child", table), slog.String("parent", ref.ParentTable), slog.Any("childCols", ref.ChildColumnNames), slog.Any("referencedCols", ref.ReferencedColumnNames))
			}
		}
	} else {
		log.Info("Parallel apply: no FK refs found", slog.String("db", vp.vr.dbClient.DBName()))
	}
	vp.fkRefs = fkRefs
	vp.parentFKRefs = buildParentFKRefs(fkRefs)

	var wg sync.WaitGroup
	for i := range workerCount {
		worker := workers[i]
		wg.Go(func() {
			err := vp.workerLoop(ctx, scheduler, commitCh, worker)
			if err != nil && err != io.EOF {
				workerErr <- err
				cancel()
			}
		})
	}

	commitDone := make(chan struct{})
	go func() {
		defer close(commitDone)
		if err := vp.commitLoop(ctx, scheduler, commitCh); err != nil {
			applyErr <- err
			// Always cancel context when commitLoop exits with an error,
			// including io.EOF (stop position reached). This ensures
			// scheduleLoop and workers shut down promptly instead of
			// blocking on a commitCh that has no reader.
			cancel()
		}
	}()

	schedErr := vp.scheduleLoop(ctx, relay, scheduler)
	if schedErr != nil {
		applyErr <- schedErr
		if schedErr != io.EOF {
			cancel()
		}
	}

	scheduler.close()
	wg.Wait()
	close(commitCh)
	<-commitDone

	// Now that commitLoop is done, it's safe to rollback any leftover
	// transaction on the main connection. This must happen after commitDone
	// because commitOnlyTxn in the commitLoop also uses the main connection.
	vp.vr.dbClient.Rollback()

	// Drain all errors and prioritize real failures over io.EOF/context.Canceled.
	// Both scheduleLoop and commitLoop may send to applyErr, so we must drain
	// the channel fully to avoid masking a commit failure behind an io.EOF.
	var realErrs []error
	var hasEOF bool
drainApplyErrs:
	for {
		select {
		case err := <-applyErr:
			if err == io.EOF || errors.Is(err, context.Canceled) {
				hasEOF = hasEOF || err == io.EOF
			} else {
				realErrs = append(realErrs, err)
			}
		default:
			break drainApplyErrs
		}
	}

	var finalErr error
	if len(realErrs) > 0 {
		finalErr = errors.Join(realErrs...)
	} else if hasEOF {
		finalErr = io.EOF
	} else {
		// No applyErr — check worker errors. Drain all and join so that
		// no diagnostic information is silently dropped.
		var workerErrs []error
	drainWorkerErrs:
		for {
			select {
			case err := <-workerErr:
				workerErrs = append(workerErrs, err)
			default:
				break drainWorkerErrs
			}
		}
		if len(workerErrs) > 0 {
			finalErr = errors.Join(workerErrs...)
		}
	}
	// Convert io.EOF (stop position reached) and context.Canceled (shutdown)
	// to nil. fetchAndApply's caller treats nil from applyEventsParallel
	// the same as io.EOF from the serial path — it stops the controller
	// without retrying.
	if finalErr == io.EOF || errors.Is(finalErr, context.Canceled) {
		return nil
	}
	return finalErr
}

// scheduleLoop reads event batches from the relay log and dispatches them
// through scheduleItems. It also handles idle-timeout position saves and
// throttle-lag estimation. Runs on the main goroutine of applyEventsParallel.
func (vp *vplayer) scheduleLoop(ctx context.Context, relay *relayLog, scheduler *applyScheduler) error {
	// Note: do NOT defer vp.vr.dbClient.Rollback() here. The main connection
	// is shared with commitLoop (via commitOnlyTxn), which may still be running
	// when scheduleLoop returns. The rollback is deferred in applyEventsParallel
	// after commitLoop has finished.
	workerCount := vp.vr.workflowConfig.ParallelReplicationWorkers
	// Compute the max number of source transactions to batch into one
	// mega-transaction. With parallel workers, we need enough separate
	// mega-transactions per relay fetch to keep all workers busy.
	//
	// The relay log size limit (default 250KB) often limits each fetch to
	// far fewer transactions than maxItems (5000). With 1-2KB rows, a
	// typical fetch may contain only ~150-250 source transactions. To
	// ensure all workers get work, we limit each mega-transaction to a
	// small multiple of the worker count. This produces enough independent
	// mega-transactions for the scheduler to keep all workers busy.
	maxBatched := 0 // 0 means unlimited (serial behavior)
	if workerCount > 1 {
		// Batch multiple source transactions into each mega-transaction.
		// This amortizes per-commit overhead (position update, MySQL COMMIT,
		// done-signal, scheduler dispatch) across multiple source txns.
		// With workerCount*4, a single relay fetch produces enough
		// mega-transactions to keep all workers busy while still reducing
		// commit overhead by Nx. The writeset for the mega-txn is the
		// union of all contained source txns, so conflict detection
		// remains correct — if any source txn in mega-A conflicts with
		// any source txn in mega-B, they serialize.
		maxBatched = workerCount * 4
	}
	state := &parallelScheduleState{
		maxBatchedCommits: maxBatched,
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		vp.serialMu.Lock()
		if time.Since(vp.timeLastSaved) >= idleTimeout && vp.unsavedEvent != nil {
			event := vp.unsavedEvent
			vp.unsavedEvent = nil
			vp.timeLastSaved = time.Now()
			vp.serialMu.Unlock()
			if err := vp.enqueueCommitOnly(ctx, scheduler, event, true, true); err != nil {
				return err
			}
		} else {
			vp.serialMu.Unlock()
		}
		if checkResult, ok := vp.vr.vre.throttlerClient.ThrottleCheckOKOrWaitAppName(ctx, throttlerapp.Name(vp.throttlerAppName)); !ok {
			// Must hold serialMu when calling updateTimeThrottled because
			// it uses vr.dbClient, which may also be in use by the
			// commitLoop for commitOnly transactions on the main connection.
			vp.serialMu.Lock()
			_ = vp.vr.updateTimeThrottled(throttlerapp.VPlayerName, checkResult.Summary())
			vp.serialMu.Unlock()
			lastTs := vp.lastTimestampNs.Load()
			offset := vp.timeOffsetNs.Load()
			// Estimate lag while throttled, same as the serial applier.
			if lastTs > 0 {
				behind := time.Now().UnixNano() - lastTs - offset
				if behind >= 0 {
					behindSecs := behind / 1e9
					vp.vr.stats.ReplicationLagSeconds.Store(behindSecs)
					vp.vr.stats.VReplicationLagGauges.Set(vp.idStr, behindSecs)
				}
			}
			continue
		}
		items, err := relay.Fetch()
		if err != nil {
			return err
		}
		if err := vp.scheduleItems(ctx, scheduler, state, items); err != nil {
			return err
		}
		// If a DDL was in this fetch, wait for the commitLoop to process it
		// (including FK metadata refresh) before starting the next fetch.
		// Without this barrier, the next fetch would snapshot stale FK refs.
		if state.ddlSeen {
			if err := scheduler.waitForIdle(ctx); err != nil {
				return err
			}
		}
	}
}

type parallelScheduleState struct {
	// curEvents accumulates VEvents for the current transaction being built.
	// Reset after each flush (COMMIT or DDL boundary).
	curEvents []*binlogdatapb.VEvent
	// curRowOnly tracks whether the current transaction contains only ROW
	// events. Set to true on the first ROW event, false on FIELD/DDL/OTHER/
	// JOURNAL events. Only meaningful when curRowOnlySet is true.
	curRowOnly bool
	// curRowOnlySet indicates whether curRowOnly has been determined for the
	// current transaction. False at the start of each transaction; set to
	// true on the first event that classifies it. This distinguishes
	// "not yet classified" from "classified as not row-only".
	curRowOnlySet bool
	// curTimestamp is the most recent non-zero event timestamp seen in the
	// current transaction, used for the time_updated column on flush.
	curTimestamp int64
	// curMustSave forces the next flush to save the position immediately
	// (set when stop position is reached or time-based batch bound fires).
	curMustSave bool
	// curPos is the GTID position from the most recent GTID event,
	// recorded in _vt.vreplication when the transaction is committed.
	curPos replication.Position
	// curCommitParent is the source MySQL commit parent from the GTID event,
	// used for commit-parent ordering when writeset is unavailable.
	curCommitParent int64
	// curSequence is the source MySQL sequence number from the GTID event,
	// used to track lastCommittedSequence in the scheduler.
	curSequence int64
	// curHasCommitMeta is true when the current transaction's GTID event
	// carried non-zero sequenceNumber or commitParent metadata.
	curHasCommitMeta bool
	// lastFlushTime tracks when the last transaction was flushed, used to
	// enforce the 500ms time-based batch bound during catch-up replay.
	lastFlushTime time.Time
	// lastHeartbeatRefresh tracks when time_updated was last refreshed via
	// SQL for empty transaction streams, independent of lastFlushTime so
	// that the idle timeout position save still fires normally.
	lastHeartbeatRefresh time.Time
	// cachedPlanSnapshot is a copy-on-write snapshot of vplayer.tablePlans,
	// refreshed only when tablePlansVersion changes (new FIELD events).
	cachedPlanSnapshot map[string]*TablePlan
	// cachedPlanVersion tracks which tablePlansVersion the snapshot
	// corresponds to, so we know when to re-snapshot.
	cachedPlanVersion int64
	// fieldIdxCache caches the field-name→index map per table to avoid
	// rebuilding it on every transaction. Most transactions touch the same
	// tables so this eliminates redundant map construction. Invalidated
	// when tablePlansVersion changes (new FIELD events arrive).
	fieldIdxCache        map[string]map[string]int
	fieldIdxCacheVersion int64
	// batchedCommitCount tracks how many source transactions have been
	// merged into the current mega-transaction via commit batching. When
	// this exceeds maxBatchedCommits, the mega-transaction is flushed even
	// if more consecutive commits follow. This ensures the parallel applier
	// produces enough mega-transactions per relay fetch to keep all workers
	// busy, rather than merging everything into one huge transaction that
	// only a single worker can process.
	batchedCommitCount int
	// maxBatchedCommits is the maximum number of source transactions to
	// merge into one mega-transaction. Set once based on the relay log
	// max items and worker count.
	maxBatchedCommits int
	// mergedSequences tracks sequence numbers of transactions that were
	// merged into the current batch. These are advanced in the scheduler
	// when the batch is actually enqueued (not before), so that
	// commit-parent dependencies aren't prematurely satisfied.
	mergedSequences []int64
	// ddlSeen is set to true when a DDL event is seen in the current fetch.
	// The scheduleLoop checks this after scheduleItems returns and waits
	// for the commitLoop to drain (so FK refs are refreshed) before
	// starting the next fetch. Reset at the start of each scheduleItems call.
	ddlSeen bool
	// postDDLFetchPending is set when a DDL was observed in the previous
	// fetch. The NEXT fetch carries the new FIELD events that the workers
	// will apply to update vp.tablePlans. Until that happens, the scheduler
	// would build writesets from the stale plan. We force-serialize the
	// entire first post-DDL fetch so workers apply the new FIELD events
	// before any writeset-based parallel scheduling resumes.
	postDDLFetchPending bool
}

// scheduleItems processes one relay log fetch worth of event batches. It tracks
// transaction boundaries (GTID → events → COMMIT), classifies transactions,
// builds writesets, handles batching of consecutive commits, and enqueues
// applyTxn structs into the scheduler. Empty transactions bypass the scheduler
// and are saved via unsavedEvent / idle timeout.
func (vp *vplayer) scheduleItems(ctx context.Context, scheduler *applyScheduler, state *parallelScheduleState, items [][]*binlogdatapb.VEvent) error {
	// Snapshot FK refs under serialMu so we have a consistent view for this
	// relay fetch. The commitLoop may update these after DDL events.
	vp.serialMu.Lock()
	fkRefs := vp.fkRefs
	parentFKRefs := vp.parentFKRefs
	vp.serialMu.Unlock()

	// After DDL events that may change schema or FK topology, force all
	// remaining transactions in this relay fetch to serialize. The
	// commitLoop will refresh FK metadata when the DDL commits, so the
	// next relay fetch will have updated snapshots.
	//
	// If the previous fetch contained a DDL, the current fetch carries the
	// new FIELD events that workers will use to refresh vp.tablePlans. Until
	// those are applied, the plan snapshot used for writeset construction
	// would still be the pre-DDL plan. Force-serialize this entire fetch so
	// execution observes the new plan before any parallel scheduling resumes.
	forceSerialize := state.postDDLFetchPending
	state.postDDLFetchPending = false
	state.ddlSeen = false

	flush := func(commitOnly bool) error {
		if len(state.curEvents) == 0 && !commitOnly {
			return nil
		}
		vp.serialMu.Lock()
		vp.parallelOrder++
		order := vp.parallelOrder
		vp.serialMu.Unlock()
		lastTs, lastCT := computeLastEventTimestamp(state.curEvents)
		payload := acquireApplyTxnPayload()
		payload.pos = state.curPos
		payload.timestamp = state.curTimestamp
		payload.mustSave = state.curMustSave
		payload.events = state.curEvents
		payload.rowOnly = state.curRowOnly
		payload.commitOnly = commitOnly
		payload.updatePosOnly = false
		payload.lastEventTimestamp = lastTs
		payload.lastEventCurrentTime = lastCT
		// query/commit/client are left nil here; the worker will
		// set them to its own connection before sending to commitCh.
		txn := acquireApplyTxn()
		txn.order = order
		txn.sequenceNumber = state.curSequence
		txn.commitParent = state.curCommitParent
		txn.hasCommitMeta = state.curHasCommitMeta
		txn.payload = payload
		if forceSerialize {
			txn.forceGlobal = true
		} else if state.curRowOnlySet && !state.curRowOnly {
			txn.forceGlobal = true
		} else if len(vp.copyState) != 0 {
			txn.forceGlobal = true
		} else {
			planSnapshot := snapshotTablePlans(vp.tablePlansMu, vp.tablePlans, vp.tablePlansVersion, &state.cachedPlanVersion, state.cachedPlanSnapshot)
			state.cachedPlanSnapshot = planSnapshot
			// Invalidate fieldIdxCache when table plans change (new FIELD events).
			if state.fieldIdxCacheVersion != state.cachedPlanVersion {
				state.fieldIdxCache = make(map[string]map[string]int)
				state.fieldIdxCacheVersion = state.cachedPlanVersion
			}
			writeset, err := buildTxnWriteset(planSnapshot, fkRefs, parentFKRefs, state.curEvents, state.fieldIdxCache)
			if err != nil {
				txn.forceGlobal = true
			} else {
				txn.writeset = writeset
			}
		}
		if err := scheduler.enqueue(txn); err != nil {
			return err
		}
		// Now that the batch is enqueued, advance any merged-away sequences.
		// This is safe because the batch is ahead of them in the commit queue,
		// so commit-parent dependencies will be satisfied in order.
		for _, seq := range state.mergedSequences {
			scheduler.advanceCommittedSequence(seq)
		}
		state.mergedSequences = state.mergedSequences[:0]
		// Pre-allocate with capacity 16 to avoid the nil→1→2→4→8 growth
		// pattern on the hot path. We can't reuse the old slice via [:0]
		// because the payload still references the backing array.
		state.curEvents = make([]*binlogdatapb.VEvent, 0, 16)
		state.curRowOnly = false
		state.curRowOnlySet = false
		state.curMustSave = false
		state.curTimestamp = 0
		state.curCommitParent = 0
		state.curSequence = 0
		state.curHasCommitMeta = false
		state.batchedCommitCount = 0
		state.lastFlushTime = time.Now()
		return nil
	}

	for i := range items {
		for j := 0; j < len(items[i]); j++ {
			event := items[i][j]
			switch event.Type {
			case binlogdatapb.VEventType_GTID:
				pos, err := binlogplayer.DecodePosition(event.Gtid)
				if err != nil {
					return err
				}
				state.curPos = pos
				state.curCommitParent = event.CommitParent
				state.curSequence = event.SequenceNumber
				state.curHasCommitMeta = event.SequenceNumber != 0 || event.CommitParent != 0
				vp.serialMu.Lock()
				vp.pos = pos
				vp.unsavedEvent = nil
				vp.serialMu.Unlock()
			case binlogdatapb.VEventType_ROW:
				state.curEvents = append(state.curEvents, event)
				if !state.curRowOnlySet {
					state.curRowOnly = true
					state.curRowOnlySet = true
				}
			case binlogdatapb.VEventType_COMMIT:
				state.curMustSave = !vp.stopPos.IsZero() && state.curPos.AtLeast(vp.stopPos)
				if len(state.curEvents) == 0 {
					if state.curMustSave {
						eventCopy := event
						if err := vp.enqueueCommitOnly(ctx, scheduler, eventCopy, true, true); err != nil {
							return err
						}
					} else {
						// During catch-up, a stream may continuously process
						// empty transactions (from other shards' data) that
						// keep the scheduleLoop busy, so the idle timeout at
						// the top of the loop never fires. Periodically refresh
						// time_updated directly via SQL to keep
						// max_v_replication_lag fresh. We use a separate
						// lastHeartbeatRefresh timer so that vp.timeLastSaved
						// remains stale and the idle timeout position save at
						// the top of scheduleLoop still fires normally.
						needRefresh := time.Since(state.lastHeartbeatRefresh) >= idleTimeout
						if needRefresh {
							state.lastHeartbeatRefresh = time.Now()
							vp.serialMu.Lock()
							err := vp.vr.updateHeartbeatTime(time.Now().Unix())
							vp.serialMu.Unlock()
							if err != nil {
								return err
							}
						}
						vp.serialMu.Lock()
						vp.unsavedEvent = event
						vp.serialMu.Unlock()
					}
					// Advance the scheduler's lastCommittedSequence for this
					// empty transaction. Without this, hasCommitMeta transactions
					// whose commitParent references this (skipped) sequence would
					// be blocked forever waiting for lastCommittedSequence to
					// reach their commitParent.
					if state.curHasCommitMeta {
						scheduler.advanceCommittedSequence(state.curSequence)
					}
					state.curEvents = make([]*binlogdatapb.VEvent, 0, 16)
					state.curRowOnly = false
					state.curRowOnlySet = false
					state.curMustSave = false
					state.curTimestamp = 0
					state.curCommitParent = 0
					state.curSequence = 0
					state.curHasCommitMeta = false
					continue
				}
				// Group multiple consecutive transactions into a single batch
				// to reduce the number of MySQL COMMITs. This mirrors the serial
				// applier's hasAnotherCommit lookahead. If another COMMIT is
				// ahead in this relay batch and we don't need to force-save,
				// skip the flush and let events accumulate. The next GTID will
				// update curPos/curSequence/curCommitParent and the accumulated
				// events will be flushed as one larger transaction.
				//
				// Time-based bound: during heavy catch-up, heartbeats don't
				// arrive to set curMustSave. Without a time bound, a single
				// batch can grow for 30+ seconds, keeping time_updated stale
				// and max_v_replication_lag stuck at 1+. Force a flush every
				// 500ms to keep lag fresh.
				if !state.lastFlushTime.IsZero() && time.Since(state.lastFlushTime) > 500*time.Millisecond {
					state.curMustSave = true
				}
				// When FK refs are present, skip batching to keep writesets
				// small. Large batches merge many parent/child operations
				// into a single writeset, causing nearly all batches to
				// conflict on FK ref keys and serializing the workload.
				// Flushing each source transaction individually lets the
				// scheduler detect truly independent transactions and run
				// them in parallel.
				hasFKRefs := len(fkRefs) > 0
				// With parallel workers, limit the mega-transaction size
				// to ensure enough transactions for all workers. Without
				// this limit, all consecutive commits in a relay fetch
				// merge into one mega-transaction, leaving all but one
				// worker idle.
				if state.maxBatchedCommits > 0 {
					state.batchedCommitCount++
					if state.batchedCommitCount >= state.maxBatchedCommits {
						state.curMustSave = true
					}
				}
				if !state.curMustSave && !hasFKRefs && hasAnotherCommit(items, i, j+1) {
					// Track merged sequence numbers so they can be advanced
					// when the batch actually commits. We must NOT advance
					// lastCommittedSequence here because the batch hasn't
					// committed yet. Empty-writeset transactions that depend
					// on commit-parent ordering would otherwise become
					// runnable too early.
					if state.curHasCommitMeta {
						state.mergedSequences = append(state.mergedSequences, state.curSequence)
					}
					// Reset only metadata — keep accumulated events and
					// rowOnly state. The next GTID will set new metadata.
					state.curCommitParent = 0
					state.curSequence = 0
					state.curHasCommitMeta = false
					state.curMustSave = false
					continue
				}
				if err := flush(false); err != nil {
					return err
				}
			case binlogdatapb.VEventType_BEGIN:
				// No-op: BEGIN is handled on-demand by workers when they encounter
				// ROW/FIELD events (via activeDBClient().Begin()). We intentionally
				// do NOT add BEGIN to curEvents so that empty transactions
				// (GTID→BEGIN→COMMIT) have curEvents=0 and take the fast path
				// (unsavedEvent) instead of being enqueued through the scheduler.
			case binlogdatapb.VEventType_FIELD:
				// FIELD events carry table metadata (column definitions) and
				// must be applied before the ROW events that follow them, but
				// they are emitted routinely by MySQL at the start of each
				// transaction — they do not indicate a schema change. The
				// execution plan only actually changes after DDL, which
				// already sets forceSerialize. Accumulate FIELD events like
				// ROW events so they stay in the same applyTxn.
				state.curEvents = append(state.curEvents, event)
			case binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER, binlogdatapb.VEventType_JOURNAL:
				if err := flush(false); err != nil {
					return err
				}
				vp.serialMu.Lock()
				vp.parallelOrder++
				order := vp.parallelOrder
				query := vp.query
				commit := vp.commit
				client := vp.dbClient
				vp.serialMu.Unlock()
				payload := acquireApplyTxnPayload()
				payload.pos = state.curPos
				payload.timestamp = event.Timestamp
				payload.mustSave = true
				payload.events = []*binlogdatapb.VEvent{event}
				payload.rowOnly = false
				payload.commitOnly = true
				payload.updatePosOnly = false
				payload.query = query
				payload.commit = commit
				payload.client = client
				payload.lastEventTimestamp = event.Timestamp
				payload.lastEventCurrentTime = event.CurrentTime
				txn := acquireApplyTxn()
				txn.order = order
				txn.sequenceNumber = event.SequenceNumber
				txn.commitParent = event.CommitParent
				txn.hasCommitMeta = event.SequenceNumber != 0 || event.CommitParent != 0
				txn.forceGlobal = true
				// OTHER events and DDL events with OnDdl=IGNORE only update the
				// replication position — they never touch user table data. Marking
				// them noConflict lets workers pick them up immediately without
				// waiting for all inflight row transactions to drain first. The
				// commitLoop still enforces strict ordering, so the position write
				// happens after all prior commits. This eliminates the forceGlobal
				// serialization stall that occurs during Online DDL cutover when the
				// RENAME TABLE DDL event arrives while workers are still applying rows.
				txn.noConflict = event.Type == binlogdatapb.VEventType_OTHER ||
					(event.Type == binlogdatapb.VEventType_DDL && vp.vr.source.OnDdl == binlogdatapb.OnDDLAction_IGNORE)
				txn.payload = payload
				if err := scheduler.enqueue(txn); err != nil {
					return err
				}
				// DDL may change schema or FK topology. Force all remaining
				// transactions in this relay fetch to serialize so they don't
				// use stale FK refs or table plans for writeset computation.
				// Also set state.ddlSeen so the scheduleLoop waits for the
				// commitLoop to refresh FK metadata before the next fetch,
				// and state.postDDLFetchPending so the next fetch (which
				// carries the new FIELD events) is also force-serialized
				// until workers apply those FIELDs and refresh the plan.
				if event.Type == binlogdatapb.VEventType_DDL {
					forceSerialize = true
					state.ddlSeen = true
					state.postDDLFetchPending = true
				}
			case binlogdatapb.VEventType_HEARTBEAT:
				// Handle heartbeats inline without enqueuing through the scheduler.
				// Heartbeats are very frequent (~250/sec) and enqueuing them as
				// forceGlobal transactions serializes the entire pipeline, making
				// VDiff sync cycles extremely slow.
				//
				// If we have accumulated events, force the next COMMIT to flush
				// instead of continuing to batch. Without this, commit batching
				// can create unbounded super-transactions during catch-up replay,
				// starving time_updated refreshes and causing max_v_replication_lag
				// to stay high indefinitely. Heartbeats arrive regularly from the
				// source (~1/sec), providing a natural bound on batch size.
				if len(state.curEvents) > 0 {
					state.curMustSave = true
				}
				//
				// Must hold serialMu for DB writes (updateTimeThrottled,
				// recordHeartbeat) because they use vr.dbClient, which may
				// also be in use by the commitLoop for commitOnly transactions.
				if event.Throttled {
					vp.serialMu.Lock()
					err := vp.vr.updateTimeThrottled(throttlerapp.VStreamerName, event.ThrottledReason)
					vp.serialMu.Unlock()
					if err != nil {
						return err
					}
				}
				vp.serialMu.Lock()
				vp.numAccumulatedHeartbeats++
				err := vp.recordHeartbeat()
				vp.serialMu.Unlock()
				if err != nil {
					return err
				}
				// Update lag from heartbeat timestamp.
				if event.Timestamp != 0 && !event.Throttled {
					tsNs := event.Timestamp * 1e9
					vp.lastTimestampNs.Store(tsNs)
					now := time.Now().UnixNano()
					offset := now - event.CurrentTime
					vp.timeOffsetNs.Store(offset)
					lag := now - tsNs - offset
					if lag >= 0 {
						lagSecs := lag / 1e9
						vp.vr.stats.ReplicationLagSeconds.Store(lagSecs)
						vp.vr.stats.VReplicationLagGauges.Set(vp.idStr, lagSecs)
					}
				} else if event.Throttled {
					// When the vstreamer is throttled, we can't determine the
					// actual lag from the event. Estimate it from the last known
					// timestamp, matching the serial applier's estimateLag().
					lastTs := vp.lastTimestampNs.Load()
					offset := vp.timeOffsetNs.Load()
					if lastTs > 0 {
						behind := time.Now().UnixNano() - lastTs - offset
						if behind >= 0 {
							behindSecs := behind / 1e9
							vp.vr.stats.ReplicationLagSeconds.Store(behindSecs)
							vp.vr.stats.VReplicationLagGauges.Set(vp.idStr, behindSecs)
						}
					}
				}
			default:
				state.curEvents = append(state.curEvents, event)
				if !state.curRowOnlySet {
					state.curRowOnly = false
					state.curRowOnlySet = true
				}
			}
			if event.Timestamp != 0 {
				state.curTimestamp = event.Timestamp
			}
		}
	}
	return nil
}

// enqueueCommitOnly creates a commitOnly transaction and enqueues it into the
// scheduler. Used for DDL, OTHER, JOURNAL events, and position-only saves
// (idle timeout). These transactions are applied by the commitLoop on the main
// connection, not by workers.
func (vp *vplayer) enqueueCommitOnly(ctx context.Context, scheduler *applyScheduler, event *binlogdatapb.VEvent, mustSave bool, updatePosOnly bool) error {
	var order int64
	var pos replication.Position
	var query func(ctx context.Context, sql string) (*sqltypes.Result, error)
	var commit func() error
	var client *vdbClient
	vp.serialMu.Lock()
	vp.parallelOrder++
	order = vp.parallelOrder
	pos = vp.pos
	query = vp.query
	commit = vp.commit
	client = vp.dbClient
	vp.serialMu.Unlock()
	payload := acquireApplyTxnPayload()
	payload.pos = pos
	payload.timestamp = event.Timestamp
	payload.mustSave = mustSave
	payload.events = []*binlogdatapb.VEvent{event}
	payload.rowOnly = false
	payload.commitOnly = true
	payload.updatePosOnly = updatePosOnly
	payload.query = query
	payload.commit = commit
	payload.client = client
	payload.lastEventTimestamp = event.Timestamp
	payload.lastEventCurrentTime = event.CurrentTime
	txn := acquireApplyTxn()
	txn.order = order
	txn.sequenceNumber = event.SequenceNumber
	txn.commitParent = event.CommitParent
	txn.hasCommitMeta = event.SequenceNumber != 0 || event.CommitParent != 0
	txn.forceGlobal = true
	txn.noConflict = updatePosOnly
	txn.payload = payload
	// done is nil for commitOnly transactions; workers send them
	// directly to commitCh without waiting for completion.
	return scheduler.enqueue(txn)
}

// workerLoop runs on each of the N worker goroutines. It blocks on
// scheduler.nextReady() until a transaction is dispatched, applies the row
// events using the worker's private MySQL connection, then sends the txn
// to commitCh. Each worker has double-buffered connections: after sending
// a transaction, the worker rotates to its spare connection and immediately
// starts the next transaction, overlapping apply with the commitLoop's commit.
func (vp *vplayer) workerLoop(ctx context.Context, scheduler *applyScheduler, commitCh chan<- *applyTxn, worker *applyWorker) error {
	// Shallow-copy vplayer once per worker lifetime instead of per
	// transaction. This eliminates a ~200-300 byte struct copy on every
	// txn in the hot path. The FK check state is reset once here since
	// each worker has its own MySQL connection.
	vp2 := *vp
	vp2.foreignKeyChecksStateInitialized = false

	// pendingDone holds the done channel of the most recently sent worker
	// transaction that the commitLoop may still be committing. We capture
	// only the channel (not the *applyTxn) because the commitLoop returns
	// the applyTxn to the pool after signaling done — if the scheduleLoop
	// reacquires it, it drains the channel, which would cause waitPending
	// to block forever if we were still dereferencing through the txn.
	var pendingDone chan struct{}

	waitPending := func() error {
		if pendingDone == nil {
			return nil
		}
		select {
		case <-pendingDone:
			pendingDone = nil
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		txn, err := scheduler.nextReady(ctx)
		if err != nil {
			return err
		}
		payload := txn.payload
		if payload.commitOnly {
			// Must wait for any pending worker commit to complete before
			// forwarding commitOnly txns, since they run on the main
			// connection and strict ordering must be maintained.
			if err := waitPending(); err != nil {
				return err
			}
			select {
			case commitCh <- txn:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}

		// Apply events on the current active connection. This runs
		// concurrently with the commitLoop committing the previous
		// transaction on the other connection (double-buffering).
		for _, event := range payload.events {
			if err := worker.applyEvent(ctx, event, payload.mustSave, &vp2); err != nil {
				worker.rollback()
				return err
			}
		}
		// In batch mode, flush all buffered SQL statements to MySQL in
		// one multi-statement call. This is the key parallelism point:
		// all workers execute their batches concurrently here, while the
		// commitLoop only needs to do a cheap COMMIT + position update.
		if err := worker.flushWorkerBatch(); err != nil {
			worker.rollback()
			return err
		}

		// Wait for the previous transaction's commit to complete. Because
		// we waited AFTER applying the current transaction, the apply and
		// commit phases overlapped — this is the key pipelining benefit.
		// If the commit finished during our apply phase, this returns
		// immediately. We must wait here because rotate() switches to the
		// connection that the commitLoop was using for the previous txn.
		if err := waitPending(); err != nil {
			return err
		}

		// Capture the current connection for the payload before rotating.
		// The commitLoop will use these to commit this transaction while
		// the worker moves on to the next transaction on the spare connection.
		activeClient := worker.client
		if worker.batchMode {
			payload.query = func(ctx context.Context, sql string) (*sqltypes.Result, error) {
				return activeClient.Execute(sql)
			}
		} else {
			payload.query = worker.query
		}
		payload.commit = worker.commit
		payload.client = worker.client

		select {
		case commitCh <- txn:
		case <-ctx.Done():
			worker.rollback()
			return ctx.Err()
		}

		// Capture the done channel BEFORE rotating. The commitLoop may
		// return the txn to the pool after signaling done, and
		// acquireApplyTxn drains the channel on reuse. By holding our
		// own reference, we are immune to that race.
		pendingDone = txn.done

		// Rotate to the spare connection for the next transaction.
		// The commitLoop will commit the current txn on the old connection
		// and signal txn.done when it's safe to reuse.
		worker.rotate()
		// Reset FK check state cache since the new connection may have
		// different session state. Each worker connection starts with
		// foreign_key_checks=0 (from clearFKCheck), but the cache may
		// say it's already set to true from the previous connection.
		vp2.foreignKeyChecksStateInitialized = false
	}
}

// commitLoop receives completed transactions from workers via commitCh and
// commits them in strict order (by the order field). For worker transactions,
// it executes the position update and commit on the worker's connection
// WITHOUT holding serialMu, then briefly locks to update vp state.
// For commitOnly transactions, it applies events on the main connection
// under serialMu.
func (vp *vplayer) commitLoop(ctx context.Context, scheduler *applyScheduler, commitCh <-chan *applyTxn) error {
	updateLag := func(payload *applyTxnPayload) {
		if payload.lastEventTimestamp != 0 {
			tsNs := payload.lastEventTimestamp * 1e9
			vp.lastTimestampNs.Store(tsNs)
			now := time.Now().UnixNano()
			offset := now - payload.lastEventCurrentTime
			vp.timeOffsetNs.Store(offset)
			lag := now - tsNs - offset
			if lag >= 0 {
				lagSecs := lag / 1e9
				vp.vr.stats.ReplicationLagSeconds.Store(lagSecs)
				vp.vr.stats.VReplicationLagGauges.Set(vp.idStr, lagSecs)
				return
			}
		}
		behind := time.Now().UnixNano() - vp.lastTimestampNs.Load() - vp.timeOffsetNs.Load()
		behindSecs := behind / 1e9
		vp.vr.stats.ReplicationLagSeconds.Store(behindSecs)
		vp.vr.stats.VReplicationLagGauges.Set(vp.idStr, behindSecs)
	}

	// commitWorkerTxn handles a worker's row transaction. It executes the
	// position update SQL and commit on the worker's private MySQL connection
	// WITHOUT holding serialMu, then briefly locks to update vp state. This
	// avoids blocking the scheduleLoop during slow MySQL commits.
	commitWorkerTxn := func(txn *applyTxn) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		payload := txn.payload

		// Generate the position update SQL. These reads are all from
		// immutable or atomic fields, no lock needed.
		updateSQL := binlogplayer.GenerateUpdatePos(vp.vr.id, payload.pos,
			time.Now().Unix(), payload.timestamp,
			vp.vr.stats.CopyRowCount.Get(),
			vp.vr.workflowConfig.StoreCompressedGTID)

		// Execute position update within the worker's open transaction,
		// then commit. No serialMu needed — we use the payload's
		// connection directly, never touching vp's fields.
		if _, err := payload.query(ctx, updateSQL); err != nil {
			return fmt.Errorf("error %v updating position", err)
		}
		if err := payload.commit(); err != nil {
			return err
		}

		// Briefly lock to update vp state that scheduleLoop reads.
		vp.serialMu.Lock()
		vp.numAccumulatedHeartbeats = 0
		vp.unsavedEvent = nil
		vp.timeLastSaved = time.Now()
		shouldStop := vp.stopPos.GTIDSet != nil
		posReached := !vp.stopPos.IsZero() && payload.pos.AtLeast(vp.stopPos)
		vp.serialMu.Unlock()

		vp.vr.stats.SetLastPosition(payload.pos)
		updateLag(payload)

		// Signal the worker that commit is done so it can reuse its
		// DB connection for the next transaction.
		txn.done <- struct{}{}

		if err := scheduler.markCommitted(txn); err != nil {
			return err
		}

		// Now that the worker transaction is committed (lock released),
		// set state on the main connection if stop position was reached.
		if shouldStop && posReached {
			if vp.saveStop {
				vp.serialMu.Lock()
				err := vp.vr.setState(binlogdatapb.VReplicationWorkflowState_Stopped, fmt.Sprintf("Stopped at position %v", vp.stopPos))
				vp.serialMu.Unlock()
				if err != nil {
					return err
				}
			}
			return io.EOF
		}
		return nil
	}

	// commitOnlyTxn handles commitOnly transactions (DDL, OTHER, JOURNAL,
	// position-only saves). These run on the main connection under serialMu.
	commitOnlyTxn := func(txn *applyTxn) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		payload := txn.payload
		vp.serialMu.Lock()
		defer vp.serialMu.Unlock()

		shouldStop := vp.stopPos.GTIDSet != nil
		// Temporarily swap pos for the main connection's updatePos call.
		prevPos := vp.pos
		if !payload.pos.IsZero() {
			vp.pos = payload.pos
		}
		defer func() { vp.pos = prevPos }()

		if payload.updatePosOnly {
			posReached, err := vp.updatePos(ctx, payload.timestamp)
			if err != nil {
				return err
			}
			updateLag(payload)
			if err := scheduler.markCommitted(txn); err != nil {
				return err
			}
			if shouldStop && posReached {
				return io.EOF
			}
			return nil
		}
		// applyEvent handles position updates internally for DDL, OTHER,
		// and JOURNAL events, and returns io.EOF when the stop position
		// is reached or when a JOURNAL forces termination. We therefore
		// do NOT call updatePos again below — doing so would produce a
		// redundant _vt.vreplication write and create an awkward
		// partial-failure window where applyEvent succeeded but a second
		// position write could fail.
		if err := vp.applyEvent(ctx, payload.events[0], payload.mustSave); err != nil {
			return err
		}
		// After DDL, refresh FK metadata so that ADD/DROP FOREIGN KEY
		// changes are reflected in subsequent writeset conflict detection.
		// We hold serialMu for the DB round-trip here. DDL is rare, and
		// the main connection must not be used concurrently by scheduleLoop.
		// Fail fast on refresh errors: stale FK topology after a schema
		// change would silently compromise conflict detection.
		if payload.events[0].Type == binlogdatapb.VEventType_DDL {
			newRefs, err := queryFKRefs(vp.vr.dbClient, vp.vr.dbClient.DBName())
			if err != nil {
				return vterrors.Wrapf(err, "failed to refresh FK metadata after DDL")
			}
			vp.fkRefs = newRefs
			vp.parentFKRefs = buildParentFKRefs(newRefs)
		}
		updateLag(payload)
		if err := scheduler.markCommitted(txn); err != nil {
			return err
		}
		return nil
	}

	commitTxn := func(txn *applyTxn) error {
		if txn.payload.commitOnly {
			return commitOnlyTxn(txn)
		}
		return commitWorkerTxn(txn)
	}

	pending := make(map[int64]*applyTxn)
	nextOrder := int64(1)

	// On error exit, signal done and release all remaining pending entries
	// to prevent worker goroutines from blocking on txn.done and to return
	// pool objects that would otherwise be leaked.
	defer func() {
		for _, txn := range pending {
			if txn.payload != nil && !txn.payload.commitOnly {
				select {
				case txn.done <- struct{}{}:
				default:
				}
			}
			releaseApplyTxn(txn)
		}
	}()

	drainPending := func() error {
		for {
			next := pending[nextOrder]
			if next == nil {
				break
			}
			delete(pending, nextOrder)
			if err := commitTxn(next); err != nil {
				// Re-add the failed txn so the defer cleanup can
				// signal its done channel and release it to the pool.
				pending[nextOrder] = next
				return err
			}
			releaseApplyTxn(next)
			nextOrder++
		}
		return nil
	}

	for {
		select {
		case txn, ok := <-commitCh:
			if !ok {
				// The commit channel has been closed so we cannot add anything else.
				// We only need to drain any already pending transactions.
				if err := drainPending(); err != nil {
					return err
				}
				if len(pending) > 0 {
					return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "parallel apply commit missing order: pending=%d next=%d", len(pending), nextOrder)
				}
				return nil
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if txn.order == 0 {
				if err := commitTxn(txn); err != nil {
					return err
				}
				releaseApplyTxn(txn)
				continue
			}
			// Add the new transaction to be committed and then drain all pending ones.
			pending[txn.order] = txn
			if err := drainPending(); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// commitTxn commits a worker's transaction. It executes the position update
// SQL and commit on the worker's connection directly (via payload.query and
// payload.commit), WITHOUT holding serialMu. After commit, it briefly locks
// serialMu to update vp state (unsavedEvent, timeLastSaved).
// To avoid self-deadlock when the stop position is reached, setState is
// called on the main connection AFTER the worker's transaction is committed
// and the row lock is released.
func (vp *vplayer) commitTxn(ctx context.Context, payload *applyTxnPayload) (bool, error) {
	queryFn := payload.query
	commitFn := payload.commit
	if queryFn == nil {
		queryFn = vp.query
	}
	if commitFn == nil {
		commitFn = vp.commit
	}

	pos := payload.pos
	if pos.IsZero() {
		pos = vp.pos
	}

	updateSQL := binlogplayer.GenerateUpdatePos(vp.vr.id, pos,
		time.Now().Unix(), payload.timestamp,
		vp.vr.stats.CopyRowCount.Get(),
		vp.vr.workflowConfig.StoreCompressedGTID)

	if _, err := queryFn(ctx, updateSQL); err != nil {
		return false, fmt.Errorf("error %v updating position", err)
	}
	if err := commitFn(); err != nil {
		return false, err
	}

	vp.numAccumulatedHeartbeats = 0
	vp.unsavedEvent = nil
	vp.timeLastSaved = time.Now()
	vp.vr.stats.SetLastPosition(pos)
	posReached := !vp.stopPos.IsZero() && pos.AtLeast(vp.stopPos)

	if posReached && vp.saveStop {
		if err := vp.vr.setState(binlogdatapb.VReplicationWorkflowState_Stopped, fmt.Sprintf("Stopped at position %v", vp.stopPos)); err != nil {
			return false, err
		}
	}
	return posReached, nil
}
