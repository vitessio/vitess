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
	"math"
	"os"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
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
	// rowOnly is true when the transaction contains only ROW events (no DDL,
	// FIELD, OTHER, or JOURNAL). Row-only transactions can have writesets
	// computed for parallel conflict detection.
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

func acquireApplyTxn() *applyTxn {
	txn := applyTxnPool.Get().(*applyTxn)
	// Reuse the buffered done channel from a previous pool cycle to avoid
	// a make(chan struct{}) heap allocation per transaction.
	if txn.done == nil {
		txn.done = make(chan struct{}, 1)
	}
	return txn
}

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
	// Preserve the buffered done channel across pool cycles to avoid
	// re-allocating it on every acquireApplyTxn call. Drain any pending
	// signal so the channel is ready for reuse.
	doneCh := txn.done
	if doneCh != nil {
		select {
		case <-doneCh:
		default:
		}
	}
	*txn = applyTxn{}
	txn.done = doneCh
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

func parallelDebugEnabled() bool {
	return os.Getenv("VREPLICATION_PARALLEL_DEBUG") == "1"
}

var parallelDebugLogMu sync.Mutex

func parallelDebugLog(msg string) {
	parallelDebugLogMu.Lock()
	defer parallelDebugLogMu.Unlock()
	f, err := os.OpenFile("/tmp/parallel_apply_debug.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s [pid=%d] %s\n", time.Now().Format("15:04:05.000"), os.Getpid(), msg)
}

func (vp *vplayer) applyEventsParallel(ctx context.Context, relay *relayLog) error {
	workerCount := vp.vr.workflowConfig.ParallelReplicationWorkers
	// parallelDebugLog(fmt.Sprintf("applyEventsParallel ENTRY: stream=%d workflow=%s workers=%d copy_state=%d", vp.vr.id, vp.vr.WorkflowName, workerCount, len(vp.copyState)))
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
			// if parallelDebugEnabled() && worker.client != nil && worker.client.InTransaction {
			// 	parallelDebugLog(fmt.Sprintf("CLOSING worker %d with InTransaction=true: stream=%d workflow=%s", i, vp.vr.id, vp.vr.WorkflowName))
			// }
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
		fkRefs = nil
	}
	vp.fkRefs = fkRefs

	var wg sync.WaitGroup
	for i := range workerCount {
		worker := workers[i]
		// workerIdx := i
		wg.Go(func() {
			err := vp.workerLoop(ctx, scheduler, commitCh, worker)
			// if parallelDebugEnabled() {
			// 	log.Warn(fmt.Sprintf("parallel apply worker exited: stream=%d workflow=%s worker=%d err=%v", vp.vr.id, vp.vr.WorkflowName, workerIdx, err))
			// }
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
			// if parallelDebugEnabled() {
			// 	log.Warn(fmt.Sprintf("parallel apply commitLoop returned err: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, err))
			// }
			applyErr <- err
			// Always cancel context when commitLoop exits with an error,
			// including io.EOF (stop position reached). This ensures
			// scheduleLoop and workers shut down promptly instead of
			// blocking on a commitCh that has no reader.
			cancel()
		}
		// else if parallelDebugEnabled() {
		// 	log.Warn(fmt.Sprintf("parallel apply commitLoop returned nil: stream=%d workflow=%s", vp.vr.id, vp.vr.WorkflowName))
		// }
	}()

	schedErr := vp.scheduleLoop(ctx, relay, scheduler)
	// if parallelDebugEnabled() {
	// 	log.Warn(fmt.Sprintf("parallel apply scheduleLoop returned: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, schedErr))
	// 	parallelDebugLog(fmt.Sprintf("scheduleLoop RETURNED: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, schedErr))
	// }
	if schedErr != nil {
		applyErr <- schedErr
		if schedErr != io.EOF {
			cancel()
		}
	}

	// if parallelDebugEnabled() {
	// 	log.Warn(fmt.Sprintf("parallel apply closing scheduler: stream=%d workflow=%s", vp.vr.id, vp.vr.WorkflowName))
	// }
	scheduler.close()
	// if parallelDebugEnabled() {
	// 	log.Warn(fmt.Sprintf("parallel apply waiting for workers: stream=%d workflow=%s", vp.vr.id, vp.vr.WorkflowName))
	// }
	wg.Wait()
	// if parallelDebugEnabled() {
	// 	log.Warn(fmt.Sprintf("parallel apply closing commitCh: stream=%d workflow=%s", vp.vr.id, vp.vr.WorkflowName))
	// }
	close(commitCh)
	<-commitDone

	// Determine the final error. Priority: applyErr (from commitLoop/scheduleLoop)
	// over workerErr, since commitLoop's io.EOF is the authoritative "stop position
	// reached" signal. Workers typically exit with context.Canceled as a consequence.
	var finalErr error
	select {
	case err := <-applyErr:
		finalErr = err
	default:
		// Drain all worker errors and join them so that no diagnostic
		// information is silently dropped. The first error triggers
		// cancel() so subsequent workers usually exit with
		// context.Canceled, but collecting all errors helps debugging
		// cases where multiple workers hit independent failures.
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

func (vp *vplayer) scheduleLoop(ctx context.Context, relay *relayLog, scheduler *applyScheduler) error {
	defer vp.vr.dbClient.Rollback()
	// lastFetchPos := ""
	// fetchCount := 0
	state := &parallelScheduleState{}
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
		// if parallelDebugEnabled() && len(items) > 0 {
		// 	fetchCount++
		// 	firstPos := getNextPosition(items, 0, 0)
		// 	if firstPos != "" && firstPos == lastFetchPos {
		// 		log.Warn(fmt.Sprintf("parallel apply fetched duplicate pos: stream=%d workflow=%s fetch=%d pos=%s", vp.vr.id, vp.vr.WorkflowName, fetchCount, firstPos))
		// 	} else {
		// 		log.Warn(fmt.Sprintf("parallel apply fetched items: stream=%d workflow=%s fetch=%d batches=%d pos=%s", vp.vr.id, vp.vr.WorkflowName, fetchCount, len(items), firstPos))
		// 	}
		// 	if firstPos != "" {
		// 		lastFetchPos = firstPos
		// 	}
		// }
		if err := vp.scheduleItems(ctx, scheduler, state, items); err != nil {
			return err
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
}

func (vp *vplayer) scheduleItems(ctx context.Context, scheduler *applyScheduler, state *parallelScheduleState, items [][]*binlogdatapb.VEvent) error {
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
		// if parallelDebugEnabled() {
		// 	parallelDebugLog(fmt.Sprintf("FLUSH txn: stream=%d workflow=%s order=%d events=%d rowOnly=%t commitOnly=%t hasMeta=%t seq=%d parent=%d pos=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, len(payload.events), payload.rowOnly, payload.commitOnly, txn.hasCommitMeta, txn.sequenceNumber, txn.commitParent, payload.pos))
		// }
		// if parallelDebugEnabled() && len(payload.events) > 0 {
		// 	for _, event := range payload.events {
		// 		if event.Type == binlogdatapb.VEventType_ROW && event.RowEvent != nil {
		// 			parallelDebugLog(fmt.Sprintf("FLUSH rowevent: stream=%d workflow=%s order=%d table=%s row_changes=%d", vp.vr.id, vp.vr.WorkflowName, txn.order, event.RowEvent.TableName, len(event.RowEvent.RowChanges)))
		// 		}
		// 	}
		// }
		if state.curRowOnlySet && !state.curRowOnly {
			txn.forceGlobal = true
		} else if len(vp.copyState) != 0 {
			txn.forceGlobal = true
		} else {
			planSnapshot := snapshotTablePlans(vp.tablePlansMu, vp.tablePlans, &vp.tablePlansVersion, &state.cachedPlanVersion, state.cachedPlanSnapshot)
			state.cachedPlanSnapshot = planSnapshot
			// Invalidate fieldIdxCache when table plans change (new FIELD events).
			if state.fieldIdxCacheVersion != state.cachedPlanVersion {
				state.fieldIdxCache = make(map[string]map[string]int)
				state.fieldIdxCacheVersion = state.cachedPlanVersion
			}
			// if parallelDebugEnabled() {
			// 	for _, ev := range state.curEvents {
			// 		if ev.Type == binlogdatapb.VEventType_ROW && ev.RowEvent != nil {
			// 			plan := planSnapshot[ev.RowEvent.TableName]
			// 			if plan == nil {
			// 				parallelDebugLog(fmt.Sprintf("FLUSH plan MISSING for table=%s order=%d", ev.RowEvent.TableName, order))
			// 			} else {
			// 				parallelDebugLog(fmt.Sprintf("FLUSH plan for table=%s order=%d: PKIndices=%v Fields=%d", ev.RowEvent.TableName, order, plan.PKIndices, len(plan.Fields)))
			// 			}
			// 		}
			// 	}
			// }
			writeset, err := buildTxnWriteset(planSnapshot, vp.fkRefs, state.curEvents, state.fieldIdxCache)
			if err != nil {
				// Table plan may not be populated yet (FIELD event not yet applied).
				// Treat as forceGlobal to serialize safely.
				txn.forceGlobal = true
				// if parallelDebugEnabled() {
				// 	parallelDebugLog(fmt.Sprintf("FLUSH writeset error, forcing global: stream=%d workflow=%s order=%d err=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, err))
				// }
			} else {
				txn.writeset = writeset
			}
		}
		// if parallelDebugEnabled() {
		// 	parallelDebugLog(fmt.Sprintf("FLUSH writeset: stream=%d workflow=%s order=%d forceGlobal=%t writeset=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, txn.forceGlobal, txn.writeset))
		// }
		if err := scheduler.enqueue(txn); err != nil {
			return err
		}
		// Pre-allocate with capacity 16 to avoid the nil→1→2→4→8 growth
		// pattern on the hot path. We can't reuse the old slice via [:0]
		// because the payload still references the backing array.
		state.curEvents = make([]*binlogdatapb.VEvent, 0, 16)
		state.curRowOnly = false
		state.curRowOnlySet = false
		state.curMustSave = false
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
				// if parallelDebugEnabled() {
				// 	parallelDebugLog(fmt.Sprintf("scheduleItems GTID: stream=%d workflow=%s seq=%d parent=%d pos=%v", vp.vr.id, vp.vr.WorkflowName, state.curSequence, state.curCommitParent, state.curPos))
				// }
				vp.serialMu.Lock()
				vp.pos = pos
				vp.unsavedEvent = nil
				vp.serialMu.Unlock()
			case binlogdatapb.VEventType_ROW:
				// if parallelDebugEnabled() && event.RowEvent != nil {
				// 	parallelDebugLog(fmt.Sprintf("scheduleItems ROW: stream=%d workflow=%s table=%s changes=%d pos=%v", vp.vr.id, vp.vr.WorkflowName, event.RowEvent.TableName, len(event.RowEvent.RowChanges), state.curPos))
				// }
				state.curEvents = append(state.curEvents, event)
				if !state.curRowOnlySet {
					state.curRowOnly = true
					state.curRowOnlySet = true
				}
			case binlogdatapb.VEventType_COMMIT:
				// if parallelDebugEnabled() {
				// 	parallelDebugLog(fmt.Sprintf("scheduleItems COMMIT: stream=%d workflow=%s pos=%v curEvents=%d", vp.vr.id, vp.vr.WorkflowName, state.curPos, len(state.curEvents)))
				// }
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
							// if parallelDebugEnabled() {
							// 	parallelDebugLog(fmt.Sprintf("scheduleItems EMPTY TXN time_updated refresh: stream=%d workflow=%s",
							// 		vp.vr.id, vp.vr.WorkflowName))
							// }
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
					// if parallelDebugEnabled() {
					// 	parallelDebugLog(fmt.Sprintf("scheduleItems BATCH TIME BOUND: stream=%d workflow=%s elapsed=%v — forcing flush",
					// 		vp.vr.id, vp.vr.WorkflowName, time.Since(state.lastFlushTime)))
					// }
				}
				// When FK refs are present, skip batching to keep writesets
				// small. Large batches merge many parent/child operations
				// into a single writeset, causing nearly all batches to
				// conflict on FK ref keys and serializing the workload.
				// Flushing each source transaction individually lets the
				// scheduler detect truly independent transactions and run
				// them in parallel.
				hasFKRefs := len(vp.fkRefs) > 0
				if !state.curMustSave && !hasFKRefs && hasAnotherCommit(items, i, j+1) {
					// if parallelDebugEnabled() {
					// 	parallelDebugLog(fmt.Sprintf("scheduleItems BATCH: stream=%d workflow=%s seq=%d parent=%d curEvents=%d — skipping flush, another commit ahead",
					// 		vp.vr.id, vp.vr.WorkflowName, state.curSequence, state.curCommitParent, len(state.curEvents)))
					// }
					// Advance lastCommittedSequence for this merged-away
					// transaction so that future hasCommitMeta transactions
					// whose commitParent references this sequence are not
					// blocked. The events will be flushed with the final
					// transaction in this batch.
					if state.curHasCommitMeta {
						scheduler.advanceCommittedSequence(state.curSequence)
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
				state.curEvents = make([]*binlogdatapb.VEvent, 0, 16)
				state.curRowOnly = false
				state.curRowOnlySet = false
				state.curMustSave = false
				state.curTimestamp = 0
				state.curCommitParent = 0
				state.curSequence = 0
				state.curHasCommitMeta = false
			case binlogdatapb.VEventType_BEGIN:
				// No-op: BEGIN is handled on-demand by workers when they encounter
				// ROW/FIELD events (via activeDBClient().Begin()). We intentionally
				// do NOT add BEGIN to curEvents so that empty transactions
				// (GTID→BEGIN→COMMIT) have curEvents=0 and take the fast path
				// (unsavedEvent) instead of being enqueued through the scheduler.
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
				vp.numAccumulatedHeartbeats++
				vp.serialMu.Lock()
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
				// if parallelDebugEnabled() {
				// 	parallelDebugLog(fmt.Sprintf("scheduleItems DEFAULT: stream=%d workflow=%s type=%v pos=%v", vp.vr.id, vp.vr.WorkflowName, event.Type, state.curPos))
				// }
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
	// if parallelDebugEnabled() {
	// 	log.Warn(fmt.Sprintf("parallel apply schedule commit-only: stream=%d workflow=%s order=%d type=%v hasMeta=%t updatePosOnly=%t noConflict=%t pos=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, event.Type, txn.hasCommitMeta, payload.updatePosOnly, txn.noConflict, payload.pos))
	// }
	return scheduler.enqueue(txn)
}

func (vp *vplayer) workerLoop(ctx context.Context, scheduler *applyScheduler, commitCh chan<- *applyTxn, worker *applyWorker) error {
	// Shallow-copy vplayer once per worker lifetime instead of per
	// transaction. This eliminates a ~200-300 byte struct copy on every
	// txn in the hot path. The FK check state is reset once here since
	// each worker has its own MySQL connection.
	vp2 := *vp
	vp2.foreignKeyChecksStateInitialized = false
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// if parallelDebugEnabled() {
		// 	parallelDebugLog(fmt.Sprintf("workerLoop WAITING nextReady: stream=%d workflow=%s worker=%p", vp.vr.id, vp.vr.WorkflowName, worker))
		// }
		txn, err := scheduler.nextReady(ctx)
		if err != nil {
			// if parallelDebugEnabled() {
			// 	parallelDebugLog(fmt.Sprintf("workerLoop nextReady ERROR: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, err))
			// }
			return err
		}
		payload := txn.payload
		// if parallelDebugEnabled() {
		// 	parallelDebugLog(fmt.Sprintf("workerLoop GOT TXN: stream=%d workflow=%s order=%d events=%d commitOnly=%t forceGlobal=%t pos=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, len(payload.events), payload.commitOnly, txn.forceGlobal, payload.pos))
		// }
		if payload.commitOnly {
			// if parallelDebugEnabled() {
			// 	log.Warn(fmt.Sprintf("parallel apply worker commit-only: stream=%d workflow=%s order=%d events=%d pos=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, len(payload.events), payload.pos))
			// }
			select {
			case commitCh <- txn:
			case <-ctx.Done():
				return ctx.Err()
			}
			continue
		}
		// if parallelDebugEnabled() {
		// 	log.Warn(fmt.Sprintf("parallel apply worker apply: stream=%d workflow=%s order=%d events=%d pos=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, len(payload.events), payload.pos))
		// 	for _, event := range payload.events {
		// 		if event.Type == binlogdatapb.VEventType_ROW && event.RowEvent != nil {
		// 			parallelDebugLog(fmt.Sprintf("WORKER ROW event: stream=%d workflow=%s order=%d table=%s row_changes=%d", vp.vr.id, vp.vr.WorkflowName, txn.order, event.RowEvent.TableName, len(event.RowEvent.RowChanges)))
		// 		} else {
		// 			parallelDebugLog(fmt.Sprintf("WORKER event: stream=%d workflow=%s order=%d type=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, event.Type))
		// 		}
		// 	}
		// }
		for _, event := range payload.events {
			if err := worker.applyEvent(ctx, event, payload.mustSave, &vp2); err != nil {
				worker.rollback()
				return err
			}
		}
		// if parallelDebugEnabled() {
		// 	parallelDebugLog(fmt.Sprintf("workerLoop SENDING to commitCh: stream=%d workflow=%s order=%d pos=%v", vp.vr.id, vp.vr.WorkflowName, txn.order, payload.pos))
		// }
		payload.query = worker.query
		payload.commit = worker.commit
		payload.client = worker.client
		select {
		case commitCh <- txn:
		case <-ctx.Done():
			return ctx.Err()
		}
		// Wait for the commitLoop to finish committing this txn before
		// reusing the worker's DB connection. commitOnly txns are handled
		// directly by commitLoop and don't need this handshake.
		if !payload.commitOnly {
			select {
			case <-txn.done:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (vp *vplayer) commitLoop(ctx context.Context, scheduler *applyScheduler, commitCh <-chan *applyTxn) error {
	commitTxn := func(txn *applyTxn) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		payload := txn.payload
		updateLag := func() {
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
		vp.serialMu.Lock()
		prevQuery := vp.query
		prevCommit := vp.commit
		prevClient := vp.dbClient
		prevPos := vp.pos
		if payload.query != nil {
			vp.query = payload.query
		}
		if payload.commit != nil {
			vp.commit = payload.commit
		}
		if payload.client != nil {
			vp.dbClient = payload.client
		}
		if !payload.pos.IsZero() {
			vp.pos = payload.pos
		}
		defer func() {
			vp.query = prevQuery
			vp.commit = prevCommit
			vp.dbClient = prevClient
			vp.pos = prevPos
			vp.serialMu.Unlock()
		}()
		if payload.commitOnly {
			shouldStop := vp.stopPos.GTIDSet != nil
			if payload.updatePosOnly {
				posReached, err := vp.updatePos(ctx, payload.timestamp)
				if err != nil {
					return err
				}
				if shouldStop && posReached {
					return io.EOF
				}
			} else {
				if err := vp.applyEvent(ctx, payload.events[0], payload.mustSave); err != nil {
					return err
				}
				if payload.events[0].Type == binlogdatapb.VEventType_HEARTBEAT {
					vp.numAccumulatedHeartbeats++
					if err := vp.recordHeartbeat(); err != nil {
						return err
					}
				}
				posReached, err := vp.updatePos(ctx, payload.timestamp)
				if err != nil {
					return err
				}
				if shouldStop && posReached {
					return io.EOF
				}
			}
			updateLag()
			if err := scheduler.markCommitted(txn); err != nil {
				return err
			}
			return nil
		}
		shouldStop := vp.stopPos.GTIDSet != nil
		posReached, err := vp.commitTxn(ctx, payload)
		if err != nil {
			return err
		}
		updateLag()
		// Non-commitOnly: signal the worker that commit is done so it
		// can reuse its DB connection for the next transaction.
		txn.done <- struct{}{}
		if err := scheduler.markCommitted(txn); err != nil {
			return err
		}
		if shouldStop && posReached {
			// if parallelDebugEnabled() {
			// 	parallelDebugLog(fmt.Sprintf("commitLoop EOF: commitOnly=false stream=%d workflow=%s stopPos=%v pos=%v", vp.vr.id, vp.vr.WorkflowName, vp.stopPos, vp.pos))
			// }
			return io.EOF
		}
		return nil
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
				if err := drainPending(); err != nil {
					return err
				}
				if len(pending) > 0 {
					return fmt.Errorf("parallel apply commit missing order: pending=%d next=%d", len(pending), nextOrder)
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
			pending[txn.order] = txn
			if err := drainPending(); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (vp *vplayer) commitTxn(ctx context.Context, payload *applyTxnPayload) (bool, error) {
	// if parallelDebugEnabled() {
	// 	hasCustomer := false
	// 	for _, event := range payload.events {
	// 		if event.Type == binlogdatapb.VEventType_ROW && event.RowEvent != nil && event.RowEvent.TableName == "customer" {
	// 			hasCustomer = true
	// 			break
	// 		}
	// 	}
	// 	if hasCustomer {
	// 		parallelDebugLog(fmt.Sprintf("COMMIT customer: stream=%d workflow=%s pos=%v events=%d inTxn=%v", vp.vr.id, vp.vr.WorkflowName, payload.pos, len(payload.events), vp.activeDBClient().InTransaction))
	// 	}
	// }
	// For worker transactions, updatePos acquires a row lock on _vt.vreplication
	// within the worker's open transaction. If posReached, setState would try to
	// UPDATE the same row on the MAIN connection, causing a self-deadlock.
	// To avoid this: temporarily disable saveStop so updatePos skips setState,
	// commit the worker transaction (releasing the row lock), then call setState
	// on the main connection.
	isWorkerTxn := payload.client != nil && payload.client != vp.vr.dbClient
	origSaveStop := vp.saveStop
	if isWorkerTxn {
		vp.saveStop = false
	}
	posReached, err := vp.updatePos(ctx, payload.timestamp)
	if isWorkerTxn {
		vp.saveStop = origSaveStop
	}
	if err != nil {
		return false, err
	}
	if err := vp.commit(); err != nil {
		return false, err
	}
	// Now that the worker transaction is committed (lock released), set state
	// on the main connection if the stop position was reached.
	if posReached && origSaveStop {
		if err := vp.vr.setState(binlogdatapb.VReplicationWorkflowState_Stopped, fmt.Sprintf("Stopped at position %v", vp.stopPos)); err != nil {
			return false, err
		}
	}
	// if parallelDebugEnabled() {
	// 	hasCustomer := false
	// 	for _, event := range payload.events {
	// 		if event.Type == binlogdatapb.VEventType_ROW && event.RowEvent != nil && event.RowEvent.TableName == "customer" {
	// 			hasCustomer = true
	// 			break
	// 		}
	// 	}
	// 	if hasCustomer {
	// 		parallelDebugLog(fmt.Sprintf("COMMIT customer DONE: stream=%d workflow=%s pos=%v", vp.vr.id, vp.vr.WorkflowName, payload.pos))
	// 		// Verification: read back dec80 to confirm data is persisted
	// 		if payload.client != nil {
	// 			qr, verErr := payload.client.ExecuteFetch("select cid, dec80 from customer order by cid", 100)
	// 			if verErr != nil {
	// 				parallelDebugLog(fmt.Sprintf("VERIFY customer FAILED: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, verErr))
	// 			} else {
	// 				parallelDebugLog(fmt.Sprintf("VERIFY customer AFTER COMMIT: stream=%d workflow=%s rows=%v", vp.vr.id, vp.vr.WorkflowName, qr.Rows))
	// 			}
	// 		}
	// 	}
	// }
	return posReached, nil
}
