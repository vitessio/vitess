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
	pos           replication.Position
	timestamp     int64
	mustSave      bool
	events        []*binlogdatapb.VEvent
	rowOnly       bool
	commitOnly    bool
	updatePosOnly bool
	query         func(ctx context.Context, sql string) (*sqltypes.Result, error)
	commit        func() error
	client        *vdbClient
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
	return applyTxnPool.Get().(*applyTxn)
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
	commitCh := make(chan *applyTxn, workerCount)
	applyErr := make(chan error, 2)
	workerErr := make(chan error, workerCount)

	workers := make([]*applyWorker, 0, workerCount)
	for range workerCount {
		worker, err := newApplyWorker(ctx, vp.vr)
		if err != nil {
			return err
		}
		workers = append(workers, worker)
	}
	defer func() {
		for _, worker := range workers {
			// if parallelDebugEnabled() && worker.client != nil && worker.client.InTransaction {
			// 	parallelDebugLog(fmt.Sprintf("CLOSING worker %d with InTransaction=true: stream=%d workflow=%s", i, vp.vr.id, vp.vr.WorkflowName))
			// }
			worker.close()
		}
	}()

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
		// if parallelDebugEnabled() {
		// 	log.Warn(fmt.Sprintf("parallel apply returning applyErr: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, err))
		// 	parallelDebugLog(fmt.Sprintf("applyEventsParallel RETURN applyErr: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, err))
		// }
	default:
		select {
		case err := <-workerErr:
			finalErr = err
			// if parallelDebugEnabled() {
			// 	log.Warn(fmt.Sprintf("parallel apply returning workerErr: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, err))
			// 	parallelDebugLog(fmt.Sprintf("applyEventsParallel RETURN workerErr: stream=%d workflow=%s err=%v", vp.vr.id, vp.vr.WorkflowName, err))
			// }
		default:
			// if parallelDebugEnabled() {
			// 	log.Warn(fmt.Sprintf("parallel apply returning default nil: stream=%d workflow=%s", vp.vr.id, vp.vr.WorkflowName))
			// 	parallelDebugLog(fmt.Sprintf("applyEventsParallel RETURN default nil: stream=%d workflow=%s", vp.vr.id, vp.vr.WorkflowName))
			// }
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
	curEvents            []*binlogdatapb.VEvent
	curRowOnly           bool
	curRowOnlySet        bool
	curTimestamp         int64
	curMustSave          bool
	curPos               replication.Position
	curCommitParent      int64
	curSequence          int64
	curHasCommitMeta     bool
	lastFlushTime        time.Time
	lastHeartbeatRefresh time.Time
	cachedPlanSnapshot   map[string]*TablePlan
	cachedPlanVersion    int64
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
		txn.done = make(chan struct{})
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
			writeset, err := buildTxnWriteset(planSnapshot, vp.fkRefs, state.curEvents)
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
		state.curEvents = nil
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
					state.curEvents = nil
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
				state.curEvents = nil
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
		vp2 := *vp
		vp2.foreignKeyChecksStateInitialized = false
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
		if txn.done != nil {
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
			if txn.done != nil {
				close(txn.done)
			}
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
		if txn.done != nil {
			close(txn.done)
		}
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

	drainPending := func() error {
		for {
			next := pending[nextOrder]
			if next == nil {
				break
			}
			delete(pending, nextOrder)
			if err := commitTxn(next); err != nil {
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
