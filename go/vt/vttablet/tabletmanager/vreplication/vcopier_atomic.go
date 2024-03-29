/*
Copyright 2023 The Vitess Authors.

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
	"fmt"
	"io"
	"strconv"
	"time"

	"vitess.io/vitess/go/vt/vttablet"

	"google.golang.org/protobuf/encoding/prototext"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

/*
This file is similar to vcopier.go: it handles the copy phase for the AtomicCopy where all tables
are streamed in a single phase.
*/

type copyAllState struct {
	vc               *vcopier
	plan             *ReplicatorPlan
	currentTableName string
	tables           map[string]bool
}

// newCopyAllState creates the required table plans and sets up the copy state for all tables in the source.
func newCopyAllState(vc *vcopier) (*copyAllState, error) {
	state := &copyAllState{
		vc: vc,
	}
	plan, err := buildReplicatorPlan(vc.vr.source, vc.vr.colInfoMap, nil, vc.vr.stats, vc.vr.vre.env.CollationEnv(), vc.vr.vre.env.Parser())
	if err != nil {
		return nil, err
	}
	state.plan = plan
	state.tables = make(map[string]bool, len(plan.TargetTables))
	for _, table := range plan.TargetTables {
		state.tables[table.TargetName] = false
	}
	return state, nil
}

// copyAll copies all tables from the source to the target sequentially, finishing one table first and then moving to the next..
func (vc *vcopier) copyAll(ctx context.Context, settings binlogplayer.VRSettings) error {
	var err error

	log.Infof("Starting copyAll for %s", settings.WorkflowName)
	defer log.Infof("Returning from copyAll for %s", settings.WorkflowName)
	defer vc.vr.dbClient.Rollback()

	state, err := newCopyAllState(vc)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, vttablet.CopyPhaseDuration)
	defer cancel()

	rowsCopiedTicker := time.NewTicker(rowsCopiedUpdateInterval)
	defer rowsCopiedTicker.Stop()

	parallelism := getInsertParallelism()
	copyWorkerFactory := vc.newCopyWorkerFactory(parallelism)
	var copyWorkQueue *vcopierCopyWorkQueue

	// Allocate a result channel to collect results from tasks. To not block fast workers, we allocate a buffer of
	// MaxResultsInFlight results per worker.
	const MaxResultsInFlight = 4
	resultCh := make(chan *vcopierCopyTaskResult, parallelism*MaxResultsInFlight)
	defer close(resultCh)

	var lastpk *querypb.Row
	var pkfields []*querypb.Field
	var lastpkbv map[string]*querypb.BindVariable
	// Use this for task sequencing.
	var prevCh <-chan *vcopierCopyTaskResult
	var gtid string

	serr := vc.vr.sourceVStreamer.VStreamTables(ctx, func(resp *binlogdatapb.VStreamTablesResponse) error {
		defer vc.vr.stats.PhaseTimings.Record("copy", time.Now())
		defer vc.vr.stats.CopyLoopCount.Add(1)
		log.Infof("VStreamTablesResponse: received table %s, #fields %d, #rows %d, gtid %s, lastpk %+v",
			resp.TableName, len(resp.Fields), len(resp.Rows), resp.Gtid, resp.Lastpk)
		tableName := resp.TableName
		gtid = resp.Gtid

		updateRowsCopied := func() error {
			updateRowsQuery := binlogplayer.GenerateUpdateRowsCopied(vc.vr.id, vc.vr.stats.CopyRowCount.Get())
			_, err := vc.vr.dbClient.Execute(updateRowsQuery)
			return err
		}

		if err := updateRowsCopied(); err != nil {
			return err
		}
		select {
		case <-rowsCopiedTicker.C:
			if err := updateRowsCopied(); err != nil {
				return err
			}
		case <-ctx.Done():
			return io.EOF
		default:
		}
		if tableName != state.currentTableName {
			if copyWorkQueue != nil {
				copyWorkQueue.close()
			}
			copyWorkQueue = vc.newCopyWorkQueue(parallelism, copyWorkerFactory)
			if state.currentTableName != "" {
				log.Infof("copy of table %s is done at lastpk %+v", state.currentTableName, lastpkbv)
				if err := vc.deleteCopyState(state.currentTableName); err != nil {
					return err
				}
			} else {
				log.Infof("starting copy phase with table %s", tableName)
			}

			state.currentTableName = tableName
		}

		// A new copy queue is created for each table. The queue is closed when the table is done.
		if !copyWorkQueue.isOpen {
			if len(resp.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", resp)
			}

			lastpk = nil
			// pkfields are only used for logging, so that we can monitor progress.
			pkfields = make([]*querypb.Field, len(resp.Pkfields))
			for _, f := range resp.Pkfields {
				pkfields = append(pkfields, f.CloneVT())
			}

			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: tableName,
			}
			for _, f := range resp.Fields {
				fieldEvent.Fields = append(fieldEvent.Fields, f.CloneVT())
			}
			tablePlan, err := state.plan.buildExecutionPlan(fieldEvent)
			if err != nil {
				return err
			}

			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf(
				"insert into _vt.copy_state (lastpk, vrepl_id, table_name) values (%a, %s, %s)", ":lastpk",
				strconv.Itoa(int(vc.vr.id)),
				encodeString(tableName))
			addLatestCopyState := buf.ParsedQuery()
			copyWorkQueue.open(addLatestCopyState, pkfields, tablePlan)
		}
		// When rowstreamer has finished streaming all rows, we get a callback with empty rows.
		if len(resp.Rows) == 0 {
			return nil
		}
		// Get the last committed pk into a loggable form.
		lastpkbuf, merr := prototext.Marshal(&querypb.QueryResult{
			Fields: pkfields,
			Rows:   []*querypb.Row{lastpk},
		})

		if merr != nil {
			return fmt.Errorf("failed to marshal pk fields and value into query result: %s", merr.Error())
		}
		lastpkbv = map[string]*querypb.BindVariable{
			"lastpk": {
				Type:  sqltypes.VarBinary,
				Value: lastpkbuf,
			},
		}
		log.Infof("copying table %s with lastpk %v", tableName, lastpkbv)
		// Prepare a vcopierCopyTask for the current batch of work.
		currCh := make(chan *vcopierCopyTaskResult, 1)
		currT := newVCopierCopyTask(newVCopierCopyTaskArgs(resp.Rows, resp.Lastpk))

		// Send result to the global resultCh and currCh. resultCh is used by
		// the loop to return results to VStreamRows. currCh will be used to
		// sequence the start of the nextT.
		currT.lifecycle.onResult().sendTo(currCh)
		currT.lifecycle.onResult().sendTo(resultCh)

		// Use prevCh to Sequence the prevT with the currT so that:
		// * The prevT is completed before we begin updating
		//   _vt.copy_state for currT.
		// * If prevT fails or is canceled, the current task is
		//   canceled.
		// prevCh is nil only for the first task in the vcopier run.
		if prevCh != nil {
			// prevT publishes to prevCh, and currT is the only thing that can
			// consume from prevCh. If prevT is already done, then prevCh will
			// have a value in it. If prevT isn't yet done, then prevCh will
			// have a value later. Either way, AwaitCompletion should
			// eventually get a value, unless there is a context expiry.
			currT.lifecycle.before(vcopierCopyTaskInsertCopyState).awaitCompletion(prevCh)
		}

		// Store currCh in prevCh. The nextT will use this for sequencing.
		prevCh = currCh

		// Update stats after task is done.
		currT.lifecycle.onResult().do(func(_ context.Context, result *vcopierCopyTaskResult) {
			if result.state == vcopierCopyTaskFail {
				vc.vr.stats.ErrorCounts.Add([]string{"Copy"}, 1)
			}
			if result.state == vcopierCopyTaskComplete {
				vc.vr.stats.CopyRowCount.Add(int64(len(result.args.rows)))
				vc.vr.stats.QueryCount.Add("copy", 1)
				vc.vr.stats.TableCopyRowCounts.Add(tableName, int64(len(result.args.rows)))
				vc.vr.stats.TableCopyTimings.Add(tableName, time.Since(result.startedAt))
			}
		})

		if err := copyWorkQueue.enqueue(ctx, currT); err != nil {
			log.Warningf("failed to enqueue task in workflow %s: %s", vc.vr.WorkflowName, err.Error())
			return err
		}

		// When async execution is not enabled, a done task will be available
		// in the resultCh after each Enqueue, unless there was a queue state
		// error (e.g. couldn't obtain a worker from pool).
		//
		// When async execution is enabled, results will show up in the channel
		// eventually, possibly in a subsequent VStreamRows loop. It's still
		// a good idea to check this channel on every pass so that:
		//
		// * resultCh doesn't fill up. If it does fill up then tasks won't be
		//   able to add their results to the channel, and progress in this
		//   goroutine will be blocked.
		// * We keep lastpk up-to-date.
		select {
		case result := <-resultCh:
			if result != nil {
				switch result.state {
				case vcopierCopyTaskCancel:
					log.Warningf("task was canceled in workflow %s: %v", vc.vr.WorkflowName, result.err)
					return io.EOF
				case vcopierCopyTaskComplete:
					// Collect lastpk. Needed for logging at the end.
					lastpk = result.args.lastpk
				case vcopierCopyTaskFail:
					return vterrors.Wrapf(result.err, "task error")
				}
			} else {
				return io.EOF
			}
		default:
		}
		return nil
	})
	if serr != nil {
		log.Infof("VStreamTables failed: %v", serr)
		return serr
	}
	// A context expiration was probably caused by a PlannedReparentShard or an
	// elapsed copy phase duration. CopyAll is not resilient to these events.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped", state.currentTableName)
		return fmt.Errorf("CopyAll was interrupted due to context expiration")
	default:
		if err := vc.deleteCopyState(state.currentTableName); err != nil {
			return err
		}
		if copyWorkQueue != nil {
			copyWorkQueue.close()
		}
		if err := vc.updatePos(ctx, gtid); err != nil {
			return err
		}
		log.Infof("Completed copy of all tables")
	}
	return nil
}

// deleteCopyState deletes the copy state entry for a table, signifying that the copy phase is complete for that table.
func (vc *vcopier) deleteCopyState(tableName string) error {
	log.Infof("Deleting copy state for table %s", tableName)
	delQuery := fmt.Sprintf("delete from _vt.copy_state where table_name=%s and vrepl_id = %d", encodeString(tableName), vc.vr.id)
	if _, err := vc.vr.dbClient.Execute(delQuery); err != nil {
		return err
	}
	return nil
}
