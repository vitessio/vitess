/*
Copyright 2019 The Vitess Authors.

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
	"math"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/bytes2"
	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/pools"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type vcopier struct {
	vr               *vreplicator
	throttlerAppName string
}

// vcopierCopyTask stores the args and lifecycle hooks of a copy task.
type vcopierCopyTask struct {
	args      *vcopierCopyTaskArgs
	lifecycle *vcopierCopyTaskLifecycle
}

// vcopierCopyTaskArgs stores the input of a copy task.
type vcopierCopyTaskArgs struct {
	lastpk *querypb.Row
	rows   []*querypb.Row
}

// vcopierCopyTaskHooks contains callback functions to be triggered as a copy
// task progresses through in-progress phases of its lifecycle.
type vcopierCopyTaskHooks struct {
	fns []func(context.Context, *vcopierCopyTaskArgs) error
}

// vcopierCopyTaskLifecycle can be used to inject additional behaviors into the
// vcopierCopyTask execution.
//
// It contains two types of hooks. In-progress hooks (simply called "hooks")
// which can be registered before or after various phases of the copy task,
// such as "insert row", "commit", etc. Result hooks are used to register
// callbacks to be triggered when a task is "done" (= canceled, completed,
// failed).
type vcopierCopyTaskLifecycle struct {
	hooks       map[string]*vcopierCopyTaskHooks
	resultHooks *vcopierCopyTaskResultHooks
}

// vcopierCopyTaskResult contains information about a task that is done.
type vcopierCopyTaskResult struct {
	args      *vcopierCopyTaskArgs
	err       error
	startedAt time.Time
	state     vcopierCopyTaskState
}

// vcopierCopyTaskHooks contains callback functions to be triggered when a copy
// reaches a "done" state (= canceled, completed, failed).
type vcopierCopyTaskResultHooks struct {
	fns []func(context.Context, *vcopierCopyTaskResult)
}

// vcopierCopyTaskState marks the states and sub-states that a copy task goes
// through.
//
//  1. Pending
//  2. Begin
//  3. Insert rows
//  4. Insert copy state
//  5. Commit
//  6. One of:
//     - Complete
//     - Cancel
//     - Fail
type vcopierCopyTaskState int

const (
	vcopierCopyTaskPending vcopierCopyTaskState = iota
	vcopierCopyTaskBegin
	vcopierCopyTaskInsertRows
	vcopierCopyTaskInsertCopyState
	vcopierCopyTaskCommit
	vcopierCopyTaskCancel
	vcopierCopyTaskComplete
	vcopierCopyTaskFail
)

// vcopierCopyWorkQueue accepts tasks via Enqueue, and distributes those tasks
// concurrently (or synchronously) to internal workers.
type vcopierCopyWorkQueue struct {
	concurrent    bool
	isOpen        bool
	maxDepth      int
	workerFactory func(context.Context) (*vcopierCopyWorker, error)
	workerPool    *pools.ResourcePool
}

// vcopierCopyWorker will Execute a single task at a time in the calling
// goroutine.
type vcopierCopyWorker struct {
	*vdbClient
	closeDbClient   bool
	copyStateInsert *sqlparser.ParsedQuery
	isOpen          bool
	pkfields        []*querypb.Field
	sqlbuffer       bytes2.Buffer
	tablePlan       *TablePlan
}

func newVCopier(vr *vreplicator) *vcopier {
	return &vcopier{
		vr:               vr,
		throttlerAppName: vr.throttlerAppName(),
	}
}

func newVCopierCopyTask(args *vcopierCopyTaskArgs) *vcopierCopyTask {
	return &vcopierCopyTask{
		args:      args,
		lifecycle: newVCopierCopyTaskLifecycle(),
	}
}

func newVCopierCopyTaskArgs(rows []*querypb.Row, lastpk *querypb.Row) *vcopierCopyTaskArgs {
	return &vcopierCopyTaskArgs{
		rows:   rows,
		lastpk: lastpk,
	}
}

func newVCopierCopyTaskHooks() *vcopierCopyTaskHooks {
	return &vcopierCopyTaskHooks{
		fns: make([]func(context.Context, *vcopierCopyTaskArgs) error, 0),
	}
}

func newVCopierCopyTaskLifecycle() *vcopierCopyTaskLifecycle {
	return &vcopierCopyTaskLifecycle{
		hooks:       make(map[string]*vcopierCopyTaskHooks),
		resultHooks: newVCopierCopyTaskResultHooks(),
	}
}

func newVCopierCopyTaskResult(args *vcopierCopyTaskArgs, startedAt time.Time, state vcopierCopyTaskState, err error) *vcopierCopyTaskResult {
	return &vcopierCopyTaskResult{
		args:      args,
		err:       err,
		startedAt: startedAt,
		state:     state,
	}
}

func newVCopierCopyTaskResultHooks() *vcopierCopyTaskResultHooks {
	return &vcopierCopyTaskResultHooks{
		fns: make([]func(context.Context, *vcopierCopyTaskResult), 0),
	}
}

func newVCopierCopyWorkQueue(
	concurrent bool,
	maxDepth int,
	workerFactory func(ctx context.Context) (*vcopierCopyWorker, error),
) *vcopierCopyWorkQueue {
	maxDepth = int(math.Max(float64(maxDepth), 1))
	return &vcopierCopyWorkQueue{
		concurrent:    concurrent,
		maxDepth:      maxDepth,
		workerFactory: workerFactory,
	}
}

func newVCopierCopyWorker(
	closeDbClient bool,
	vdbClient *vdbClient,
) *vcopierCopyWorker {
	return &vcopierCopyWorker{
		closeDbClient: closeDbClient,
		vdbClient:     vdbClient,
	}
}

// initTablesForCopy (phase 1) identifies the list of tables to be copied and inserts
// them into copy_state. If there are no tables to copy, it explicitly stops
// the stream. Otherwise, the copy phase (phase 2) may think that all tables are copied.
// This will cause us to go into the replication phase (phase 3) without a starting position.
func (vc *vcopier) initTablesForCopy(ctx context.Context) error {
	defer vc.vr.dbClient.Rollback()

	plan, err := buildReplicatorPlan(vc.vr.source, vc.vr.colInfoMap, nil, vc.vr.stats)
	if err != nil {
		return err
	}
	if err := vc.vr.dbClient.Begin(); err != nil {
		return err
	}
	// Insert the table list only if at least one table matches.
	if len(plan.TargetTables) != 0 {
		var buf strings.Builder
		buf.WriteString("insert into _vt.copy_state(vrepl_id, table_name) values ")
		prefix := ""
		for name := range plan.TargetTables {
			fmt.Fprintf(&buf, "%s(%d, %s)", prefix, vc.vr.id, encodeString(name))
			prefix = ", "
		}
		if _, err := vc.vr.dbClient.Execute(buf.String()); err != nil {
			return err
		}
		if err := vc.vr.setState(binlogplayer.VReplicationCopying, ""); err != nil {
			return err
		}
		if err := vc.vr.insertLog(LogCopyStart, fmt.Sprintf("Copy phase started for %d table(s)",
			len(plan.TargetTables))); err != nil {
			return err
		}

		if vc.vr.supportsDeferredSecondaryKeys() {
			settings, err := binlogplayer.ReadVRSettings(vc.vr.dbClient, vc.vr.id)
			if err != nil {
				return err
			}
			if settings.DeferSecondaryKeys {
				if err := vc.vr.insertLog(LogCopyStart, fmt.Sprintf("Copy phase temporarily dropping secondary keys for %d table(s)",
					len(plan.TargetTables))); err != nil {
					return err
				}
				for name := range plan.TargetTables {
					if err := vc.vr.stashSecondaryKeys(ctx, name); err != nil {
						return err
					}
				}
				if err := vc.vr.insertLog(LogCopyStart,
					fmt.Sprintf("Copy phase finished dropping secondary keys and saving post copy actions to restore them for %d table(s)",
						len(plan.TargetTables))); err != nil {
					return err
				}
			}
		}
	} else {
		if err := vc.vr.setState(binlogplayer.BlpStopped, "There is nothing to replicate"); err != nil {
			return err
		}
	}
	return vc.vr.dbClient.Commit()
}

// copyNext performs a multi-step process on each iteration.
// Step 1: catchup: During this step, it replicates from the source from the last position.
// This is a partial replication: events are applied only to tables or subsets of tables
// that have already been copied. This goes on until replication catches up.
// Step 2: Start streaming. This returns the initial field info along with the GTID
// as of which the snapshot is being streamed.
// Step 3: fastForward: The target is fast-forwarded to the GTID obtained. This should
// be quick because we were mostly caught up as of step 1. This ensures that the
// snapshot of the rows are consistent with the position where the target stopped.
// Step 4: copy rows: Copy the next set of rows from the stream that was started in Step 2.
// This goes on until all rows are copied, or a timeout. In both cases, copyNext
// returns, and the replicator decides whether to invoke copyNext again, or to
// go to the next phase if all the copying is done.
// Steps 2, 3 and 4 are performed by copyTable.
// copyNext also builds the copyState metadata that contains the tables and their last
// primary key that was copied. A nil Result means that nothing has been copied.
// A table that was fully copied is removed from copyState.
func (vc *vcopier) copyNext(ctx context.Context, settings binlogplayer.VRSettings) error {
	qr, err := vc.vr.dbClient.Execute(fmt.Sprintf("select table_name, lastpk from _vt.copy_state where vrepl_id = %d and id in (select max(id) from _vt.copy_state group by vrepl_id, table_name)", vc.vr.id))
	if err != nil {
		return err
	}
	var tableToCopy string
	copyState := make(map[string]*sqltypes.Result)
	for _, row := range qr.Rows {
		tableName := row[0].ToString()
		lastpk := row[1].ToString()
		if tableToCopy == "" {
			tableToCopy = tableName
		}
		copyState[tableName] = nil
		if lastpk != "" {
			var r querypb.QueryResult
			if err := prototext.Unmarshal([]byte(lastpk), &r); err != nil {
				return err
			}
			copyState[tableName] = sqltypes.Proto3ToResult(&r)
		}
	}
	if len(copyState) == 0 {
		return fmt.Errorf("unexpected: there are no tables to copy")
	}
	if err := vc.catchup(ctx, copyState); err != nil {
		return err
	}
	return vc.copyTable(ctx, tableToCopy, copyState)
}

// catchup replays events to the subset of the tables that have been copied
// until replication is caught up. In order to stop, the seconds behind primary has
// to fall below replicationLagTolerance.
func (vc *vcopier) catchup(ctx context.Context, copyState map[string]*sqltypes.Result) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer vc.vr.stats.PhaseTimings.Record("catchup", time.Now())

	settings, err := binlogplayer.ReadVRSettings(vc.vr.dbClient, vc.vr.id)
	if err != nil {
		return err
	}
	// If there's no start position, it means we're copying the
	// first table. So, there's nothing to catch up to.
	if settings.StartPos.IsZero() {
		return nil
	}

	// Start vreplication.
	errch := make(chan error, 1)
	go func() {
		errch <- newVPlayer(vc.vr, settings, copyState, mysql.Position{}, "catchup").play(ctx)
	}()

	// Wait for catchup.
	tkr := time.NewTicker(waitRetryTime)
	defer tkr.Stop()
	seconds := int64(replicaLagTolerance / time.Second)
	for {
		sbm := vc.vr.stats.ReplicationLagSeconds.Get()
		if sbm < seconds {
			cancel()
			// Make sure vplayer returns before returning.
			<-errch
			return nil
		}
		select {
		case err := <-errch:
			if err != nil {
				return err
			}
			return io.EOF
		case <-ctx.Done():
			// Make sure vplayer returns before returning.
			<-errch
			return io.EOF
		case <-tkr.C:
		}
	}
}

// copyTable performs the synchronized copy of the next set of rows from
// the current table being copied. Each packet received is transactionally
// committed with the lastpk. This allows for consistent resumability.
func (vc *vcopier) copyTable(ctx context.Context, tableName string, copyState map[string]*sqltypes.Result) error {
	defer vc.vr.dbClient.Rollback()
	defer vc.vr.stats.PhaseTimings.Record("copy", time.Now())
	defer vc.vr.stats.CopyLoopCount.Add(1)

	log.Infof("Copying table %s, lastpk: %v", tableName, copyState[tableName])

	plan, err := buildReplicatorPlan(vc.vr.source, vc.vr.colInfoMap, nil, vc.vr.stats)
	if err != nil {
		return err
	}

	initialPlan, ok := plan.TargetTables[tableName]
	if !ok {
		return fmt.Errorf("plan not found for table: %s, current plans are: %#v", tableName, plan.TargetTables)
	}

	ctx, cancel := context.WithTimeout(ctx, copyPhaseDuration)
	defer cancel()

	var lastpkpb *querypb.QueryResult
	if lastpkqr := copyState[tableName]; lastpkqr != nil {
		lastpkpb = sqltypes.ResultToProto3(lastpkqr)
	}

	rowsCopiedTicker := time.NewTicker(rowsCopiedUpdateInterval)
	defer rowsCopiedTicker.Stop()
	copyStateGCTicker := time.NewTicker(copyStateGCInterval)
	defer copyStateGCTicker.Stop()

	parallelism := int(math.Max(1, float64(vreplicationParallelInsertWorkers)))
	copyWorkerFactory := vc.newCopyWorkerFactory(parallelism)
	copyWorkQueue := vc.newCopyWorkQueue(parallelism, copyWorkerFactory)
	defer copyWorkQueue.close()

	// Allocate a result channel to collect results from tasks.
	resultCh := make(chan *vcopierCopyTaskResult, parallelism*4)
	defer close(resultCh)

	var lastpk *querypb.Row
	var pkfields []*querypb.Field

	// Use this for task sequencing.
	var prevCh <-chan *vcopierCopyTaskResult

	serr := vc.vr.sourceVStreamer.VStreamRows(ctx, initialPlan.SendRule.Filter, lastpkpb, func(rows *binlogdatapb.VStreamRowsResponse) error {
		for {
			select {
			case <-rowsCopiedTicker.C:
				update := binlogplayer.GenerateUpdateRowsCopied(vc.vr.id, vc.vr.stats.CopyRowCount.Get())
				_, _ = vc.vr.dbClient.Execute(update)
			case <-ctx.Done():
				return io.EOF
			default:
			}
			select {
			case <-copyStateGCTicker.C:
				// Garbage collect older copy_state rows:
				//   - Using a goroutine so that we are not blocking the copy flow
				//   - Using a new connection so that we do not change the transactional behavior of the copy itself
				// This helps to ensure that the table does not grow too large and the
				// number of rows does not have a big impact on the queries used for
				// the workflow.
				go func() {
					gcQuery := fmt.Sprintf("delete from _vt.copy_state where vrepl_id = %d and table_name = %s and id < (select maxid from (select max(id) as maxid from _vt.copy_state where vrepl_id = %d and table_name = %s) as depsel)",
						vc.vr.id, encodeString(tableName), vc.vr.id, encodeString(tableName))
					dbClient := vc.vr.vre.getDBClient(false)
					if err := dbClient.Connect(); err != nil {
						log.Errorf("Error while garbage collecting older copy_state rows, could not connect to database: %v", err)
						return
					}
					defer dbClient.Close()
					if _, err := dbClient.ExecuteFetch(gcQuery, -1); err != nil {
						log.Errorf("Error while garbage collecting older copy_state rows with query %q: %v", gcQuery, err)
					}
				}()
			case <-ctx.Done():
				return io.EOF
			default:
			}
			if rows.Throttled {
				_ = vc.vr.updateTimeThrottled(RowStreamerComponentName)
				return nil
			}
			if rows.Heartbeat {
				_ = vc.vr.updateHeartbeatTime(time.Now().Unix())
				return nil
			}
			// verify throttler is happy, otherwise keep looping
			if vc.vr.vre.throttlerClient.ThrottleCheckOKOrWaitAppName(ctx, vc.throttlerAppName) {
				break // out of 'for' loop
			} else { // we're throttled
				_ = vc.vr.updateTimeThrottled(VCopierComponentName)
			}
		}
		if !copyWorkQueue.isOpen {
			if len(rows.Fields) == 0 {
				return fmt.Errorf("expecting field event first, got: %v", rows)
			}
			if err := vc.fastForward(ctx, copyState, rows.Gtid); err != nil {
				return err
			}
			fieldEvent := &binlogdatapb.FieldEvent{
				TableName: initialPlan.SendRule.Match,
			}
			fieldEvent.Fields = append(fieldEvent.Fields, rows.Fields...)
			tablePlan, err := plan.buildExecutionPlan(fieldEvent)
			if err != nil {
				return err
			}
			pkfields = append(pkfields, rows.Pkfields...)
			buf := sqlparser.NewTrackedBuffer(nil)
			buf.Myprintf(
				"insert into _vt.copy_state (lastpk, vrepl_id, table_name) values (%a, %s, %s)", ":lastpk",
				strconv.Itoa(int(vc.vr.id)),
				encodeString(tableName))
			addLatestCopyState := buf.ParsedQuery()
			copyWorkQueue.open(addLatestCopyState, pkfields, tablePlan)
		}
		if len(rows.Rows) == 0 {
			return nil
		}

		// Clone rows, since pointer values will change while async work is
		// happening. Can skip this when there's no parallelism.
		if parallelism > 1 {
			rows = proto.Clone(rows).(*binlogdatapb.VStreamRowsResponse)
		}

		// Prepare a vcopierCopyTask for the current batch of work.
		// TODO(maxeng) see if using a pre-allocated pool will speed things up.
		currCh := make(chan *vcopierCopyTaskResult, 1)
		currT := newVCopierCopyTask(newVCopierCopyTaskArgs(rows.Rows, rows.Lastpk))

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

	// Close the work queue. This will prevent new tasks from being enqueued,
	// and will wait until all workers are returned to the worker pool.
	copyWorkQueue.close()

	// When tasks are executed async, there may be tasks that complete (or fail)
	// after the last VStreamRows callback exits. Get the lastpk from completed
	// tasks, or errors from failed ones.
	var empty bool
	var terrs []error
	for !empty {
		select {
		case result := <-resultCh:
			switch result.state {
			case vcopierCopyTaskCancel:
				// A task cancelation probably indicates an expired context due
				// to a PlannedReparentShard or elapsed copy phase duration,
				// neither of which are error conditions.
			case vcopierCopyTaskComplete:
				// Get the latest lastpk, purely for logging purposes.
				lastpk = result.args.lastpk
			case vcopierCopyTaskFail:
				// Aggregate non-nil errors.
				terrs = append(terrs, result.err)
			}
		default:
			empty = true
		}
	}
	if len(terrs) > 0 {
		terr := vterrors.Aggregate(terrs)
		log.Warningf("task error in workflow %s: %v", vc.vr.WorkflowName, terr)
		return vterrors.Wrapf(terr, "task error")
	}

	// Get the last committed pk into a loggable form.
	lastpkbuf, merr := prototext.Marshal(&querypb.QueryResult{
		Fields: pkfields,
		Rows:   []*querypb.Row{lastpk},
	})
	if merr != nil {
		return fmt.Errorf("failed to marshal pk fields and value into query result: %s", merr.Error())
	}
	lastpkbv := map[string]*querypb.BindVariable{
		"lastpk": {
			Type:  sqltypes.VarBinary,
			Value: lastpkbuf,
		},
	}

	// A context expiration was probably caused by a PlannedReparentShard or an
	// elapsed copy phase duration. Those are normal, non-error interruptions
	// of a copy phase.
	select {
	case <-ctx.Done():
		log.Infof("Copy of %v stopped at lastpk: %v", tableName, lastpkbv)
		return nil
	default:
	}
	if serr != nil {
		return serr
	}

	// Perform any post copy actions
	if err := vc.vr.execPostCopyActions(ctx, tableName); err != nil {
		return vterrors.Wrapf(err, "failed to execute post copy actions for table %q", tableName)
	}

	log.Infof("Copy of %v finished at lastpk: %v", tableName, lastpkbv)
	buf := sqlparser.NewTrackedBuffer(nil)
	buf.Myprintf(
		"delete cs, pca from %s as cs left join %s as pca on cs.vrepl_id=pca.vrepl_id and cs.table_name=pca.table_name where cs.vrepl_id=%d and cs.table_name=%s",
		copyStateTableName, postCopyActionTableName,
		vc.vr.id, encodeString(tableName),
	)
	if _, err := vc.vr.dbClient.Execute(buf.String()); err != nil {
		return err
	}

	return nil
}

func (vc *vcopier) fastForward(ctx context.Context, copyState map[string]*sqltypes.Result, gtid string) error {
	defer vc.vr.stats.PhaseTimings.Record("fastforward", time.Now())
	pos, err := mysql.DecodePosition(gtid)
	if err != nil {
		return err
	}
	settings, err := binlogplayer.ReadVRSettings(vc.vr.dbClient, vc.vr.id)
	if err != nil {
		return err
	}
	if settings.StartPos.IsZero() {
		update := binlogplayer.GenerateUpdatePos(vc.vr.id, pos, time.Now().Unix(), 0, vc.vr.stats.CopyRowCount.Get(), vreplicationStoreCompressedGTID)
		_, err := vc.vr.dbClient.Execute(update)
		return err
	}
	return newVPlayer(vc.vr, settings, copyState, pos, "fastforward").play(ctx)
}

func (vc *vcopier) newCopyWorkQueue(
	parallelism int,
	workerFactory func(context.Context) (*vcopierCopyWorker, error),
) *vcopierCopyWorkQueue {
	concurrent := parallelism > 1
	return newVCopierCopyWorkQueue(concurrent, parallelism, workerFactory)
}

func (vc *vcopier) newCopyWorkerFactory(parallelism int) func(context.Context) (*vcopierCopyWorker, error) {
	if parallelism > 1 {
		return func(ctx context.Context) (*vcopierCopyWorker, error) {
			dbClient, err := vc.vr.newClientConnection(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to create new db client: %s", err.Error())
			}
			return newVCopierCopyWorker(
				true, /* close db client */
				dbClient,
			), nil
		}
	}
	return func(_ context.Context) (*vcopierCopyWorker, error) {
		return newVCopierCopyWorker(
			false, /* close db client */
			vc.vr.dbClient,
		), nil
	}
}

// close waits for all workers to be returned to the worker pool.
func (vcq *vcopierCopyWorkQueue) close() {
	if !vcq.isOpen {
		return
	}
	vcq.isOpen = false
	vcq.workerPool.Close()
}

// enqueue a new copy task. This will obtain a worker from the pool, execute
// the task with that worker, and afterwards return the worker to the pool. If
// vcopierCopyWorkQueue is configured to operate concurrently, the task will be
// executed in a separate goroutine. Otherwise the task will be executed in the
// calling goroutine.
func (vcq *vcopierCopyWorkQueue) enqueue(ctx context.Context, currT *vcopierCopyTask) error {
	if !vcq.isOpen {
		return fmt.Errorf("work queue is not open")
	}

	// Get a handle on an unused worker.
	poolH, err := vcq.workerPool.Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to get a worker from pool: %s", err.Error())
	}

	currW, ok := poolH.(*vcopierCopyWorker)
	if !ok {
		return fmt.Errorf("failed to cast pool resource to *vcopierCopyWorker")
	}

	execute := func(task *vcopierCopyTask) {
		currW.execute(ctx, task)
		vcq.workerPool.Put(poolH)
	}

	// If the work queue is configured to work concurrently, execute the task
	// in a separate goroutine. Otherwise execute the task in the calling
	// goroutine.
	if vcq.concurrent {
		go execute(currT)
	} else {
		execute(currT)
	}

	return nil
}

// open the work queue. The provided arguments are used to generate
// statements for inserting rows and copy state.
func (vcq *vcopierCopyWorkQueue) open(
	copyStateInsert *sqlparser.ParsedQuery,
	pkfields []*querypb.Field,
	tablePlan *TablePlan,
) {
	if vcq.isOpen {
		return
	}

	poolCapacity := int(math.Max(float64(vcq.maxDepth), 1))
	vcq.workerPool = pools.NewResourcePool(
		/* factory */
		func(ctx context.Context) (pools.Resource, error) {
			worker, err := vcq.workerFactory(ctx)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to create vcopier worker: %s",
					err.Error(),
				)
			}
			worker.open(copyStateInsert, pkfields, tablePlan)
			return worker, nil
		},
		poolCapacity, /* initial capacity */
		poolCapacity, /* max capacity */
		0,            /* idle timeout */
		0,            /* max lifetime */
		nil,          /* log wait */
		nil,          /* refresh check */
		0,            /* refresh interval */
	)

	vcq.isOpen = true
}

// after returns a vcopierCopyTaskHooks that can be used to register callbacks
// to be triggered after the specified vcopierCopyTaskState.
func (vtl *vcopierCopyTaskLifecycle) after(state vcopierCopyTaskState) *vcopierCopyTaskHooks {
	key := "after:" + state.String()
	if _, ok := vtl.hooks[key]; !ok {
		vtl.hooks[key] = newVCopierCopyTaskHooks()
	}
	return vtl.hooks[key]
}

// before returns a vcopierCopyTaskHooks that can be used to register callbacks
// to be triggered before the the specified vcopierCopyTaskState.
func (vtl *vcopierCopyTaskLifecycle) before(state vcopierCopyTaskState) *vcopierCopyTaskHooks {
	key := "before:" + state.String()
	if _, ok := vtl.hooks[key]; !ok {
		vtl.hooks[key] = newVCopierCopyTaskHooks()
	}
	return vtl.hooks[key]
}

// onResult returns a vcopierCopyTaskResultHooks that can be used to register
// callbacks to be triggered when a task reaches a "done" state (= canceled,
// completed, failed).
func (vtl *vcopierCopyTaskLifecycle) onResult() *vcopierCopyTaskResultHooks {
	return vtl.resultHooks
}

// tryAdvance is a convenient way of wrapping up lifecycle hooks with task
// execution steps. E.g.:
//
//	if _, err := task.lifecycle.before(nextState).notify(ctx, args); err != nil {
//		return err
//	}
//	if _, err := fn(ctx, args); err != nil {
//		return err
//	}
//	if _, err := task.lifecycle.after(nextState).notify(ctx, args); err != nil {
//		return err
//	}
//
// Is roughly equivalent to:
//
//	if _, err := task.Lifecycle.tryAdvance(ctx, args, nextState, fn); err != nil {
//	  return err
//	}
func (vtl *vcopierCopyTaskLifecycle) tryAdvance(
	ctx context.Context,
	args *vcopierCopyTaskArgs,
	nextState vcopierCopyTaskState,
	fn func(context.Context, *vcopierCopyTaskArgs) error,
) (vcopierCopyTaskState, error) {
	var err error
	newState := nextState

	if err = vtl.before(nextState).notify(ctx, args); err != nil {
		goto END
	}
	if err = fn(ctx, args); err != nil {
		goto END
	}
	if err = vtl.after(nextState).notify(ctx, args); err != nil {
		goto END
	}

END:
	if err != nil {
		newState = vcopierCopyTaskFail
		if vterrors.Code(err) == vtrpcpb.Code_CANCELED {
			newState = vcopierCopyTaskCancel
		}
	}

	return newState, err
}

// do registers a callback with the vcopierCopyTaskResultHooks, to be triggered
// when a task reaches a "done" state (= canceled, completed, failed).
func (vrh *vcopierCopyTaskResultHooks) do(fn func(context.Context, *vcopierCopyTaskResult)) {
	vrh.fns = append(vrh.fns, fn)
}

// notify triggers all callbacks registered with this vcopierCopyTaskResultHooks.
func (vrh *vcopierCopyTaskResultHooks) notify(ctx context.Context, result *vcopierCopyTaskResult) {
	for _, fn := range vrh.fns {
		fn(ctx, result)
	}
}

// sendTo registers a hook that accepts a result and sends the result to the
// provided channel. E.g.:
//
//	resultCh := make(chan *vcopierCopyTaskResult, 1)
//	task.lifecycle.onResult().sendTo(resultCh)
//	defer func() {
//	  result := <-resultCh
//	}()
func (vrh *vcopierCopyTaskResultHooks) sendTo(ch chan<- *vcopierCopyTaskResult) {
	vrh.do(func(ctx context.Context, result *vcopierCopyTaskResult) {
		select {
		case ch <- result:
		case <-ctx.Done():
			// Failure to send the result to the consumer on the other side of
			// the channel before context expires will have the following
			// consequences:
			//
			// * Subsequent tasks waiting for this task to complete won't run.
			//   That's OK. They won't hang waiting on the channel because,
			//   like this task they respond to context expiration.
			// * The outermost loop managing task execution may not know that
			//   this task failed or succeeded.
			//   - In the case that this task succeeded, statistics and logging
			//     will not indicate that this task completed. That's not great,
			//     but shouldn't negatively impact the integrity of data or the
			//     copy workflow because the current state has been persisted
			//     to the database.
			//   - In the case that this task failed, there should be no adverse
			//     impact: the outermost loop handles context expiration by
			//     stopping the copy phase without completing it.
		}
	})
}

// awaitCompletion registers a callback that returns an error unless the
// provided chan produces a vcopierTaskResult in a complete state.
//
// This is useful for sequencing vcopierCopyTasks, e.g.:
//
//	resultCh := make(chan *vcopierCopyTaskResult, 1)
//	prevT.lifecycle.onResult().sendTo(resultCh)
//	currT.Lifecycle.before(vcopierCopyTaskInsertCopyState).awaitCompletion(resultCh)
func (vth *vcopierCopyTaskHooks) awaitCompletion(resultCh <-chan *vcopierCopyTaskResult) {
	vth.do(func(ctx context.Context, args *vcopierCopyTaskArgs) error {
		select {
		case result := <-resultCh:
			if result == nil {
				return fmt.Errorf("channel was closed before a result received")
			}
			if !vcopierCopyTaskStateIsDone(result.state) {
				return fmt.Errorf("received result is not done")
			}
			if result.state != vcopierCopyTaskComplete {
				return fmt.Errorf("received result is not complete")
			}
			return nil
		case <-ctx.Done():
			// A context expiration probably indicates a PlannedReparentShard
			// or an elapsed copy phase duration. Those aren't treated as error
			// conditions, but we'll return the context error here anyway.
			//
			// Task execution will detect the presence of the error, mark this
			// task canceled, and abort. Subsequent tasks won't execute because
			// this task didn't complete.
			return vterrors.Errorf(vtrpcpb.Code_CANCELED, "context has expired")
		}
	})
}

// do registers a callback with the vcopierCopyTaskResultHooks, to be triggered
// before or after a user-specified state.
func (vth *vcopierCopyTaskHooks) do(fn func(context.Context, *vcopierCopyTaskArgs) error) {
	vth.fns = append(vth.fns, fn)
}

// notify triggers all callbacks registered with this vcopierCopyTaskHooks.
func (vth *vcopierCopyTaskHooks) notify(ctx context.Context, args *vcopierCopyTaskArgs) error {
	for _, fn := range vth.fns {
		if err := fn(ctx, args); err != nil {
			return err
		}
	}
	return nil
}

func (vts vcopierCopyTaskState) String() string {
	switch vts {
	case vcopierCopyTaskPending:
		return "pending"
	case vcopierCopyTaskBegin:
		return "begin"
	case vcopierCopyTaskInsertRows:
		return "insert-rows"
	case vcopierCopyTaskInsertCopyState:
		return "insert-copy-state"
	case vcopierCopyTaskCommit:
		return "commit"
	case vcopierCopyTaskCancel:
		return "done:cancel"
	case vcopierCopyTaskComplete:
		return "done:complete"
	case vcopierCopyTaskFail:
		return "done:fail"
	}

	return fmt.Sprintf("undefined(%d)", int(vts))
}

// ApplySetting implements pools.Resource.
func (vbc *vcopierCopyWorker) ApplySetting(context.Context, *pools.Setting) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "[BUG] vcopierCopyWorker does not implement ApplySetting")
}

// Close implements pool.Resource.
func (vbc *vcopierCopyWorker) Close() {
	if !vbc.isOpen {
		return
	}

	vbc.isOpen = false
	if vbc.closeDbClient {
		vbc.vdbClient.Close()
	}
}

// Expired implements pools.Resource.
func (vbc *vcopierCopyWorker) Expired(time.Duration) bool {
	return false
}

// IsSameSetting implements pools.Resource.
func (vbc *vcopierCopyWorker) IsSameSetting(string) bool {
	return true
}

// IsSettingApplied implements pools.Resource.
func (vbc *vcopierCopyWorker) IsSettingApplied() bool {
	return false
}

// ResetSetting implements pools.Resource.
func (vbc *vcopierCopyWorker) ResetSetting(context.Context) error {
	return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "[BUG] vcopierCopyWorker does not implement ResetSetting")
}

// execute advances a task through each state until it is done (= canceled,
// completed, failed).
func (vbc *vcopierCopyWorker) execute(ctx context.Context, task *vcopierCopyTask) *vcopierCopyTaskResult {
	startedAt := time.Now()
	state := vcopierCopyTaskPending

	var err error

	// As long as the current state is not done, keep trying to advance to the
	// next state.
	for !vcopierCopyTaskStateIsDone(state) {
		// Get the next state that we want to advance to.
		nextState := vcopierCopyTaskGetNextState(state)

		var advanceFn func(context.Context, *vcopierCopyTaskArgs) error

		// Get the advanceFn to use to advance the task to the nextState.
		switch nextState {
		case vcopierCopyTaskBegin:
			advanceFn = func(context.Context, *vcopierCopyTaskArgs) error {
				// Rollback to make sure we're in a clean state.
				if err := vbc.vdbClient.Rollback(); err != nil {
					return vterrors.Wrapf(err, "failed to rollback")
				}
				// Begin transaction.
				if err := vbc.vdbClient.Begin(); err != nil {
					return vterrors.Wrapf(err, "failed to start transaction")
				}
				return nil
			}
		case vcopierCopyTaskInsertRows:
			advanceFn = func(ctx context.Context, args *vcopierCopyTaskArgs) error {
				if _, err := vbc.insertRows(ctx, args.rows); err != nil {
					return vterrors.Wrapf(err, "failed inserting rows")
				}
				return nil
			}
		case vcopierCopyTaskInsertCopyState:
			advanceFn = func(ctx context.Context, args *vcopierCopyTaskArgs) error {
				if err := vbc.insertCopyState(ctx, args.lastpk); err != nil {
					return vterrors.Wrapf(err, "error updating _vt.copy_state")
				}
				return nil
			}
		case vcopierCopyTaskCommit:
			advanceFn = func(context.Context, *vcopierCopyTaskArgs) error {
				// Commit.
				if err := vbc.vdbClient.Commit(); err != nil {
					return vterrors.Wrapf(err, "error commiting transaction")
				}
				return nil
			}
		case vcopierCopyTaskComplete:
			advanceFn = func(context.Context, *vcopierCopyTaskArgs) error { return nil }
		default:
			err = fmt.Errorf("don't know how to advance from %s to %s", state, nextState)
			state = vcopierCopyTaskFail
			goto END
		}

		// tryAdvance tries to execute advanceFn, and also executes any
		// lifecycle hooks surrounding the provided state.
		//
		// If all lifecycle hooks and advanceFn are successfully executed,
		// tryAdvance will return state, which should be == the nextState that
		// we provided.
		//
		// If there was a failure executing lifecycle hooks or advanceFn,
		// tryAdvance will return a failure state (i.e. canceled or failed),
		// along with a diagnostic error.
		if state, err = task.lifecycle.tryAdvance(ctx, task.args, nextState, advanceFn); err != nil {
			goto END
		}
	}

END:
	// At this point, we're in a "done" state (= canceled, completed, failed).
	// Notify any onResult callbacks.
	result := newVCopierCopyTaskResult(task.args, startedAt, state, err)
	task.lifecycle.onResult().notify(ctx, result)

	return result
}

func (vbc *vcopierCopyWorker) insertCopyState(ctx context.Context, lastpk *querypb.Row) error {
	var buf []byte
	buf, err := prototext.Marshal(&querypb.QueryResult{
		Fields: vbc.pkfields,
		Rows:   []*querypb.Row{lastpk},
	})
	if err != nil {
		return err
	}
	bv := map[string]*querypb.BindVariable{
		"lastpk": {
			Type:  sqltypes.VarBinary,
			Value: buf,
		},
	}
	copyStateInsert, err := vbc.copyStateInsert.GenerateQuery(bv, nil)
	if err != nil {
		return err
	}
	if _, err := vbc.vdbClient.Execute(copyStateInsert); err != nil {
		return err
	}
	return nil
}

func (vbc *vcopierCopyWorker) insertRows(ctx context.Context, rows []*querypb.Row) (*sqltypes.Result, error) {
	return vbc.tablePlan.applyBulkInsert(
		&vbc.sqlbuffer,
		rows,
		func(sql string) (*sqltypes.Result, error) {
			return vbc.vdbClient.ExecuteWithRetry(ctx, sql)
		},
	)
}

// open the vcopierCopyWorker. The provided arguments are used to generate
// statements for inserting rows and copy state.
func (vbc *vcopierCopyWorker) open(
	copyStateInsert *sqlparser.ParsedQuery,
	pkfields []*querypb.Field,
	tablePlan *TablePlan,
) {
	if vbc.isOpen {
		return
	}
	vbc.copyStateInsert = copyStateInsert
	vbc.isOpen = true
	vbc.pkfields = pkfields
	vbc.tablePlan = tablePlan
}

// vcopierCopyTaskStateIsDone returns true if the provided state is in one of
// these three states:
//
// * vcopierCopyTaskCancel
// * vcopierCopyTaskComplete
// * vcopierCopyTaskFail
func vcopierCopyTaskStateIsDone(vts vcopierCopyTaskState) bool {
	return vts == vcopierCopyTaskCancel ||
		vts == vcopierCopyTaskComplete ||
		vts == vcopierCopyTaskFail
}

// vcopierCopyTaskGetNextState returns the optimistic next state. The next
// state after vcopierCopyTaskPending is vcopierCopyTaskInProgress, followed
// by vcopierCopyTaskInsertRows, etc.
func vcopierCopyTaskGetNextState(vts vcopierCopyTaskState) vcopierCopyTaskState {
	switch vts {
	case vcopierCopyTaskPending:
		return vcopierCopyTaskBegin
	case vcopierCopyTaskBegin:
		return vcopierCopyTaskInsertRows
	case vcopierCopyTaskInsertRows:
		return vcopierCopyTaskInsertCopyState
	case vcopierCopyTaskInsertCopyState:
		return vcopierCopyTaskCommit
	case vcopierCopyTaskCommit:
		return vcopierCopyTaskComplete
	}
	return vts
}
