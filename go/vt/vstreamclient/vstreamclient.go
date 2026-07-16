/*
Copyright 2025 The Vitess Authors.

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

package vstreamclient

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"vitess.io/vitess/go/vt/log"
	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	"vitess.io/vitess/go/vt/vterrors"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
	"vitess.io/vitess/go/vt/vtgate/vtgateconn"
)

// VStreamClient is the primary struct
type VStreamClient struct {
	cfg clientConfig

	// this session is used to obtain the shards for the keyspace, and to manage the state table
	session *vtgateconn.VTGateSession

	// shardsByKeyspace caches the cluster's serving shards per keyspace, used to validate
	// configured keyspaces and to bootstrap the initial vgtid
	shardsByKeyspace map[string][]string

	// keep per table state and config, which is used to generate the vgtid filter.
	// this is a map of keyspace.table to TableConfig, since that's how the binlog table is stored
	tables map[string]*TableConfig

	// lastEventProcessedAtUnixNano is the time after the last event batch was processed, including time for the
	// user provided flush function to complete. A zero value means Run has not successfully processed any events yet,
	// so heartbeat liveness checks should not trigger startup cancellation.
	lastEventProcessedAtUnixNano atomic.Int64
	isProcessingEvents           atomic.Bool

	// lastFlushedVgtid is the last vgtid that was flushed, which is compared to the latestVgtid to determine
	// if we need to flush again.
	lastFlushedVgtid, latestVgtid *binlogdatapb.VGtid

	// inTransaction tracks whether the stream is between a BEGIN and its terminating event. Only
	// touched by the Run goroutine. With TransactionChunkSize enabled, VTGate can deliver a
	// transaction across multiple batches while heartbeats keep arriving independently, so
	// heartbeat-triggered flushes must be deferred until the transaction terminates.
	inTransaction bool

	stats VStreamStats

	lifecycle lifecycleState
}

type clientConfig struct {
	name string
	conn *vtgateconn.VTGateConn

	filter     *binlogdatapb.Filter
	flags      *vtgatepb.VStreamFlags
	tabletType topodatapb.TabletType

	// vgtidStateKeyspace and vgtidStateTable are the keyspace and table where the last VGtid is stored
	vgtidStateKeyspace string
	vgtidStateTable    string

	// ownerToken uniquely identifies this client instance in the state row. New claims it on startup,
	// and checkpoint writes require it to still match, so of two clients running with the same stream
	// name, the newest one wins and the older one fails fast with ErrFenced.
	ownerToken string

	// to avoid flushing too often, we will only flush if it has been at least minFlushDuration since the last flush.
	// we're relying on heartbeat events to handle max duration between flushes, in case there are no other events.
	minFlushDuration time.Duration

	// this is the duration between heartbeat events. This is not a duration because the server side
	// parameter only has a granularity of seconds.
	heartbeatSeconds int
	timeLocation     *time.Location

	// startupTimeout is how long the client waits for the first event after Run starts before shutting
	// itself down. heartbeatTimeoutMultiplier controls the liveness window after the first event, as a
	// multiple of the heartbeat interval.
	startupTimeout             time.Duration
	heartbeatTimeoutMultiplier int

	// these fields configure how graceful shutdown is configured
	gracefulShutdownChan    <-chan struct{}
	gracefulShutdownSignals []os.Signal
	gracefulShutdownWaitDur time.Duration

	// eventFuncs is a map of event type to the user provided function that should be called for that event type.
	eventFuncs map[binlogdatapb.VEventType]EventFunc
}

type lifecycleState struct {
	mu                sync.Mutex
	runUsed           bool
	runActive         bool
	shutdownRequested bool
	cancelRunCtxFn    context.CancelCauseFunc

	// shutdownCause records why the shutdown was requested (e.g. ErrHeartbeatTimeout); nil for
	// user-initiated graceful shutdowns. It is preserved so Run surfaces the cause even when the
	// final graceful flush succeeds.
	shutdownCause error

	gracefulShutdownFlushChan chan struct{}
	gracefulShutdownFlushOnce sync.Once
}

// VStreamStats keeps track of the number of rows processed and flushed for the whole stream
type VStreamStats struct {
	// how many rows have been processed for the whole stream, across all tables. These are incremented as each row
	// is processed, regardless of whether it is flushed.
	RowInsertCount int
	RowUpdateCount int
	RowDeleteCount int

	FlushedRowCount int // sum of rows flushed, regardless of table and including all insert/update/delete events

	// how many times the flush function was executed for the whole stream. Not incremented for no-ops
	FlushCount int
	// sum of successful, individual, table flush functions. Only increments if the table flush func is called
	TableFlushCount int

	LastFlushedAt time.Time // only set after a flush successfully completes
}

// New initializes a new VStreamClient, which is used to stream binlog events from Vitess.
func New(ctx context.Context, name string, conn *vtgateconn.VTGateConn, tables []TableConfig, opts ...Option) (*VStreamClient, error) {
	// validate required parameters
	if name == "" {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: name is required")
	}

	if len(name) > 64 {
		return nil, vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: name must be 64 characters or less, got %d", len(name))
	}

	if conn == nil {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: conn is required")
	}

	// initialize the VStreamClient, with options and settings to be set later
	v := &VStreamClient{
		cfg: clientConfig{
			name:                       name,
			conn:                       conn,
			minFlushDuration:           DefaultMinFlushDuration,
			timeLocation:               time.UTC,
			tabletType:                 topodatapb.TabletType_REPLICA,
			gracefulShutdownWaitDur:    DefaultGracefulShutdownWaitDur,
			startupTimeout:             DefaultStartupTimeout,
			heartbeatTimeoutMultiplier: DefaultHeartbeatTimeoutMultiplier,
			ownerToken:                 uuid.NewString(),
		},
		session: conn.Session("", nil),
		tables:  make(map[string]*TableConfig),
	}

	var err error

	// load all shards, so we can validate settings before starting. It's not technically necessary to do this here,
	// but it's more user-friendly to fail early if there is misconfiguration. This needs to be done before running
	// the options, so that the shards are available for validation.
	v.shardsByKeyspace, err = getShardsByKeyspace(ctx, v.session)
	if err != nil {
		return nil, err
	}

	err = v.initTables(tables)
	if err != nil {
		return nil, err
	}

	err = validateKeyspaceRuleSets(v.tables)
	if err != nil {
		return nil, err
	}

	err = validateBinlogRowImage(ctx, conn, v.tables)
	if err != nil {
		return nil, err
	}

	// set options from the variadic list
	for _, opt := range opts {
		if err = opt(v); err != nil {
			return nil, err
		}
	}

	for _, table := range v.tables {
		table.timeLocation = v.cfg.timeLocation
	}

	// validate required options and set defaults where possible

	// convert the tables into filter + rules

	rules := make([]*binlogdatapb.Rule, 0, len(v.tables))

	for _, table := range v.tables {
		rules = append(rules, &binlogdatapb.Rule{
			Match:  table.Table,
			Filter: table.Query,
		})
	}

	v.cfg.filter = &binlogdatapb.Filter{
		Rules: rules,
	}

	if v.cfg.flags == nil {
		v.cfg.flags = DefaultFlags()
	}

	if v.cfg.heartbeatSeconds > 0 {
		v.cfg.flags.HeartbeatInterval = uint32(v.cfg.heartbeatSeconds)
	}
	if err = validateTableNameMode(v.tables, v.cfg.flags); err != nil {
		return nil, err
	}

	// handle state lookup
	if v.cfg.vgtidStateKeyspace == "" || v.cfg.vgtidStateTable == "" {
		return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: state table not configured (use WithStateTable)")
	}

	explicitStartingVGtid := v.latestVgtid

	err = initStateTable(ctx, v.session, v.cfg.vgtidStateKeyspace, v.cfg.vgtidStateTable)
	if err != nil {
		return nil, err
	}

	storedVGtid, storedTableConfig, copyCompleted, rowExists, observedOwnerToken, err := getLatestVGtid(ctx, v.session, v.cfg.name, v.cfg.vgtidStateKeyspace, v.cfg.vgtidStateTable)
	if err != nil {
		return nil, err
	}

	var useExplicitStartingVGtid bool
	v.latestVgtid, useExplicitStartingVGtid = resolveLatestVGtid(explicitStartingVGtid, storedVGtid)

	// validate before mutating any state, so a constructor that fails can never leave a healthy
	// running client fenced. Resuming from stored state requires the tables to still match, to
	// make sure users aren't expecting to catch up on a table that was added after the last run.
	if !useExplicitStartingVGtid && storedVGtid != nil {
		err = validateTableConfig(v.tables, storedTableConfig)
		if err != nil {
			return nil, err
		}
	}

	// decide what needs to be persisted
	var persistVGtid *binlogdatapb.VGtid
	var persistCopyCompleted bool

	switch {
	case useExplicitStartingVGtid:
		// the caller explicitly provided a starting point: prefer it over persisted state, and
		// persist copy_completed so no copy phase runs, now or on restart
		persistVGtid, persistCopyCompleted = v.latestVgtid, true

	case v.latestVgtid == nil, !copyCompleted:
		// either there is no usable stored position, or the previous copy never finished:
		// (re)start the copy from the beginning with a fresh vgtid, and reset copy_completed
		// so a crash mid-copy is never mistaken for completed state
		v.latestVgtid, err = newVGtid(v.tables, v.shardsByKeyspace)
		if err != nil {
			return nil, err
		}
		persistVGtid, persistCopyCompleted = v.latestVgtid, false

	default:
		// resume from the stored checkpoint; nothing to persist beyond claiming ownership
	}

	// claiming ownership and persisting the starting state are the only state-mutating steps,
	// and they run last, as a single compare-and-swap against the owner token observed when
	// state was read: it fences out a still-running client with the same stream name, while a
	// stale constructor that lost the race fails with ErrFenced instead of stealing ownership
	// back from a newer client.
	if rowExists {
		if persistVGtid != nil {
			err = updateStateRow(ctx, v.session, v.cfg.name, v.cfg.vgtidStateKeyspace, v.cfg.vgtidStateTable, v.cfg.ownerToken, observedOwnerToken, persistVGtid, v.tables, persistCopyCompleted)
		} else {
			err = claimStateOwnership(ctx, v.session, v.cfg.name, v.cfg.vgtidStateKeyspace, v.cfg.vgtidStateTable, v.cfg.ownerToken, observedOwnerToken)
		}
		if err != nil {
			return nil, err
		}
	} else {
		err = insertStateRow(ctx, v.session, v.cfg.name, v.cfg.vgtidStateKeyspace, v.cfg.vgtidStateTable, v.cfg.ownerToken, persistVGtid, v.tables, persistCopyCompleted)
		if err != nil {
			return nil, err
		}
	}

	// every branch above leaves the state table holding exactly latestVgtid, so seed lastFlushedVgtid
	// with it. Otherwise the first flush on an idle stream would issue a no-op update, which MySQL
	// reports as RowsAffected=0 and updateLatestVGtid would treat as a missing state row.
	v.lastFlushedVgtid = v.latestVgtid

	return v, nil
}

func resolveLatestVGtid(explicit, stored *binlogdatapb.VGtid) (*binlogdatapb.VGtid, bool) {
	if explicit != nil {
		return explicit, true
	}

	return stored, false
}

// GracefulShutdown waits up to the provided duration for the next safe flush boundary,
// then cancels the active Run call.
//   - if a safe event happens before the wait ends, buffered rows may be flushed and the
//     client will stop processing the stream immediately afterward
//   - if no safe event happens during that window, any buffered rows stay uncheckpointed
//     and will be reprocessed when the stream is restarted
//
// GracefulShutdown returns immediately if wait is 0, or it has already been called.
//
// The return value does not guarantee whether one final buffered flush happened before shutdown;
// uncheckpointed work is replayed on the next startup.
//
// GracefulShutdown closes the client immediately. As with any completed Run attempt,
// call New(...) to create a fresh client before running again.
func (v *VStreamClient) GracefulShutdown(wait time.Duration) {
	v.gracefulShutdownWithCause(wait, nil)
}

// gracefulShutdownWithCause is the internal implementation of GracefulShutdown. The cause, if not nil,
// is recorded before waiting, so Run surfaces why the stream was stopped even when the final graceful
// flush succeeds during the wait.
func (v *VStreamClient) gracefulShutdownWithCause(wait time.Duration, cause error) {
	cancelRunCtxFn, gracefulShutdownFlushChan, ok := v.requestShutdown(cause)
	if !ok {
		return
	}

	if wait == 0 {
		cancelRunCtxFn(cause)
		return
	}

	select {
	case <-time.After(wait):
	case <-gracefulShutdownFlushChan:
	}

	cancelRunCtxFn(cause)
}

func (v *VStreamClient) beginRun(cancelRunCtxFn context.CancelCauseFunc) bool {
	v.lifecycle.mu.Lock()
	defer v.lifecycle.mu.Unlock()
	if v.lifecycle.runUsed {
		return false
	}
	v.lifecycle.runUsed = true
	v.lifecycle.runActive = true
	v.lifecycle.shutdownRequested = false
	v.lifecycle.shutdownCause = nil
	v.lifecycle.cancelRunCtxFn = cancelRunCtxFn
	v.lifecycle.gracefulShutdownFlushChan = make(chan struct{})
	v.lifecycle.gracefulShutdownFlushOnce = sync.Once{}
	return true
}

func (v *VStreamClient) endRun() {
	v.lifecycle.mu.Lock()
	v.lifecycle.runActive = false
	v.lifecycle.shutdownRequested = false
	v.lifecycle.cancelRunCtxFn = nil
	gracefulShutdownFlushChan := v.lifecycle.gracefulShutdownFlushChan
	gracefulShutdownFlushOnce := &v.lifecycle.gracefulShutdownFlushOnce
	v.lifecycle.mu.Unlock()

	// wake any GracefulShutdown caller still waiting for a flush: once Run has exited, no
	// future flush can ever close the channel, so the caller would otherwise block for its
	// entire wait duration
	if gracefulShutdownFlushChan != nil {
		gracefulShutdownFlushOnce.Do(func() {
			close(gracefulShutdownFlushChan)
		})
	}
}

func (v *VStreamClient) requestShutdown(cause error) (context.CancelCauseFunc, <-chan struct{}, bool) {
	v.lifecycle.mu.Lock()
	defer v.lifecycle.mu.Unlock()
	if !v.lifecycle.runActive || v.lifecycle.shutdownRequested || v.lifecycle.cancelRunCtxFn == nil {
		return nil, nil, false
	}
	v.lifecycle.shutdownRequested = true
	v.lifecycle.shutdownCause = cause
	return v.lifecycle.cancelRunCtxFn, v.lifecycle.gracefulShutdownFlushChan, true
}

// ShutdownRequested reports whether a graceful shutdown has been requested for the active Run,
// either by a GracefulShutdown call or by the internal liveness monitor. Flush functions can use
// it to detect that the current flush is the last one before the stream stops.
func (v *VStreamClient) ShutdownRequested() bool {
	v.lifecycle.mu.Lock()
	defer v.lifecycle.mu.Unlock()
	return v.lifecycle.shutdownRequested
}

func (v *VStreamClient) getShutdownCause() error {
	v.lifecycle.mu.Lock()
	defer v.lifecycle.mu.Unlock()
	return v.lifecycle.shutdownCause
}

func (v *VStreamClient) getGracefulShutdownFlushChan() <-chan struct{} {
	v.lifecycle.mu.Lock()
	defer v.lifecycle.mu.Unlock()
	return v.lifecycle.gracefulShutdownFlushChan
}

func (v *VStreamClient) signalGracefulShutdownFlushed() {
	v.lifecycle.mu.Lock()
	shutdownRequested := v.lifecycle.shutdownRequested
	gracefulShutdownFlushChan := v.lifecycle.gracefulShutdownFlushChan
	gracefulShutdownFlushOnce := &v.lifecycle.gracefulShutdownFlushOnce
	v.lifecycle.mu.Unlock()

	if !shutdownRequested || gracefulShutdownFlushChan == nil {
		return
	}

	gracefulShutdownFlushOnce.Do(func() {
		close(gracefulShutdownFlushChan)
	})
}

func getShardsByKeyspace(ctx context.Context, session *vtgateconn.VTGateSession) (map[string][]string, error) {
	query := "SHOW VITESS_SHARDS"
	result, err := session.Execute(ctx, query, nil, false)
	if err != nil {
		return nil, fmt.Errorf("vstreamclient: failed to get shards by keyspace: %w", err)
	}

	shardsByKeyspace := make(map[string][]string)

	for _, row := range result.Rows {
		keyspace, shard, found := strings.Cut(row[0].ToString(), "/")
		if !found {
			return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "vstreamclient: failed to parse keyspace_id: %s", row[0].ToString())
		}

		shardsByKeyspace[keyspace] = append(shardsByKeyspace[keyspace], shard)
	}

	return shardsByKeyspace, nil
}

// validateBinlogRowImage checks that every streamed source keyspace uses binlog_row_image=FULL.
// With NOBLOB or MINIMAL, delete before-images can omit column values in a way that is
// indistinguishable from SQL NULL to both the default decoder and custom scanners, silently
// corrupting nullable fields downstream. If the setting cannot be queried (e.g. restricted
// permissions), a warning is logged and the check is skipped.
func validateBinlogRowImage(ctx context.Context, conn *vtgateconn.VTGateConn, tables map[string]*TableConfig) error {
	keyspaces := make(map[string]bool)
	for _, table := range tables {
		keyspaces[table.Keyspace] = true
	}

	for _, keyspace := range slices.Sorted(maps.Keys(keyspaces)) {
		session := conn.Session(keyspace, nil)
		result, err := session.Execute(ctx, "select @@global.binlog_row_image", nil, false)
		if err != nil || len(result.Rows) == 0 || len(result.Rows[0]) == 0 {
			log.Warn(
				"vstreamclient: could not verify binlog_row_image; delete events may be silently corrupted unless it is FULL",
				slog.String("keyspace", keyspace),
				slog.Any("error", err),
			)
			continue
		}

		rowImage := result.Rows[0][0].ToString()
		if !strings.EqualFold(rowImage, "FULL") {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: keyspace %s uses binlog_row_image=%s, but this client requires FULL: with partial row images, omitted delete before-image values are indistinguishable from NULL", keyspace, rowImage)
		}
	}

	return nil
}

// validateKeyspaceRuleSets rejects configurations whose (table, query) rule sets differ across
// keyspaces. A single vstream filter is forwarded to every keyspace in the vgtid, so with
// heterogeneous sets each keyspace would also receive the other keyspaces' rules: the copy phase
// can fail on tables that don't exist there, and if a same-named foreign table does exist, its
// rows are streamed and then rejected or misrouted by this client.
func validateKeyspaceRuleSets(tables map[string]*TableConfig) error {
	rulesByKeyspace := make(map[string][]string)
	for _, table := range tables {
		rulesByKeyspace[table.Keyspace] = append(rulesByKeyspace[table.Keyspace], table.Table+" -> "+table.Query)
	}

	if len(rulesByKeyspace) <= 1 {
		return nil
	}

	var referenceKeyspace, referenceRules string
	for _, keyspace := range slices.Sorted(maps.Keys(rulesByKeyspace)) {
		rules := rulesByKeyspace[keyspace]
		slices.Sort(rules)
		joined := strings.Join(rules, "; ")

		if referenceKeyspace == "" {
			referenceKeyspace, referenceRules = keyspace, joined
			continue
		}
		if joined != referenceRules {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: keyspaces %s and %s have different table/query sets, but a single vstream filter applies every rule to every keyspace; configure identical sets per keyspace or use a separate client per keyspace", referenceKeyspace, keyspace)
		}
	}

	return nil
}

func validateTableNameMode(tables map[string]*TableConfig, flags *vtgatepb.VStreamFlags) error {
	if flags == nil || !flags.ExcludeKeyspaceFromTableName {
		return nil
	}

	keyspacesByTable := make(map[string][]string)
	for _, table := range tables {
		keyspacesByTable[table.Table] = append(keyspacesByTable[table.Table], table.Keyspace)
	}

	var ambiguous []string
	for tableName, keyspaces := range keyspacesByTable {
		if len(keyspaces) < 2 {
			continue
		}

		slices.Sort(keyspaces)
		ambiguous = append(ambiguous, fmt.Sprintf("%s (%s)", tableName, strings.Join(keyspaces, ", ")))
	}

	if len(ambiguous) == 0 {
		return nil
	}

	slices.Sort(ambiguous)
	return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "vstreamclient: ExcludeKeyspaceFromTableName cannot be used with same-named tables across keyspaces: %s", strings.Join(ambiguous, "; "))
}
