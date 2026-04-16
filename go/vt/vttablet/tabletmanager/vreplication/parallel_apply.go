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
	"maps"
	"math"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/replication"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/binlog/binlogplayer"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/throttle/throttlerapp"

	binlogdatapb "vitess.io/vitess/go/vt/proto/binlogdata"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

var (
	workerLoopTestHookAfterSend         func(*applyTxn)
	workerLoopTestHookBeforeWaitPending func(chan struct{})
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

type postDDLStalePlan struct {
	stalePlan      *TablePlan
	refreshedPlans map[string]*TablePlan
	allowDisappear bool
}

// clonePostDDLStalePlan deep-copies the refreshed-name set so scheduler and
// commitLoop state can evolve independently without sharing inner maps.
func clonePostDDLStalePlan(stale postDDLStalePlan) postDDLStalePlan {
	clone := stale
	if len(stale.refreshedPlans) == 0 {
		return clone
	}
	clone.refreshedPlans = make(map[string]*TablePlan, len(stale.refreshedPlans))
	maps.Copy(clone.refreshedPlans, stale.refreshedPlans)
	return clone
}

// clonePostDDLStalePlans returns a detached copy of the current barrier state.
func clonePostDDLStalePlans(src map[string]postDDLStalePlan) map[string]postDDLStalePlan {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[string]postDDLStalePlan, len(src))
	for name, stale := range src {
		cloned[name] = clonePostDDLStalePlan(stale)
	}
	return cloned
}

func cloneDroppedTables(src map[string]struct{}) map[string]struct{} {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[string]struct{}, len(src))
	for name := range src {
		cloned[name] = struct{}{}
	}
	return cloned
}

func canonicalPostDDLTableKey[T any](entries map[string]T, name string) string {
	if name == "" {
		return ""
	}
	if _, ok := entries[name]; ok {
		return name
	}
	for candidate := range entries {
		if strings.EqualFold(candidate, name) {
			return candidate
		}
	}
	return name
}

func postDDLTableKeyMatches(a, b string) bool {
	return a != "" && b != "" && strings.EqualFold(a, b)
}

// snapshotPostDDLStalePlans widens an unknown-DDL barrier to all currently
// live plans except names already known to have been dropped.
func snapshotPostDDLStalePlans(tablePlans map[string]*TablePlan, droppedTables map[string]struct{}) map[string]postDDLStalePlan {
	if len(tablePlans) == 0 {
		return nil
	}
	tracked := make(map[string]postDDLStalePlan, len(tablePlans))
	for name, plan := range tablePlans {
		if _, dropped := droppedTables[canonicalPostDDLTableKey(droppedTables, name)]; dropped {
			continue
		}
		tracked[name] = postDDLStalePlan{
			stalePlan:      plan,
			refreshedPlans: map[string]*TablePlan{name: plan},
		}
	}
	if len(tracked) == 0 {
		return nil
	}
	return tracked
}

func addPostDDLStalePlan(tracked map[string]postDDLStalePlan, tablePlans map[string]*TablePlan, droppedTables map[string]struct{}, allowDroppedRefreshedNames bool, staleName string, refreshedNames ...string) {
	staleName = canonicalPostDDLTableKey(tablePlans, staleName)
	if _, dropped := droppedTables[canonicalPostDDLTableKey(droppedTables, staleName)]; dropped {
		return
	}
	plan, ok := tablePlans[staleName]
	if !ok {
		return
	}
	entry := postDDLStalePlan{
		stalePlan:      plan,
		refreshedPlans: make(map[string]*TablePlan, len(refreshedNames)),
	}
	for _, refreshedName := range refreshedNames {
		if refreshedName == "" {
			continue
		}
		refreshedName = canonicalPostDDLTableKey(tablePlans, refreshedName)
		if !allowDroppedRefreshedNames && refreshedName != staleName {
			if _, dropped := droppedTables[canonicalPostDDLTableKey(droppedTables, refreshedName)]; dropped {
				continue
			}
		}
		if tablePlans == nil {
			continue
		}
		entry.refreshedPlans[refreshedName] = tablePlans[refreshedName]
	}
	if len(entry.refreshedPlans) == 0 {
		return
	}
	tracked[staleName] = entry
}

// extractDDLAffectedTables parses a DDL statement and returns the tracked stale
// plans plus the table names whose future FIELD refresh can satisfy each entry.
// The caller uses this to keep the stale-plan barrier scoped to the plans that
// actually matter for the DDL, including rename operations where the refreshed
// FIELD arrives under a different table name.
func extractDDLAffectedTables(sql string, parser *sqlparser.Parser, tablePlans map[string]*TablePlan, droppedTables map[string]struct{}) (map[string]postDDLStalePlan, bool) {
	stmt, err := parser.ParseStrictDDL(sql)
	if err != nil {
		tracked := snapshotPostDDLStalePlans(tablePlans, droppedTables)
		return tracked, len(tracked) != 0
	}
	ddlStmt, ok := stmt.(sqlparser.DDLStatement)
	if !ok {
		tracked := snapshotPostDDLStalePlans(tablePlans, droppedTables)
		return tracked, len(tracked) != 0
	}
	tracked := make(map[string]postDDLStalePlan)
	switch stmt := ddlStmt.(type) {
	case *sqlparser.CreateTable:
		// A same-name recreate can arrive before FIELD refreshes the live plan.
		// Keep the stale pre-drop plan tracked until the new FIELD replaces it.
		addPostDDLStalePlan(tracked, tablePlans, nil, true, stmt.Table.Name.String(), stmt.Table.Name.String())
	case *sqlparser.RenameTable:
		for _, pair := range stmt.TablePairs {
			addPostDDLStalePlan(tracked, tablePlans, droppedTables, true, pair.FromTable.Name.String(), pair.ToTable.Name.String())
		}
	case *sqlparser.AlterTable:
		refreshedNames := []string{stmt.Table.Name.String()}
		allowDroppedRefreshedNames := false
		for _, option := range stmt.AlterOptions {
			if rename, ok := option.(*sqlparser.RenameTableName); ok {
				refreshedNames = []string{rename.Table.Name.String()}
				allowDroppedRefreshedNames = true
			}
		}
		addPostDDLStalePlan(tracked, tablePlans, droppedTables, allowDroppedRefreshedNames, stmt.Table.Name.String(), refreshedNames...)
	case *sqlparser.DropTable:
		for _, table := range stmt.FromTables {
			if table.IsEmpty() {
				continue
			}
			name := canonicalPostDDLTableKey(tablePlans, table.Name.String())
			addPostDDLStalePlan(tracked, tablePlans, nil, true, name, name)
			entry := tracked[name]
			entry.allowDisappear = true
			tracked[name] = entry
		}
	default:
		for _, table := range ddlStmt.AffectedTables() {
			if table.IsEmpty() {
				continue
			}
			name := table.Name.String()
			addPostDDLStalePlan(tracked, tablePlans, droppedTables, false, name, name)
		}
	}
	if len(tracked) == 0 {
		return nil, false
	}
	return tracked, false
}

func extractDroppedTables(sql string, parser *sqlparser.Parser) map[string]struct{} {
	stmt, err := parser.ParseStrictDDL(sql)
	if err != nil {
		return nil
	}
	dropped := map[string]struct{}{}
	switch stmt := stmt.(type) {
	case *sqlparser.DropTable:
		for _, table := range stmt.FromTables {
			if table.IsEmpty() {
				continue
			}
			dropped[strings.ToLower(table.Name.String())] = struct{}{}
		}
	}
	if len(dropped) == 0 {
		return nil
	}
	return dropped
}

// retireResolvedPostDDLTablePlans removes stale rename-source plans once the
// rename barrier is fully satisfied. This keeps later unknown-DDL snapshots
// from tracking names that no longer exist, while preserving fail-closed
// behavior until the rename target actually refreshes.
func retireResolvedPostDDLTablePlans(tablePlans map[string]*TablePlan, stalePlans map[string]postDDLStalePlan) bool {
	retired := false
	for staleName, stale := range stalePlans {
		if stale.stalePlan == nil {
			continue
		}
		if _, recreated := stale.refreshedPlans[staleName]; recreated {
			continue
		}
		if tablePlans[staleName] != stale.stalePlan {
			continue
		}
		delete(tablePlans, staleName)
		retired = true
	}
	return retired
}

func resolvedPostDDLStalePlans(tablePlans map[string]*TablePlan, droppedTables map[string]struct{}, stalePlans map[string]postDDLStalePlan) map[string]postDDLStalePlan {
	if len(stalePlans) == 0 {
		return nil
	}
	resolved := make(map[string]postDDLStalePlan, len(stalePlans))
	for name, stale := range stalePlans {
		if stale.allowDisappear {
			if _, ok := droppedTables[canonicalPostDDLTableKey(droppedTables, name)]; ok {
				resolved[name] = clonePostDDLStalePlan(stale)
				continue
			}
		}
		allRefreshed := true
		for refreshedName, priorPlan := range stale.refreshedPlans {
			refreshedPlan, ok := tablePlans[canonicalPostDDLTableKey(tablePlans, refreshedName)]
			if !ok || refreshedPlan == priorPlan {
				allRefreshed = false
				break
			}
		}
		if allRefreshed {
			resolved[name] = clonePostDDLStalePlan(stale)
		}
	}
	if len(resolved) == 0 {
		return nil
	}
	return resolved
}

func mergePostDDLStalePlans(dst, src map[string]postDDLStalePlan) map[string]postDDLStalePlan {
	if len(src) == 0 {
		return dst
	}
	if dst == nil {
		dst = make(map[string]postDDLStalePlan, len(src))
	}
	for name, stale := range src {
		existing, ok := dst[name]
		if !ok {
			dst[name] = clonePostDDLStalePlan(stale)
			continue
		}
		merged := existing
		if merged.stalePlan == nil {
			merged.stalePlan = stale.stalePlan
		}
		if len(stale.refreshedPlans) != 0 {
			if merged.refreshedPlans == nil {
				merged.refreshedPlans = make(map[string]*TablePlan, len(existing.refreshedPlans)+len(stale.refreshedPlans))
				maps.Copy(merged.refreshedPlans, existing.refreshedPlans)
			}
			for refreshedName, refreshedPlan := range stale.refreshedPlans {
				if _, ok := merged.refreshedPlans[refreshedName]; !ok {
					merged.refreshedPlans[refreshedName] = refreshedPlan
				}
			}
		}
		merged.allowDisappear = merged.allowDisappear || stale.allowDisappear
		dst[name] = merged
	}
	return dst
}

func extractDDLRenameTargets(sql string, parser *sqlparser.Parser) map[string]string {
	stmt, err := parser.ParseStrictDDL(sql)
	if err != nil {
		return nil
	}
	renames := map[string]string{}
	switch stmt := stmt.(type) {
	case *sqlparser.RenameTable:
		for _, pair := range stmt.TablePairs {
			fromName := strings.ToLower(pair.FromTable.Name.String())
			toName := strings.ToLower(pair.ToTable.Name.String())
			if fromName == "" || toName == "" {
				continue
			}
			renames[fromName] = toName
		}
	case *sqlparser.AlterTable:
		for _, option := range stmt.AlterOptions {
			rename, ok := option.(*sqlparser.RenameTableName)
			if !ok {
				continue
			}
			fromName := strings.ToLower(stmt.Table.Name.String())
			toName := strings.ToLower(rename.Table.Name.String())
			if fromName == "" || toName == "" {
				continue
			}
			renames[fromName] = toName
		}
	}
	if len(renames) == 0 {
		return nil
	}
	return renames
}

func shouldPublishExecIgnoreDDLBarrier(ctx context.Context, vp *vplayer, statement string) (bool, error) {
	if vp == nil || vp.vr == nil || vp.vr.vre == nil || vp.vr.vre.env == nil {
		return false, nil
	}
	parser := vp.vr.vre.env.Parser()
	stmt, err := parser.ParseStrictDDL(statement)
	if err != nil {
		return false, nil
	}
	alter, ok := stmt.(*sqlparser.AlterTable)
	if !ok || alter.Table.IsEmpty() {
		return false, nil
	}
	tableName := alter.Table.Name.String()
	vp.tablePlansMu.RLock()
	cachedPlan := vp.tablePlans[canonicalPostDDLTableKey(vp.tablePlans, tableName)]
	vp.tablePlansMu.RUnlock()
	if cachedPlan == nil {
		return false, nil
	}
	for _, option := range alter.AlterOptions {
		switch option := option.(type) {
		case *sqlparser.AddIndexDefinition:
			if option.IndexDefinition == nil || option.IndexDefinition.Info == nil || !option.IndexDefinition.Info.IsUnique() {
				continue
			}
			hasExtraUniqueSecondary, err := vp.vr.hasExtraUniqueSecondaryIndex(ctx, cachedPlan.TargetName, cachedPlan)
			if err != nil {
				return false, err
			}
			return hasExtraUniqueSecondary != cachedPlan.HasExtraUniqueSecondary, nil
		case *sqlparser.DropKey:
			if option.Type != sqlparser.NormalKeyType && option.Type != sqlparser.ConstraintType {
				continue
			}
			hasExtraUniqueSecondary, err := vp.vr.hasExtraUniqueSecondaryIndex(ctx, cachedPlan.TargetName, cachedPlan)
			if err != nil {
				return false, err
			}
			return hasExtraUniqueSecondary != cachedPlan.HasExtraUniqueSecondary, nil
		}
	}
	return false, nil
}

func retargetPostDDLStalePlans(stalePlans map[string]postDDLStalePlan, renameTargets map[string]string, tablePlans map[string]*TablePlan) {
	if len(stalePlans) == 0 || len(renameTargets) == 0 {
		return
	}
	for staleName, stale := range stalePlans {
		if len(stale.refreshedPlans) == 0 {
			continue
		}
		refreshedPlans := make(map[string]*TablePlan, len(stale.refreshedPlans))
		changed := false
		for refreshedName, priorPlan := range stale.refreshedPlans {
			if toName, ok := renameTargets[canonicalPostDDLTableKey(renameTargets, refreshedName)]; ok {
				// Retarget from the original watched names only so overlapping
				// rename sets do not cascade based on map iteration order.
				toName = canonicalPostDDLTableKey(tablePlans, toName)
				refreshedPlans[toName] = tablePlans[toName]
				changed = true
				continue
			}
			refreshedPlans[refreshedName] = priorPlan
		}
		if !changed {
			continue
		}
		stale.refreshedPlans = refreshedPlans
		stalePlans[staleName] = stale
	}
}

// unresolvedPostDDLStalePlans drops entries whose replacement FIELD has already
// arrived, so only still-stale table plans participate in later scheduling.
func unresolvedPostDDLStalePlans(tablePlans map[string]*TablePlan, droppedTables map[string]struct{}, stalePlans map[string]postDDLStalePlan) map[string]postDDLStalePlan {
	if len(stalePlans) == 0 {
		return nil
	}
	unresolved := make(map[string]postDDLStalePlan, len(stalePlans))
	for name, stale := range stalePlans {
		if stale.allowDisappear {
			if _, ok := droppedTables[canonicalPostDDLTableKey(droppedTables, name)]; ok {
				continue
			}
		}
		allRefreshed := true
		for refreshedName, priorPlan := range stale.refreshedPlans {
			refreshedPlan, ok := tablePlans[canonicalPostDDLTableKey(tablePlans, refreshedName)]
			if !ok {
				allRefreshed = false
				break
			}
			if refreshedPlan == priorPlan {
				allRefreshed = false
				break
			}
		}
		if allRefreshed {
			continue
		}
		unresolved[name] = clonePostDDLStalePlan(stale)
	}
	if len(unresolved) == 0 {
		return nil
	}
	return unresolved
}

// txnTouchesPostDDLBarrier keeps known DDL barriers table-scoped while still
// letting unknown DDLs remain conservative until every tracked plan refreshes.
func txnTouchesPostDDLBarrier(events []*binlogdatapb.VEvent, stalePlans map[string]postDDLStalePlan, conservative bool) bool {
	if len(stalePlans) == 0 {
		return false
	}
	for _, event := range events {
		var tableName string
		switch event.Type {
		case binlogdatapb.VEventType_FIELD:
			if event.FieldEvent != nil {
				tableName = event.FieldEvent.TableName
			}
		case binlogdatapb.VEventType_ROW:
			if event.RowEvent != nil {
				tableName = event.RowEvent.TableName
			}
		}
		if tableName == "" {
			continue
		}
		if conservative {
			return true
		}
		for staleName, stale := range stalePlans {
			if postDDLTableKeyMatches(tableName, staleName) {
				return true
			}
			for refreshedName := range stale.refreshedPlans {
				if postDDLTableKeyMatches(tableName, refreshedName) {
					return true
				}
			}
		}
	}
	return false
}

func postDDLRefreshTargetMatchesCachedPlan(stalePlans map[string]postDDLStalePlan, refreshedName string, cachedPlan *TablePlan) bool {
	for _, stale := range stalePlans {
		for trackedName, priorPlan := range stale.refreshedPlans {
			if !postDDLTableKeyMatches(trackedName, refreshedName) {
				continue
			}
			if priorPlan == cachedPlan {
				return true
			}
		}
	}
	return false
}

func mergeDroppedTables(dst, src map[string]struct{}) map[string]struct{} {
	if len(src) == 0 {
		return dst
	}
	merged := make(map[string]struct{}, len(dst)+len(src))
	for name := range dst {
		merged[name] = struct{}{}
	}
	for name := range src {
		merged[name] = struct{}{}
	}
	return merged
}

func extractFieldRefreshTables(events []*binlogdatapb.VEvent) map[string]struct{} {
	var refreshed map[string]struct{}
	for _, event := range events {
		if event.Type != binlogdatapb.VEventType_FIELD || event.FieldEvent == nil || event.FieldEvent.TableName == "" {
			continue
		}
		if refreshed == nil {
			refreshed = make(map[string]struct{})
		}
		refreshed[event.FieldEvent.TableName] = struct{}{}
	}
	return refreshed
}

// Same-txn FIELD+ROW means the worker may build or replace the plan before it
// applies the row. The scheduler cannot safely compute a writeset from the
// pre-apply snapshot, so serialize these rare refresh transactions.
func txnNeedsFieldRefreshSerialization(events []*binlogdatapb.VEvent) bool {
	refreshedTables := extractFieldRefreshTables(events)
	if len(refreshedTables) == 0 {
		return false
	}
	for _, event := range events {
		if event.Type != binlogdatapb.VEventType_ROW || event.RowEvent == nil {
			continue
		}
		if _, ok := refreshedTables[event.RowEvent.TableName]; ok {
			return true
		}
	}
	return false
}

func txnTouchesPendingFieldRefresh(events []*binlogdatapb.VEvent, pending map[string]int) bool {
	if len(pending) == 0 {
		return false
	}
	for _, event := range events {
		if event.Type != binlogdatapb.VEventType_ROW || event.RowEvent == nil {
			continue
		}
		for tableName, count := range pending {
			if count <= 0 {
				continue
			}
			if postDDLTableKeyMatches(event.RowEvent.TableName, tableName) {
				return true
			}
		}
	}
	return false
}

func workerLocalVPlayer(vp *vplayer) vplayer {
	return vplayer{
		vr:                  vp.vr,
		copyState:           vp.copyState,
		replicatorPlan:      vp.replicatorPlan,
		canAcceptStmtEvents: vp.canAcceptStmtEvents,
		tablePlansMu:        vp.tablePlansMu,
		tablePlans:          vp.tablePlans,
		tablePlansVersion:   vp.tablePlansVersion,
		batchMode:           vp.batchMode,
		phase:               vp.phase,
		serialMu:            vp.serialMu,
	}
}

func writesetErrorForcesSerialization(err error) bool {
	if vterrors.Code(err) != vtrpcpb.Code_FAILED_PRECONDITION {
		return false
	}
	return strings.Contains(err.Error(), "partial row image on table ") ||
		strings.Contains(err.Error(), "not in streamed fields")
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
	if err := ctx.Err(); err != nil {
		return err
	}

	scheduler := newApplyScheduler(ctx)
	// Buffer 4x worker count to decouple worker throughput from commit
	// latency. Workers block when commitCh is full, stalling the pipeline.
	commitCh := make(chan *applyTxn, workerCount*4)
	applyErr := make(chan error, 2)
	commitLoopErr := make(chan error, 1)
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
	//
	// Fail closed: if we can't read FK metadata we cannot know whether
	// the schema has FK constraints that require cross-table ordering.
	// Continuing with fkRefs=nil would silently degrade to PK-only
	// writeset scheduling and could reorder parent/child transactions.
	// Return the error so the workflow retries via the normal retry path.
	fkRefs, err := queryFKRefs(vp.vr.dbClient, vp.vr.dbClient.DBName())
	if err != nil {
		return vterrors.Wrapf(err, "parallel apply: failed to query FK metadata from %q", vp.vr.dbClient.DBName())
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
			commitLoopErr <- err
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
	}

	scheduler.close()
	wg.Wait()
	close(commitCh)
	<-commitDone
	select {
	case err := <-commitLoopErr:
		if err == io.EOF {
			return nil
		}
		applyErr <- err
	default:
	}

	// Now that commitLoop is done, it's safe to rollback any leftover
	// transaction on the main connection. This must happen after commitDone
	// because commitOnlyTxn in the commitLoop also uses the main connection.
	vp.vr.dbClient.Rollback()

	// Drain all errors and prioritize real failures over io.EOF/context.Canceled.
	// We must always inspect workerErr too: teardown after a worker failure makes
	// scheduleLoop/commitLoop commonly return io.EOF/context.Canceled, and those
	// benign shutdown signals must not mask the original worker error.
	var realErrs []error
	var hasEOF bool
	var hasCanceled bool
	classifyErr := func(err error) {
		if err == nil {
			return
		}
		if err == io.EOF {
			hasEOF = true
			return
		}
		if errors.Is(err, context.Canceled) {
			hasCanceled = true
			return
		}
		realErrs = append(realErrs, err)
	}
drainApplyErrs:
	for {
		select {
		case err := <-applyErr:
			classifyErr(err)
		default:
			break drainApplyErrs
		}
	}
drainWorkerErrs:
	for {
		select {
		case err := <-workerErr:
			classifyErr(err)
		default:
			break drainWorkerErrs
		}
	}
	if len(realErrs) > 0 {
		return errors.Join(realErrs...)
	}
	// Convert io.EOF (stop position reached) and context.Canceled (shutdown)
	// to nil. fetchAndApply's caller treats nil from applyEventsParallel
	// the same as io.EOF from the serial path — it stops the controller
	// without retrying.
	if hasEOF || hasCanceled {
		return nil
	}
	return nil
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
			if err := vp.enqueueCommitOnly(ctx, scheduler, event, true, true, 0, 0, false); err != nil {
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
		//
		// Also drain when the post-DDL stale-plan guard is active. This
		// ensures that all serialized work (including FIELD events for the
		// DDL-affected table) has been applied by workers before the next
		// fetch's scheduleItems re-evaluates the guard by comparing plan
		// pointers.
		if state.ddlSeen || state.postDDLStalePlans != nil {
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
	// batchMissingCommitMeta is sticky across batched source transactions.
	// Once a merged batch contains any txn without commit metadata, the
	// flushed mega-transaction must stay in the no-metadata scheduler mode.
	batchMissingCommitMeta bool
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
	// postDDLStalePlans records a snapshot of the tablePlans entries at the
	// time an executed DDL was observed. Parallel scheduling is force-
	// serialized as long as any plan in this snapshot is still the same
	// object in the live tablePlans map. When a FIELD event for the DDL-
	// affected table arrives, vplayer.applyEvent builds a new *TablePlan
	// and stores it in tablePlans, replacing the stale pointer. At that
	// point the guard clears for that table.
	//
	// This is per-table rather than global-version-based because vstreamer
	// only emits FIELD on first-seen or remapped table ids. An unrelated
	// table's FIELD would bump the global tablePlansVersion but not replace
	// the DDL-affected table's plan pointer.
	//
	// nil means no DDL barrier is active.
	postDDLStalePlans map[string]postDDLStalePlan
	// postDDLDroppedTables records dropped tables that have been explicitly
	// satisfied for the current DDL barrier.
	postDDLDroppedTables map[string]struct{}
	// postDDLConservative marks barriers from unparsed DDL, where we must keep
	// serializing tracked-table transactions until every captured plan refreshes.
	postDDLConservative bool
}

// scheduleItems processes one relay log fetch worth of event batches. It tracks
// transaction boundaries (GTID → events → COMMIT), classifies transactions,
// builds writesets, handles batching of consecutive commits, and enqueues
// applyTxn structs into the scheduler. Empty transactions bypass the scheduler
// and are saved via unsavedEvent / idle timeout.
func (vp *vplayer) scheduleItems(ctx context.Context, scheduler *applyScheduler, state *parallelScheduleState, items [][]*binlogdatapb.VEvent) error {
	stopPosReached := func(pos replication.Position) bool {
		return !vp.stopPos.IsZero() && !pos.IsZero() && pos.AtLeast(vp.stopPos)
	}
	journalTerminates := func(event *binlogdatapb.VEvent) bool {
		if event.Type != binlogdatapb.VEventType_JOURNAL || event.Journal == nil {
			return false
		}
		if event.Journal.MigrationType != binlogdatapb.MigrationType_TABLES {
			return true
		}
		jtables := make(map[string]struct{}, len(event.Journal.Tables))
		for _, table := range event.Journal.Tables {
			jtables[table] = struct{}{}
		}
		found := false
		notFound := false
		for tableName := range vp.replicatorPlan.TablePlans {
			if _, ok := jtables[tableName]; ok {
				found = true
			} else {
				notFound = true
			}
		}
		switch {
		case found && notFound:
			return true
		case notFound:
			return false
		default:
			return true
		}
	}
	ddlTerminates := func(event *binlogdatapb.VEvent) bool {
		return event.Type == binlogdatapb.VEventType_DDL && vp.vr.source.OnDdl == binlogdatapb.OnDDLAction_STOP
	}

	// Snapshot FK refs under serialMu so we have a consistent view for this
	// relay fetch. The commitLoop may update these after DDL events.
	vp.serialMu.Lock()
	fkRefs := vp.fkRefs
	parentFKRefs := vp.parentFKRefs
	pendingFieldRefreshTables := maps.Clone(vp.pendingFieldRefreshTables)
	state.postDDLDroppedTables = cloneDroppedTables(vp.postDDLDroppedTables)
	if len(vp.postDDLStalePlans) != 0 {
		if state.postDDLStalePlans == nil {
			state.postDDLStalePlans = make(map[string]postDDLStalePlan, len(vp.postDDLStalePlans))
		}
		for name, stale := range vp.postDDLStalePlans {
			state.postDDLStalePlans[name] = clonePostDDLStalePlan(stale)
		}
	}
	state.postDDLConservative = state.postDDLConservative || vp.postDDLConservative
	vp.serialMu.Unlock()

	// After DDL events that may change schema or FK topology, force all
	// remaining transactions in this relay fetch to serialize. The
	// commitLoop will refresh FK metadata when the DDL commits, so the
	// next relay fetch will have updated snapshots.
	//
	// If a previous DDL that was executed on the target changed the schema,
	// force-serialize until the DDL-affected table's plan pointer has been
	// replaced by a new FIELD event. We check by comparing the live
	// tablePlans pointers to the snapshot taken at DDL time. When the
	// affected table's plan is replaced (new *TablePlan from FIELD), the
	// stale pointer no longer matches and the guard clears.
	forceSerialize := false
	if state.postDDLStalePlans != nil {
		// Keep serializing until every affected table plan captured at DDL time
		// has been replaced. Unrelated FIELD events must not clear the barrier.
		vp.tablePlansMu.Lock()
		resolvedStalePlans := resolvedPostDDLStalePlans(vp.tablePlans, state.postDDLDroppedTables, state.postDDLStalePlans)
		if retireResolvedPostDDLTablePlans(vp.tablePlans, resolvedStalePlans) {
			vp.tablePlansVersion.Add(1)
		}
		state.postDDLStalePlans = unresolvedPostDDLStalePlans(vp.tablePlans, state.postDDLDroppedTables, state.postDDLStalePlans)
		vp.tablePlansMu.Unlock()
		if state.postDDLStalePlans == nil {
			state.postDDLStalePlans = nil
			state.postDDLDroppedTables = nil
			state.postDDLConservative = false
			vp.serialMu.Lock()
			vp.postDDLStalePlans = nil
			vp.postDDLConservative = false
			vp.serialMu.Unlock()
		}
	}
	state.ddlSeen = false
	var fkBatchingResolvedTables map[string]struct{}
	fkBatchingResolvedVersion := int64(-1)
	writesetCache := &txnWritesetCache{fieldIdxCache: state.fieldIdxCache}
	getFKBatchingSnapshot := func() (map[string]*TablePlan, map[string]struct{}) {
		planSnapshot := snapshotTablePlans(vp.tablePlansMu, vp.tablePlans, vp.tablePlansVersion, &state.cachedPlanVersion, state.cachedPlanSnapshot)
		state.cachedPlanSnapshot = planSnapshot
		if fkBatchingResolvedVersion == state.cachedPlanVersion {
			return planSnapshot, fkBatchingResolvedTables
		}
		fkBatchingResolvedTables = buildResolvedFKRefTableSet(fkRefs, parentFKRefs, buildCanonicalTargetTableNames(planSnapshot))
		fkBatchingResolvedVersion = state.cachedPlanVersion
		return planSnapshot, fkBatchingResolvedTables
	}

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
		if !state.batchMissingCommitMeta {
			txn.sequenceNumber = state.curSequence
			txn.commitParent = state.curCommitParent
			txn.hasCommitMeta = state.curHasCommitMeta
		}
		txn.payload = payload
		postDDLSerialize := state.postDDLStalePlans != nil && txnTouchesPostDDLBarrier(state.curEvents, state.postDDLStalePlans, state.postDDLConservative)
		if forceSerialize {
			txn.forceGlobal = true
		} else if postDDLSerialize {
			txn.forceGlobal = true
		} else if state.curRowOnlySet && !state.curRowOnly {
			txn.forceGlobal = true
		} else if len(vp.copyState) != 0 {
			txn.forceGlobal = true
		} else if txnNeedsFieldRefreshSerialization(state.curEvents) {
			txn.forceGlobal = true
		} else if txnTouchesPendingFieldRefresh(state.curEvents, pendingFieldRefreshTables) {
			txn.forceGlobal = true
		} else {
			planSnapshot := snapshotTablePlans(vp.tablePlansMu, vp.tablePlans, vp.tablePlansVersion, &state.cachedPlanVersion, state.cachedPlanSnapshot)
			state.cachedPlanSnapshot = planSnapshot
			if txnTouchesExtraUniqueSecondary(state.curEvents, planSnapshot) || txnTouchesUnsupportedWritesetMapping(state.curEvents, planSnapshot) {
				txn.forceGlobal = true
			} else {
				// Invalidate fieldIdxCache when table plans change (new FIELD events).
				if state.fieldIdxCacheVersion != state.cachedPlanVersion {
					state.fieldIdxCache = make(map[string]map[string]int)
					state.fieldIdxCacheVersion = state.cachedPlanVersion
					writesetCache = &txnWritesetCache{fieldIdxCache: state.fieldIdxCache}
				} else if writesetCache == nil {
					writesetCache = &txnWritesetCache{fieldIdxCache: state.fieldIdxCache}
				}
				writeset, err := buildTxnWritesetWithCache(planSnapshot, fkRefs, parentFKRefs, state.curEvents, writesetCache)
				if err != nil {
					if writesetErrorForcesSerialization(err) {
						txn.forceGlobal = true
					} else {
						releaseApplyTxn(txn)
						return err
					}
				} else {
					txn.writeset = writeset
				}
			}
		}
		// Attach any merged-away sequences to the txn so the scheduler can
		// advance lastCommittedSequence for them when this batch actually
		// commits (inside markCommitted), not now at enqueue time. Advancing
		// at enqueue would satisfy commit-parent dependencies for later
		// empty-writeset txns before the batch containing those sequences
		// has actually committed.
		if len(state.mergedSequences) > 0 {
			txn.mergedSequences = append(txn.mergedSequences[:0], state.mergedSequences...)
			state.mergedSequences = state.mergedSequences[:0]
		}
		if state.batchMissingCommitMeta && state.curHasCommitMeta && state.curSequence > 0 {
			txn.mergedSequences = append(txn.mergedSequences, state.curSequence)
		}
		if err := scheduler.enqueue(txn); err != nil {
			return err
		}
		if refreshedTables := extractFieldRefreshTables(payload.events); len(refreshedTables) != 0 {
			if pendingFieldRefreshTables == nil {
				pendingFieldRefreshTables = make(map[string]int, len(refreshedTables))
			}
			vp.serialMu.Lock()
			if vp.pendingFieldRefreshTables == nil {
				vp.pendingFieldRefreshTables = make(map[string]int, len(refreshedTables))
			}
			for tableName := range refreshedTables {
				pendingFieldRefreshTables[tableName]++
				vp.pendingFieldRefreshTables[tableName]++
			}
			vp.serialMu.Unlock()
		}
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
		state.batchMissingCommitMeta = false
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
				if !state.curHasCommitMeta {
					state.batchMissingCommitMeta = true
				}
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
				posReached := stopPosReached(state.curPos)
				state.curMustSave = posReached
				if len(state.curEvents) == 0 {
					if state.curMustSave {
						eventCopy := event
						if err := vp.enqueueCommitOnly(ctx, scheduler, eventCopy, true, true, state.curSequence, state.curCommitParent, state.curHasCommitMeta); err != nil {
							return err
						}
						return io.EOF
					}

					now := time.Now()
					queuePositionSave := false
					vp.serialMu.Lock()
					if time.Since(vp.timeLastSaved) >= idleTimeout {
						vp.timeLastSaved = now
						queuePositionSave = true
					}
					vp.serialMu.Unlock()
					if queuePositionSave {
						state.lastHeartbeatRefresh = now
						eventCopy := event
						if err := vp.enqueueCommitOnly(ctx, scheduler, eventCopy, true, true, state.curSequence, state.curCommitParent, state.curHasCommitMeta); err != nil {
							return err
						}
					} else {
						// During catch-up, a stream may continuously process
						// empty transactions (from other shards' data) that
						// keep the scheduleLoop busy, so the idle timeout at
						// the top of the loop never fires. Periodically refresh
						// time_updated directly via SQL to keep
						// max_v_replication_lag fresh until the next ordered
						// position save is queued.
						needRefresh := time.Since(state.lastHeartbeatRefresh) >= idleTimeout
						if needRefresh {
							state.lastHeartbeatRefresh = now
							vp.serialMu.Lock()
							err := vp.vr.updateHeartbeatTime(now.Unix())
							vp.serialMu.Unlock()
							if err != nil {
								return err
							}
						}
						vp.serialMu.Lock()
						vp.unsavedEvent = event
						vp.serialMu.Unlock()
						// Advance lastCommittedSequence immediately only when the
						// empty transaction stays on the unsavedEvent path.
						// Queued position saves publish their sequence on commit.
						if state.curHasCommitMeta {
							scheduler.advanceCommittedSequence(state.curSequence)
						}
					}
					// Advance lastCommittedSequence immediately for this empty
					// transaction. Empty txns have no data effects, so their
					// commit-parent dependency is trivially satisfied. Deferring
					// the advance (via mergedSequences) would deadlock: a later
					// txn with commitParent=thisSequence and empty writeset would
					// block forever waiting for lastCommittedSequence to reach
					// its commitParent, but the deferred sequence only publishes
					// when that later txn commits — a circular dependency.
					//
					// markCommitted() uses max() for lastCommittedSequence, so
					// this early advance cannot regress the watermark when a
					// later txn commits with a lower sequence number.
					state.curEvents = make([]*binlogdatapb.VEvent, 0, 16)
					state.curRowOnly = false
					state.curRowOnlySet = false
					state.curMustSave = false
					state.curTimestamp = 0
					state.curCommitParent = 0
					state.curSequence = 0
					state.curHasCommitMeta = false
					state.batchMissingCommitMeta = false
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
				// When the current transaction touches FK-related tables,
				// skip batching to keep writesets small. Merging parent/
				// child operations into one mega-transaction would make
				// nearly all batches conflict on FK ref keys, serializing
				// the workload. Flushing each source transaction
				// individually lets the scheduler detect truly independent
				// transactions and run them in parallel. Transactions on
				// unrelated tables can still batch normally.
				hasFKRefs := false
				if len(fkRefs) > 0 || len(parentFKRefs) > 0 {
					planSnapshot, resolvedFKRefTables := getFKBatchingSnapshot()
					for _, ev := range state.curEvents {
						if ev.Type != binlogdatapb.VEventType_ROW || ev.RowEvent == nil {
							continue
						}
						tableName := ev.RowEvent.TableName
						if plan := planSnapshot[tableName]; plan != nil {
							tableName = plan.TargetName
						}
						if _, ok := resolvedFKRefTables[tableName]; ok {
							hasFKRefs = true
							break
						}
					}
				}
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
				if posReached {
					return io.EOF
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
			case binlogdatapb.VEventType_INSERT,
				binlogdatapb.VEventType_DELETE,
				binlogdatapb.VEventType_UPDATE,
				binlogdatapb.VEventType_REPLACE,
				binlogdatapb.VEventType_SAVEPOINT:
				// Statement-based DML events are supported for external MySQL
				// streams. Keep them in the transaction payload, but classify the
				// transaction as non-row-only so it serializes like the serial
				// applier's statement path.
				state.curEvents = append(state.curEvents, event)
				state.curRowOnly = false
				state.curRowOnlySet = true
			case binlogdatapb.VEventType_DDL, binlogdatapb.VEventType_OTHER, binlogdatapb.VEventType_JOURNAL:
				if err := flush(false); err != nil {
					return err
				}
				posReached := stopPosReached(state.curPos)
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
				txn.sequenceNumber = state.curSequence
				txn.commitParent = state.curCommitParent
				txn.hasCommitMeta = state.curHasCommitMeta
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
				// DDL that is actually executed on the target (EXEC, EXEC_IGNORE)
				// may change schema or FK topology. Force all remaining
				// transactions in this relay fetch to serialize so they don't
				// use stale FK refs or table plans for writeset computation.
				// Also set state.ddlSeen so the scheduleLoop waits for the
				// commitLoop to refresh FK metadata before the next fetch.
				// Record the current tablePlansVersion so that force-
				// serialization persists until workers apply new FIELD events
				// that bump the version past this snapshot.
				//
				// IGNORE and STOP DDLs do not modify the target schema, so
				// they don't need the barrier. STOP terminates the workflow
				// entirely. IGNORE just advances the position.
				if event.Type == binlogdatapb.VEventType_DDL &&
					(vp.vr.source.OnDdl == binlogdatapb.OnDDLAction_EXEC ||
						vp.vr.source.OnDdl == binlogdatapb.OnDDLAction_EXEC_IGNORE) {
					forceSerialize = true
					state.ddlSeen = true
				}
				if posReached || journalTerminates(event) || ddlTerminates(event) {
					return io.EOF
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
			case binlogdatapb.VEventType_ROWS_QUERY:
				// Informational only; keep it with the surrounding txn if present.
				// vstreamer emits ROWS_QUERY ahead of the ROW events it describes,
				// so this metadata must not force serialization on its own.
				state.curEvents = append(state.curEvents, event)
			case binlogdatapb.VEventType_VERSION:
				// VERSION is informational only for the applier. Preserve the
				// old serial behavior by ignoring it instead of failing the stream.
			default:
				return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "unsupported vevent type: %v", event.Type)
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
func (vp *vplayer) enqueueCommitOnly(ctx context.Context, scheduler *applyScheduler, event *binlogdatapb.VEvent, mustSave bool, updatePosOnly bool, sequenceNumber int64, commitParent int64, hasCommitMeta bool) error {
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
	txn.sequenceNumber = sequenceNumber
	txn.commitParent = commitParent
	txn.hasCommitMeta = hasCommitMeta
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
	// Workers only apply ROW/FIELD/ROWS_QUERY events. Build a narrow local
	// vplayer view once and refresh the DDL barrier snapshots per transaction
	// under serialMu, instead of racing on a whole-struct shallow copy.
	workerVP := workerLocalVPlayer(vp)

	// pendingDone holds the done channel of the most recently sent worker
	// transaction that the commitLoop may still be committing. We capture
	// only the channel (not the *applyTxn) because the commitLoop returns
	// the applyTxn to the pool after signaling done — if the scheduleLoop
	// reacquires it, it drains the channel, which would cause waitPending
	// to block forever if we were still dereferencing through the txn.
	var pendingDone chan struct{}

	// Test hooks let the regression test freeze the worker at precise points in
	// the handoff without relying on scheduler timing.
	afterSendHook := workerLoopTestHookAfterSend
	beforeWaitPendingHook := workerLoopTestHookBeforeWaitPending

	waitPending := func() error {
		if pendingDone == nil {
			return nil
		}
		if beforeWaitPendingHook != nil {
			beforeWaitPendingHook(pendingDone)
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
			// Forward commitOnly txns (DDL, OTHER, JOURNAL, position saves)
			// to the commitLoop immediately without waiting for any pending
			// worker commit. commitOnly work runs on the main connection, not
			// the worker's connection, so it has no dependency on the prior
			// row txn's commit. The commitLoop enforces strict ordering via
			// nextOrder regardless of when the txn arrives in commitCh.
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
		activeApplyClient := worker.client
		stopInterrupt := context.AfterFunc(ctx, func() {
			if activeApplyClient != nil {
				activeApplyClient.Close()
			}
		})
		vp.serialMu.Lock()
		workerVP.postDDLStalePlans = clonePostDDLStalePlans(vp.postDDLStalePlans)
		workerVP.postDDLDroppedTables = cloneDroppedTables(vp.postDDLDroppedTables)
		vp.serialMu.Unlock()
		for _, event := range payload.events {
			if err := worker.applyEvent(ctx, event, payload.mustSave, &workerVP); err != nil {
				stopInterrupt()
				worker.rollback()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return err
			}
		}
		// In batch mode, flush all buffered SQL statements to MySQL in
		// one multi-statement call. This is the key parallelism point:
		// all workers execute their batches concurrently here, while the
		// commitLoop only needs to do a cheap COMMIT + position update.
		if err := worker.flushWorkerBatch(); err != nil {
			stopInterrupt()
			worker.rollback()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		stopInterrupt()

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

		done := txn.done
		select {
		case commitCh <- txn:
		case <-ctx.Done():
			worker.rollback()
			return ctx.Err()
		}
		if afterSendHook != nil {
			afterSendHook(txn)
		}

		// Capture the done channel BEFORE rotating. The commitLoop may
		// return the txn to the pool after signaling done, and
		// acquireApplyTxn drains the channel on reuse. By holding our
		// own reference, we are immune to that race.
		pendingDone = done

		// Rotate to the spare connection for the next transaction.
		// The commitLoop will commit the current txn on the old connection
		// and signal txn.done when it's safe to reuse.
		worker.rotate()
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
	// position update SQL, optional stop-state update, and commit on the
	// worker's private MySQL connection WITHOUT holding serialMu. This avoids
	// blocking the scheduleLoop during slow MySQL commits.
	commitWorkerTxn := func(txn *applyTxn) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		payload := txn.payload
		queryFn := payload.query
		if queryFn == nil {
			queryFn = vp.query
		}
		commitFn := payload.commit
		if commitFn == nil {
			commitFn = vp.commit
		}
		dbClient := payload.client
		if dbClient == nil {
			dbClient = vp.activeDBClient()
		}

		posReached, err := vp.updatePosWithoutStop(ctx, payload.pos, payload.timestamp, queryFn)
		if err != nil {
			return err
		}
		if posReached {
			if err := vp.setStopPositionStateImmediate(dbClient); err != nil {
				return err
			}
		}
		if err := commitFn(); err != nil {
			return err
		}

		// Briefly lock to update vp state that scheduleLoop reads.
		// Do NOT clear vp.unsavedEvent here: a later empty transaction
		// may have recorded a higher position that hasn't been flushed yet.
		// The idle-timeout saver at the top of scheduleLoop will handle it.
		vp.serialMu.Lock()
		vp.recordPositionSave(payload.pos, false)
		for refreshedName := range extractFieldRefreshTables(payload.events) {
			if vp.pendingFieldRefreshTables != nil {
				key := canonicalPostDDLTableKey(vp.pendingFieldRefreshTables, refreshedName)
				if remaining := vp.pendingFieldRefreshTables[key] - 1; remaining > 0 {
					vp.pendingFieldRefreshTables[key] = remaining
				} else {
					delete(vp.pendingFieldRefreshTables, key)
				}
			}
			delete(vp.postDDLDroppedTables, canonicalPostDDLTableKey(vp.postDDLDroppedTables, refreshedName))
		}
		vp.serialMu.Unlock()

		updateLag(payload)

		// Signal the worker that commit is done so it can reuse its
		// DB connection for the next transaction.
		txn.done <- struct{}{}

		if err := scheduler.markCommitted(txn); err != nil {
			return err
		}

		if posReached {
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
		dbClient := payload.client
		if dbClient == nil {
			dbClient = vp.activeDBClient()
		}
		vp.serialMu.Lock()
		defer vp.serialMu.Unlock()

		if payload.updatePosOnly {
			savePos := payload.pos
			if savePos.IsZero() {
				savePos = vp.pos
			}
			posReached := !savePos.IsZero() && !vp.stopPos.IsZero() && savePos.AtLeast(vp.stopPos)
			if posReached && vp.saveStop {
				if err := dbClient.BeginImmediate(); err != nil {
					return err
				}
				if _, err := dbClient.ExecuteFetch(vp.generateUpdatePosQuery(savePos, payload.timestamp), 1); err != nil {
					return fmt.Errorf("error %v updating position", err)
				}
				if err := vp.setStopPositionStateImmediate(dbClient); err != nil {
					return err
				}
				if err := dbClient.Commit(); err != nil {
					return err
				}
				vp.recordPositionSave(savePos, false)
				updateLag(payload)
				if err := scheduler.markCommitted(txn); err != nil {
					return err
				}
				return io.EOF
			}

			queryFn := payload.query
			if queryFn == nil {
				queryFn = vp.query
			}

			posReached, err := vp.updatePosWithoutStop(ctx, savePos, payload.timestamp, queryFn)
			if err != nil {
				return err
			}
			if payload.timestamp == 0 {
				if err := vp.vr.updateHeartbeatTime(time.Now().Unix()); err != nil {
					return err
				}
			}
			vp.recordPositionSave(savePos, false)
			if posReached {
				if err := vp.setStopPositionState(dbClient); err != nil {
					return err
				}
			}
			updateLag(payload)
			if err := scheduler.markCommitted(txn); err != nil {
				return err
			}
			if posReached {
				return io.EOF
			}
			return nil
		}

		// Temporarily swap pos for the main connection's updatePos call.
		prevPos := vp.pos
		if !payload.pos.IsZero() {
			vp.pos = payload.pos
		}
		defer func() { vp.pos = prevPos }()

		// applyEvent handles position updates internally for DDL, OTHER,
		// and JOURNAL events, and returns io.EOF when the stop position
		// is reached or when a JOURNAL forces termination. We therefore
		// do NOT call updatePos again below — doing so would produce a
		// redundant _vt.vreplication write and create an awkward
		// partial-failure window where applyEvent succeeded but a second
		// position write could fail.
		event := payload.events[0]
		ddlExecuted := false
		publishExecIgnoreDDLBarrier := false
		var terminalErr error
		if event.Type == binlogdatapb.VEventType_DDL {
			var err error
			ddlExecuted, err = vp.applyDDLEvent(ctx, event, nil)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return err
				}
				terminalErr = err
			}
			if !ddlExecuted && vp.vr.source.OnDdl == binlogdatapb.OnDDLAction_EXEC_IGNORE {
				publishExecIgnoreDDLBarrier, err = shouldPublishExecIgnoreDDLBarrier(ctx, vp, event.Statement)
				if err != nil {
					return vterrors.Wrapf(err, "failed to inspect EXEC_IGNORE DDL target schema")
				}
			}
		} else if err := vp.applyEvent(ctx, event, payload.mustSave); err != nil {
			if !errors.Is(err, io.EOF) {
				return err
			}
			terminalErr = err
		}
		// After EXEC DDLs and all EXEC_IGNORE DDLs, refresh FK metadata so
		// that ADD/DROP FOREIGN KEY changes are reflected in subsequent
		// writeset conflict detection. EXEC_IGNORE still advances the stream
		// position after a statement error, and that error can mean the
		// target is already in the post-DDL FK state (for example, dropping
		// a foreign key that's already gone). Continuing with the old FK
		// cache would silently use stale conflict metadata.
		//
		// We hold serialMu for the DB round-trip here. DDL is rare, and
		// the main connection must not be used concurrently by scheduleLoop.
		// Fail fast on refresh errors: stale FK topology after a schema
		// change would silently compromise conflict detection.
		if event.Type == binlogdatapb.VEventType_DDL && (ddlExecuted || vp.vr.source.OnDdl == binlogdatapb.OnDDLAction_EXEC_IGNORE) {
			newRefs, err := queryFKRefs(vp.vr.dbClient, vp.vr.dbClient.DBName())
			if err != nil {
				return vterrors.Wrapf(err, "failed to refresh FK metadata after DDL")
			}
			vp.fkRefs = newRefs
			vp.parentFKRefs = buildParentFKRefs(newRefs)
		}
		if event.Type == binlogdatapb.VEventType_DDL && (ddlExecuted || publishExecIgnoreDDLBarrier) {
			vp.tablePlansMu.RLock()
			renameTargets := extractDDLRenameTargets(event.Statement, vp.vr.vre.env.Parser())
			retargetPostDDLStalePlans(vp.postDDLStalePlans, renameTargets, vp.tablePlans)
			ddlStalePlans, conservative := extractDDLAffectedTables(event.Statement, vp.vr.vre.env.Parser(), vp.tablePlans, vp.postDDLDroppedTables)
			ddlStalePlans = unresolvedPostDDLStalePlans(vp.tablePlans, vp.postDDLDroppedTables, ddlStalePlans)
			vp.tablePlansMu.RUnlock()
			vp.postDDLStalePlans = mergePostDDLStalePlans(vp.postDDLStalePlans, ddlStalePlans)
			vp.postDDLConservative = vp.postDDLConservative || conservative
			vp.postDDLDroppedTables = mergeDroppedTables(vp.postDDLDroppedTables, extractDroppedTables(event.Statement, vp.vr.vre.env.Parser()))
		}
		updateLag(payload)
		if err := scheduler.markCommitted(txn); err != nil {
			return err
		}
		return terminalErr
	}

	commitTxn := func(txn *applyTxn) error {
		if txn.payload.commitOnly {
			return commitOnlyTxn(txn)
		}
		return commitWorkerTxn(txn)
	}

	pending := make(map[int64]*applyTxn)
	nextOrder := int64(1)

	// On error exit, release all remaining pending entries to return pool
	// objects that would otherwise be leaked. We intentionally do NOT
	// signal txn.done here: workers unblock via ctx.Done() instead (the
	// caller cancels the context on commitLoop error). Signaling done
	// would tell the worker its old connection is safe to reuse, but
	// the commit may have failed leaving the connection in a dirty state.
	defer func() {
		for _, txn := range pending {
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
