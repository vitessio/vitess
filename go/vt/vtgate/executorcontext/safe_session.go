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

package executorcontext

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"vitess.io/vitess/go/mysql/datetime"
	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/srvtopo"
	"vitess.io/vitess/go/vt/sysvars"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type (
	// SafeSession is a mutex-protected version of the Session.
	// It is thread-safe if each thread only accesses one shard.
	// (the use pattern is 'Find', if not found, then 'AppendOrUpdate',
	// for a single shard)
	SafeSession struct {
		mu              sync.Mutex
		mustRollback    bool
		autocommitState autocommitState
		commitOrder     vtgatepb.CommitOrder
		savepointState  savepointState
		// rollbackOnPartialExec is set if any DML was successfully
		// executed. If there was a subsequent failure, if we have a savepoint we rollback to that.
		// Otherwise, the transaction is rolled back.
		rollbackOnPartialExec string
		savepointName         string

		// this is a signal that found_rows has already been handled by the primitives,
		// and doesn't have to be updated by the executor
		foundRowsHandled bool

		// queryFromVindex is used to avoid erroring out on multi-db transaction
		// as the query that started a new transaction on the shard belong to a vindex.
		queryFromVindex bool

		logging *ExecuteLogger

		*vtgatepb.Session
	}

	ExecuteLogger struct {
		mu      sync.Mutex
		entries []engine.ExecuteEntry
		lastID  int
		parser  *sqlparser.Parser
	}

	// autocommitState keeps track of whether a single round-trip
	// commit to vttablet is possible. It starts as autocommitable
	// if we started a transaction because of the autocommit flag
	// being set. Otherwise, it starts as notAutocommitable.
	// If execute is recursively called using the same session,
	// like from a vindex, we will already be in a transaction,
	// and this should cause the state to become notAutocommitable.
	//
	// SafeSession lets you request a commit token, which will
	// be issued if the state is autocommitable,
	// implying that no intermediate transactions were started.
	// If so, the state transitions to autocommited, which is terminal.
	// If the token is successfully issued, the caller has to perform
	// the commit. If a token cannot be issued, then a traditional
	// commit has to be performed at the outermost level where
	// the autocommitable transition happened.
	autocommitState int

	// savepointState keeps track of whether savepoints need to be inserted
	// before running the query. This will help us prevent rolling back the
	// entire transaction in case of partial failures, and be closer to MySQL
	// compatibility, by only reverting the changes from the failed statement
	// If execute is recursively called using the same session,
	// like from a vindex, we should not override the savePointState.
	// It is set the first time and is then permanent for the remainder of the query
	// execution. It should not be affected later by transactions starting or not.
	savepointState int
)

const (
	notAutocommittable = autocommitState(iota)
	autocommittable
	autocommitted
)

const (
	savepointStateNotSet = savepointState(iota)
	// savepointNotNeeded - savepoint is not required
	savepointNotNeeded
	// savepointNeeded - savepoint may be required
	savepointNeeded
	// savepointSet - savepoint is set on the session
	savepointSet
	// savepointRollbackSet - rollback to savepoint is set on the session
	savepointRollbackSet
	// savepointRollback - rollback happened on the savepoint
	savepointRollback
)

const TxRollback = "Rollback Transaction"

// NewSafeSession returns a new SafeSession based on the Session
func NewSafeSession(sessn *vtgatepb.Session) *SafeSession {
	if sessn == nil {
		sessn = &vtgatepb.Session{}
	}
	return &SafeSession{Session: sessn}
}

// NewAutocommitSession returns a SafeSession based on the original
// session, but with autocommit enabled.
func NewAutocommitSession(sessn *vtgatepb.Session) *SafeSession {
	newSession := sessn.CloneVT()
	newSession.InTransaction = false
	newSession.ShardSessions = nil
	newSession.PreSessions = nil
	newSession.PostSessions = nil
	newSession.LockSession = nil
	newSession.Autocommit = true
	newSession.Warnings = nil
	return NewSafeSession(newSession)
}

// ResetTx clears the session
func (session *SafeSession) ResetTx() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.resetCommonLocked()
	// If settings pools is enabled on the vttablet.
	// This variable will be true but there will not be a shard session with reserved connection id.
	// So, we should check the shard session and not just this variable.
	if session.Session.InReservedConn {
		allSessions := append(session.ShardSessions, append(session.PreSessions, session.PostSessions...)...)
		for _, ss := range allSessions {
			if ss.ReservedId != 0 {
				// found that reserved connection exists.
				// abort here, we should keep the shard sessions.
				return
			}
		}
	}
	session.ShardSessions = nil
	session.PreSessions = nil
	session.PostSessions = nil
}

// Reset clears the session
func (session *SafeSession) Reset() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.resetCommonLocked()
	session.ShardSessions = nil
	session.PreSessions = nil
	session.PostSessions = nil
}

// ResetAll resets the shard sessions and lock session.
func (session *SafeSession) ResetAll() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.resetCommonLocked()
	session.ShardSessions = nil
	session.PreSessions = nil
	session.PostSessions = nil
	session.LockSession = nil
	session.AdvisoryLock = nil
}

func (session *SafeSession) resetCommonLocked() {
	session.mustRollback = false
	session.autocommitState = notAutocommittable
	session.Session.InTransaction = false
	session.commitOrder = vtgatepb.CommitOrder_NORMAL
	session.Savepoints = nil
	if session.Options != nil {
		session.Options.TransactionAccessMode = nil
	}
}

// NewAutocommitSession returns a SafeSession based on the original
// session, but with autocommit enabled.
func (session *SafeSession) NewAutocommitSession() *SafeSession {
	ss := NewAutocommitSession(session.Session)
	ss.logging = session.logging
	return ss
}

// IsFoundRowsHandled returns the foundRowsHandled.
func (session *SafeSession) IsFoundRowsHandled() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.foundRowsHandled
}

// SetFoundRows set the found rows value.
func (session *SafeSession) SetFoundRows(value uint64) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.FoundRows = value
	session.foundRowsHandled = true
}

// SetInDMLExecution set the `inDMLExecution` value.
func (session *SafeSession) SetInDMLExecution(inDMLExec bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.GetOrCreateOptions().InDmlExecution = inDMLExec
}

// GetRollbackOnPartialExec returns the rollbackOnPartialExec value.
func (session *SafeSession) GetRollbackOnPartialExec() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.rollbackOnPartialExec
}

// SetQueryFromVindex set the queryFromVindex value.
func (session *SafeSession) SetQueryFromVindex(value bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.queryFromVindex = value
}

// GetQueryFromVindex returns the queryFromVindex value.
func (session *SafeSession) GetQueryFromVindex() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.queryFromVindex
}

// SetQueryTimeout sets the query timeout
func (session *SafeSession) SetQueryTimeout(queryTimeout int64) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.QueryTimeout = queryTimeout
}

// GetQueryTimeout gets the query timeout
func (session *SafeSession) GetQueryTimeout() int64 {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.QueryTimeout
}

// SavePoints returns the save points of the session. It's safe to use concurrently
func (session *SafeSession) SavePoints() []string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.GetSavepoints()
}

// SetAutocommittable sets the state to autocommitable if true.
// Otherwise, it's notAutocommitable.
func (session *SafeSession) SetAutocommittable(flag bool) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.autocommitState == autocommitted {
		// Unreachable.
		return
	}

	if flag {
		session.autocommitState = autocommittable
	} else {
		session.autocommitState = notAutocommittable
	}
}

// AutocommitApproval returns true if we can perform a single round-trip
// autocommit. If so, the caller is responsible for committing their
// transaction.
func (session *SafeSession) AutocommitApproval() bool {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.autocommitState == autocommitted {
		// Unreachable.
		return false
	}

	if session.autocommitState == autocommittable {
		session.autocommitState = autocommitted
		return true
	}
	return false
}

// SetSavepointState sets the state only once for the complete query execution life.
// Calling the function multiple times will have no effect, only the first call would be used.
// Default state is savepointStateNotSet,
// if savepoint needed (spNeed true) then it will be set to savepointNeeded otherwise savepointNotNeeded.
func (session *SafeSession) SetSavepointState(spNeed bool) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.savepointState != savepointStateNotSet {
		return
	}

	if spNeed {
		session.savepointState = savepointNeeded
	} else {
		session.savepointState = savepointNotNeeded
	}
}

// CanAddSavepoint returns true if we should insert savepoint and there is no existing savepoint.
func (session *SafeSession) CanAddSavepoint() bool {
	session.mu.Lock()
	defer session.mu.Unlock()

	return session.savepointState == savepointNeeded
}

// SetSavepoint stores the savepoint name to session.
func (session *SafeSession) SetSavepoint(name string) {
	session.mu.Lock()
	defer session.mu.Unlock()

	session.savepointName = name
	session.savepointState = savepointSet
}

// SetRollbackCommand stores the rollback command to session and executed if required.
func (session *SafeSession) SetRollbackCommand() {
	session.mu.Lock()
	defer session.mu.Unlock()

	// if the rollback already happened on the savepoint. There is nothing to set or execute on later.
	if session.savepointState == savepointRollback {
		return
	}

	if session.savepointState == savepointSet {
		session.rollbackOnPartialExec = fmt.Sprintf("rollback to %s", session.savepointName)
	} else {
		session.rollbackOnPartialExec = TxRollback
	}
	session.savepointState = savepointRollbackSet
}

// SavepointRollback updates the state that transaction was rolledback to the savepoint stored in the session.
func (session *SafeSession) SavepointRollback() {
	session.mu.Lock()
	defer session.mu.Unlock()

	session.savepointState = savepointRollback
}

// IsRollbackSet returns true if rollback to savepoint can be done.
func (session *SafeSession) IsRollbackSet() bool {
	session.mu.Lock()
	defer session.mu.Unlock()

	return session.savepointState == savepointRollbackSet
}

// SetCommitOrder sets the commit order.
func (session *SafeSession) SetCommitOrder(co vtgatepb.CommitOrder) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.commitOrder = co
}

// GetCommitOrder returns the commit order.
func (session *SafeSession) GetCommitOrder() vtgatepb.CommitOrder {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.commitOrder
}

func (session *SafeSession) SetErrorUntilRollback(enable bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.ErrorUntilRollback = enable
}

func (session *SafeSession) IsErrorUntilRollback() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.GetErrorUntilRollback()
}

// GetLogger returns executor logger.
func (session *SafeSession) GetLogger() *ExecuteLogger {
	return session.logging
}

// InTransaction returns true if we are in a transaction
func (session *SafeSession) InTransaction() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.InTransaction
}

// FindAndChangeSessionIfInSingleTxMode retrieves the ShardSession matching the given keyspace, shard, and tablet type.
// It performs additional checks and may modify the ShardSession in specific cases for single-mode transactions.
//
// Key behavior:
// 1. Retrieves the appropriate list of sessions (PreSessions, PostSessions, or default ShardSessions) based on the commit order.
// 2. Identifies a matching session by keyspace, shard, and tablet type.
// 3. If the session meets specific conditions (e.g., non-vindex-only, single transaction mode), it updates the session state:
//   - Converts a vindex-only session to a standard session if required by the transaction type.
//   - If a multi-shard transaction is detected in Single mode, marks the session for rollback and returns an error.
//
// Parameters:
// - keyspace: The keyspace of the target shard.
// - shard: The shard name of the target.
// - tabletType: The type of the tablet for the shard session.
// - txMode: The transaction mode (e.g., Single, Multi).
//
// Returns:
// - The matching ShardSession, if found and valid for the operation.
// - An error if a Single-mode transaction attempts to span multiple shards.
func (session *SafeSession) FindAndChangeSessionIfInSingleTxMode(keyspace, shard string, tabletType topodatapb.TabletType, txMode vtgatepb.TransactionMode) (*vtgatepb.Session_ShardSession, error) {
	session.mu.Lock()
	defer session.mu.Unlock()

	shardSession := session.findSessionLocked(keyspace, shard, tabletType)

	if shardSession == nil {
		return nil, nil
	}

	if !shardSession.VindexOnly {
		return shardSession, nil
	}

	if err := session.singleModeErrorOnCrossShard(txMode, 0); err != nil {
		return nil, err
	}

	// the shard session is now used by non-vindex query as well,
	// so it is not an exclusive vindex only shard session anymore.
	shardSession.VindexOnly = false
	return shardSession, nil
}

func (session *SafeSession) findSessionLocked(keyspace, shard string, tabletType topodatapb.TabletType) *vtgatepb.Session_ShardSession {
	// Select the appropriate session list based on the commit order.
	var sessions []*vtgatepb.Session_ShardSession
	switch session.commitOrder {
	case vtgatepb.CommitOrder_PRE:
		sessions = session.PreSessions
	case vtgatepb.CommitOrder_POST:
		sessions = session.PostSessions
	default:
		sessions = session.ShardSessions
	}

	// Find and return the matching shard session.
	for _, shardSession := range sessions {
		if shardSession.Target.Keyspace == keyspace &&
			shardSession.Target.Shard == shard &&
			shardSession.Target.TabletType == tabletType {
			return shardSession
		}
	}
	return nil
}

type ShardActionInfo interface {
	TransactionID() int64
	ReservedID() int64
	RowsAffected() bool
	Alias() *topodatapb.TabletAlias
}

// AppendOrUpdate adds a new ShardSession, or updates an existing one if one already exists for the given shard session
func (session *SafeSession) AppendOrUpdate(target *querypb.Target, info ShardActionInfo, existingSession *vtgatepb.Session_ShardSession, txMode vtgatepb.TransactionMode) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	// additional check of transaction id is required
	// as now in autocommit mode there can be session due to reserved connection
	// that needs to be stored as shard session.
	if session.autocommitState == autocommitted && info.TransactionID() != 0 {
		// Should be unreachable
		return vterrors.VT13001("unexpected 'autocommitted' state in transaction")
	}
	if !(session.Session.InTransaction || session.Session.InReservedConn) {
		// Should be unreachable
		return vterrors.VT13001("current session is neither in transaction nor in reserved connection")
	}
	session.autocommitState = notAutocommittable

	if existingSession != nil {
		existingSession.TransactionId = info.TransactionID()
		existingSession.ReservedId = info.ReservedID()
		if !existingSession.RowsAffected {
			existingSession.RowsAffected = info.RowsAffected()
		}
		if existingSession.VindexOnly {
			existingSession.VindexOnly = session.queryFromVindex
		}
		if err := session.singleModeErrorOnCrossShard(txMode, 1); err != nil {
			return err
		}
		return nil
	}
	newSession := &vtgatepb.Session_ShardSession{
		Target:        target,
		TabletAlias:   info.Alias(),
		TransactionId: info.TransactionID(),
		ReservedId:    info.ReservedID(),
		RowsAffected:  info.RowsAffected(),
		VindexOnly:    session.queryFromVindex,
	}

	// Always append, in order for rollback to succeed.
	switch session.commitOrder {
	case vtgatepb.CommitOrder_NORMAL:
		session.ShardSessions = append(session.ShardSessions, newSession)
		if err := session.singleModeErrorOnCrossShard(txMode, 1); err != nil {
			return err
		}
	case vtgatepb.CommitOrder_PRE:
		session.PreSessions = append(session.PreSessions, newSession)
	case vtgatepb.CommitOrder_POST:
		session.PostSessions = append(session.PostSessions, newSession)
	default:
		// Should be unreachable
		return vterrors.VT13001(fmt.Sprintf("unexpected commitOrder to append shard session: %v", session.commitOrder))
	}

	return nil
}

// singleModeErrorOnCrossShard checks if a transaction violates the Single mode constraint by spanning multiple shards.
func (session *SafeSession) singleModeErrorOnCrossShard(txMode vtgatepb.TransactionMode, exceedsCrossShard int) error {
	// Skip the check if:
	// 1. The query comes from a lookup vindex.
	// 2. The transaction mode is not Single.
	// 3. The transaction is not in the normal shard session.
	if session.queryFromVindex || session.commitOrder != vtgatepb.CommitOrder_NORMAL || !session.isSingleDB(txMode) {
		return nil
	}

	// If the transaction spans multiple shards, abort it.
	if actualNoOfShardSession(session.ShardSessions) > exceedsCrossShard {
		session.mustRollback = true // Mark the session for rollback.
		return vterrors.Errorf(vtrpcpb.Code_ABORTED, "multi-db transaction attempted: %v", session.ShardSessions)
	}

	return nil
}

func actualNoOfShardSession(sessions []*vtgatepb.Session_ShardSession) int {
	actualSS := 0
	for _, ss := range sessions {
		if ss.VindexOnly {
			continue
		}
		actualSS++
	}
	return actualSS
}

func (session *SafeSession) isSingleDB(txMode vtgatepb.TransactionMode) bool {
	return session.TransactionMode == vtgatepb.TransactionMode_SINGLE ||
		(session.TransactionMode == vtgatepb.TransactionMode_UNSPECIFIED && txMode == vtgatepb.TransactionMode_SINGLE)
}

// SetRollback sets the flag indicating that the transaction must be rolled back.
// The call is a no-op if the session is not in a transaction.
func (session *SafeSession) SetRollback() {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.Session.InTransaction {
		session.mustRollback = true
	}
}

// MustRollback returns true if the transaction must be rolled back.
func (session *SafeSession) MustRollback() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.mustRollback
}

// RecordWarning stores the given warning in the session
func (session *SafeSession) RecordWarning(warning *querypb.QueryWarning) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.Warnings = append(session.Session.Warnings, warning)
}

// ClearWarnings removes all the warnings from the session
func (session *SafeSession) ClearWarnings() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.Warnings = nil
}

// SetUserDefinedVariable sets the user defined variable in the session.
func (session *SafeSession) SetUserDefinedVariable(key string, value *querypb.BindVariable) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.UserDefinedVariables == nil {
		session.UserDefinedVariables = make(map[string]*querypb.BindVariable)
	}
	session.UserDefinedVariables[key] = value
}

// SetTargetString sets the target string in the session.
func (session *SafeSession) SetTargetString(target string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.TargetString = target
}

// SetSystemVariable sets the system variable in the session.
func (session *SafeSession) SetSystemVariable(name string, expr string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.SystemVariables == nil {
		session.SystemVariables = make(map[string]string)
	}
	session.SystemVariables[name] = expr
}

// GetSystemVariables takes a visitor function that will receive each MySQL system variable in the session.
// This function will only yield system variables which apply to MySQL itself; Vitess-aware system variables
// will be skipped.
func (session *SafeSession) GetSystemVariables(f func(k string, v string)) {
	session.mu.Lock()
	defer session.mu.Unlock()
	for k, v := range session.SystemVariables {
		if sysvars.IsVitessAware(k) {
			continue
		}
		f(k, v)
	}
}

// HasSystemVariables returns whether the session has system variables that would apply to MySQL
func (session *SafeSession) HasSystemVariables() (found bool) {
	session.GetSystemVariables(func(_ string, _ string) {
		found = true
	})
	return
}

func (session *SafeSession) TimeZone() *time.Location {
	session.mu.Lock()
	zoneSQL, ok := session.SystemVariables["time_zone"]
	session.mu.Unlock()

	if !ok {
		return time.Local
	}

	tz, err := sqltypes.DecodeStringSQL(zoneSQL)
	if err != nil {
		return time.Local
	}

	loc, err := datetime.ParseTimeZone(tz)
	if err != nil {
		return time.Local
	}
	return loc
}

// ForeignKeyChecks returns the foreign_key_checks stored in system_variables map in the session.
func (session *SafeSession) ForeignKeyChecks() *bool {
	session.mu.Lock()
	fkVal, ok := session.SystemVariables[sysvars.ForeignKeyChecks]
	session.mu.Unlock()

	if !ok {
		return nil
	}
	switch strings.ToLower(fkVal) {
	case "off", "0":
		fkCheckBool := false
		return &fkCheckBool
	case "on", "1":
		fkCheckBool := true
		return &fkCheckBool
	}
	return nil
}

// SetOptions sets the options
func (session *SafeSession) SetOptions(options *querypb.ExecuteOptions) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Options = options
}

// StoreSavepoint stores the savepoint and release savepoint queries in the session
func (session *SafeSession) StoreSavepoint(sql string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Savepoints = append(session.Savepoints, sql)
}

// InReservedConn returns true if the session needs to execute on a dedicated connection
func (session *SafeSession) InReservedConn() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.InReservedConn
}

// SetReservedConn set the InReservedConn setting.
func (session *SafeSession) SetReservedConn(reservedConn bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.InReservedConn = reservedConn
}

// SetPreQueries returns the prequeries that need to be run when reserving a connection
func (session *SafeSession) SetPreQueries() []string {
	// extract keys
	var keys []string
	sysVars := make(map[string]string)
	session.GetSystemVariables(func(k string, v string) {
		keys = append(keys, k)
		sysVars[k] = v
	})

	// if not system variables to set, return
	if len(keys) == 0 {
		return nil
	}

	// sort the keys
	sort.Strings(keys)

	// build the query using sorted keys
	var preQuery strings.Builder
	first := true
	for _, k := range keys {
		if first {
			preQuery.WriteString(fmt.Sprintf("set %s = %s", k, sysVars[k]))
			first = false
		} else {
			preQuery.WriteString(fmt.Sprintf(", %s = %s", k, sysVars[k]))
		}
	}
	return []string{preQuery.String()}
}

// SetLockSession sets the lock session.
func (session *SafeSession) SetLockSession(lockSession *vtgatepb.Session_ShardSession) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.LockSession = lockSession
	session.LastLockHeartbeat = time.Now().Unix()
}

// UpdateLockHeartbeat updates the LastLockHeartbeat time
func (session *SafeSession) UpdateLockHeartbeat() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.LastLockHeartbeat = time.Now().Unix()
}

// GetLockHeartbeat returns last time the lock heartbeat was sent.
func (session *SafeSession) GetLockHeartbeat() int64 {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.LastLockHeartbeat
}

// InLockSession returns whether locking is used on this session.
func (session *SafeSession) InLockSession() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.LockSession != nil
}

// ResetLock resets the lock session
func (session *SafeSession) ResetLock() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.LockSession = nil
	session.AdvisoryLock = nil
}

// ResetShard reset the shard session for the provided tablet alias.
func (session *SafeSession) ResetShard(tabletAlias *topodatapb.TabletAlias) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	// Always append, in order for rollback to succeed.
	switch session.commitOrder {
	case vtgatepb.CommitOrder_NORMAL:
		newSessions, err := removeShard(tabletAlias, session.ShardSessions)
		if err != nil {
			return err
		}
		session.ShardSessions = newSessions
	case vtgatepb.CommitOrder_PRE:
		newSessions, err := removeShard(tabletAlias, session.PreSessions)
		if err != nil {
			return err
		}
		session.PreSessions = newSessions
	case vtgatepb.CommitOrder_POST:
		newSessions, err := removeShard(tabletAlias, session.PostSessions)
		if err != nil {
			return err
		}
		session.PostSessions = newSessions
	default:
		// Should be unreachable
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] SafeSession.ResetShard: unexpected commitOrder")
	}
	return nil
}

// SetDDLStrategy set the DDLStrategy setting.
func (session *SafeSession) SetDDLStrategy(strategy string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.DDLStrategy = strategy
}

// GetDDLStrategy returns the DDLStrategy value.
func (session *SafeSession) GetDDLStrategy() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.DDLStrategy
}

// SetMigrationContext set the migration_context setting.
func (session *SafeSession) SetMigrationContext(migrationContext string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.MigrationContext = migrationContext
}

// GetMigrationContext returns the migration_context value.
func (session *SafeSession) GetMigrationContext() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.MigrationContext
}

// GetSessionUUID returns the SessionUUID value.
func (session *SafeSession) GetSessionUUID() string {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.SessionUUID
}

// SetSessionEnableSystemSettings set the SessionEnableSystemSettings setting.
func (session *SafeSession) SetSessionEnableSystemSettings(allow bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.EnableSystemSettings = allow
}

// GetSessionEnableSystemSettings returns the SessionEnableSystemSettings value.
func (session *SafeSession) GetSessionEnableSystemSettings() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.EnableSystemSettings
}

// SetReadAfterWriteGTID set the ReadAfterWriteGtid setting.
func (session *SafeSession) SetReadAfterWriteGTID(vtgtid string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.ReadAfterWrite == nil {
		session.ReadAfterWrite = &vtgatepb.ReadAfterWrite{}
	}
	session.ReadAfterWrite.ReadAfterWriteGtid = vtgtid
}

// SetReadAfterWriteTimeout set the ReadAfterWriteTimeout setting.
func (session *SafeSession) SetReadAfterWriteTimeout(timeout float64) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.ReadAfterWrite == nil {
		session.ReadAfterWrite = &vtgatepb.ReadAfterWrite{}
	}
	session.ReadAfterWrite.ReadAfterWriteTimeout = timeout
}

// SetSessionTrackGtids set the SessionTrackGtids setting.
func (session *SafeSession) SetSessionTrackGtids(enable bool) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.ReadAfterWrite == nil {
		session.ReadAfterWrite = &vtgatepb.ReadAfterWrite{}
	}
	session.ReadAfterWrite.SessionTrackGtids = enable
}

func removeShard(tabletAlias *topodatapb.TabletAlias, sessions []*vtgatepb.Session_ShardSession) ([]*vtgatepb.Session_ShardSession, error) {
	idx := -1
	for i, session := range sessions {
		if proto.Equal(session.TabletAlias, tabletAlias) {
			if session.TransactionId != 0 {
				return nil, vterrors.VT13001("removing shard session when in transaction")
			}
			idx = i
		}
	}
	if idx == -1 {
		return nil, vterrors.VT13001("tried to remove missing shard")
	}
	return append(sessions[:idx], sessions[idx+1:]...), nil
}

// GetOrCreateOptions will return the current options struct, or create one and return it if no-one exists
func (session *SafeSession) GetOrCreateOptions() *querypb.ExecuteOptions {
	if session.Session.Options == nil {
		session.Session.Options = &querypb.ExecuteOptions{}
	}
	return session.Session.Options
}

func (session *SafeSession) CachePlan() bool {
	if session == nil || session.Options == nil {
		return true
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	return !(session.Options.SkipQueryPlanCache || session.Options.HasCreatedTempTables)
}

func (session *SafeSession) GetSelectLimit() int {
	if session == nil || session.Options == nil {
		return -1
	}

	session.mu.Lock()
	defer session.mu.Unlock()

	return int(session.Options.SqlSelectLimit)
}

// IsTxOpen returns true if there is open connection to any of the shard.
func (session *SafeSession) IsTxOpen() bool {
	session.mu.Lock()
	defer session.mu.Unlock()

	return len(session.ShardSessions) > 0 || len(session.PreSessions) > 0 || len(session.PostSessions) > 0
}

// GetSessions returns the shard session for the current commit order.
func (session *SafeSession) GetSessions() []*vtgatepb.Session_ShardSession {
	session.mu.Lock()
	defer session.mu.Unlock()

	switch session.commitOrder {
	case vtgatepb.CommitOrder_PRE:
		return session.PreSessions
	case vtgatepb.CommitOrder_POST:
		return session.PostSessions
	default:
		return session.ShardSessions
	}
}

func (session *SafeSession) RemoveInternalSavepoint() {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.savepointName == "" {
		return
	}
	sCount := len(session.Savepoints)
	if sCount == 0 {
		return
	}
	sLast := sCount - 1
	if strings.Contains(session.Savepoints[sLast], session.savepointName) {
		session.Savepoints = session.Savepoints[0:sLast]
	}
}

// HasAdvisoryLock returns if any advisory lock is taken
func (session *SafeSession) HasAdvisoryLock() bool {
	session.mu.Lock()
	defer session.mu.Unlock()

	return len(session.AdvisoryLock) != 0
}

// AddAdvisoryLock adds the advisory lock to the list.
func (session *SafeSession) AddAdvisoryLock(name string) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.AdvisoryLock == nil {
		session.AdvisoryLock = map[string]int64{name: 1}
		return
	}
	count, exists := session.AdvisoryLock[name]
	if exists {
		count++
	}
	session.AdvisoryLock[name] = count
}

// RemoveAdvisoryLock removes the advisory lock from the list.
func (session *SafeSession) RemoveAdvisoryLock(name string) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.AdvisoryLock == nil {
		return
	}
	count, exists := session.AdvisoryLock[name]
	if !exists {
		return
	}
	count--
	if count == 0 {
		delete(session.AdvisoryLock, name)
		return
	}
	session.AdvisoryLock[name] = count
}

// ClearAdvisoryLock clears the advisory lock list.
func (session *SafeSession) ClearAdvisoryLock() {
	session.mu.Lock()
	defer session.mu.Unlock()

	session.AdvisoryLock = nil
}

func (session *SafeSession) EnableLogging(parser *sqlparser.Parser) {
	session.mu.Lock()
	defer session.mu.Unlock()

	session.logging = &ExecuteLogger{
		parser: parser,
	}
}

// GetUDV returns the bind variable value for the user defined variable.
func (session *SafeSession) GetUDV(name string) *querypb.BindVariable {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.UserDefinedVariables == nil {
		return nil
	}
	return session.UserDefinedVariables[name]
}

// StorePrepareData stores the prepared data information for the given key.
func (session *SafeSession) StorePrepareData(key string, value *vtgatepb.PrepareData) {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.PrepareStatement == nil {
		session.PrepareStatement = map[string]*vtgatepb.PrepareData{}
	}
	session.PrepareStatement[key] = value
}

// GetPrepareData returns the prepared data information for the given key.
func (session *SafeSession) GetPrepareData(name string) *vtgatepb.PrepareData {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.PrepareStatement == nil {
		return nil
	}
	return session.PrepareStatement[name]
}

func (session *SafeSession) Log(primitive engine.Primitive, target *querypb.Target, gateway srvtopo.Gateway, query string, begin bool, bv map[string]*querypb.BindVariable) {
	session.logging.Log(primitive, target, gateway, query, begin, bv)
}

func (session *SafeSession) GetLogs() []engine.ExecuteEntry {
	return session.logging.GetLogs()
}

func (l *ExecuteLogger) Log(primitive engine.Primitive, target *querypb.Target, gateway srvtopo.Gateway, query string, begin bool, bv map[string]*querypb.BindVariable) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	id := l.lastID
	l.lastID++
	if begin {
		l.entries = append(l.entries, engine.ExecuteEntry{
			ID:        id,
			Target:    target,
			Gateway:   gateway,
			Query:     "begin",
			FiredFrom: primitive,
		})
	}
	ast, err := l.parser.Parse(query)
	if err != nil {
		panic("query not able to parse. this should not happen")
	}
	pq := sqlparser.NewParsedQuery(ast)
	if bv == nil {
		bv = map[string]*querypb.BindVariable{}
	}
	q, err := pq.GenerateQuery(bv, nil)
	if err != nil {
		panic("query not able to generate query. this should not happen")
	}

	l.entries = append(l.entries, engine.ExecuteEntry{
		ID:        id,
		Target:    target,
		Gateway:   gateway,
		Query:     q,
		FiredFrom: primitive,
	})
}

func (l *ExecuteLogger) GetLogs() []engine.ExecuteEntry {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	result := make([]engine.ExecuteEntry, len(l.entries))
	copy(result, l.entries)
	return result
}
