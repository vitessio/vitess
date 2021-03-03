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

package vtgate

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"

	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// SafeSession is a mutex-protected version of the Session.
// It is thread-safe if each thread only accesses one shard.
// (the use pattern is 'Find', if not found, then 'AppendOrUpdate',
// for a single shard)
type SafeSession struct {
	mu              sync.Mutex
	mustRollback    bool
	autocommitState autocommitState
	commitOrder     vtgatepb.CommitOrder

	// this is a signal that found_rows has already been handles by the primitives,
	// and doesn't have to be updated by the executor
	foundRowsHandled bool
	*vtgatepb.Session
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
type autocommitState int

const (
	notAutocommittable = autocommitState(iota)
	autocommittable
	autocommitted
)

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
	newSession := proto.Clone(sessn).(*vtgatepb.Session)
	newSession.InTransaction = false
	newSession.ShardSessions = nil
	newSession.PreSessions = nil
	newSession.PostSessions = nil
	newSession.Autocommit = true
	newSession.Warnings = nil
	return NewSafeSession(newSession)
}

// ResetTx clears the session
func (session *SafeSession) ResetTx() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.mustRollback = false
	session.autocommitState = notAutocommittable
	session.Session.InTransaction = false
	session.commitOrder = vtgatepb.CommitOrder_NORMAL
	session.Savepoints = nil
	if !session.Session.InReservedConn {
		session.ShardSessions = nil
		session.PreSessions = nil
		session.PostSessions = nil
	}
}

// Reset clears the session
func (session *SafeSession) Reset() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.mustRollback = false
	session.autocommitState = notAutocommittable
	session.Session.InTransaction = false
	session.commitOrder = vtgatepb.CommitOrder_NORMAL
	session.Savepoints = nil
	session.ShardSessions = nil
	session.PreSessions = nil
	session.PostSessions = nil
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

// SetCommitOrder sets the commit order.
func (session *SafeSession) SetCommitOrder(co vtgatepb.CommitOrder) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.commitOrder = co
}

// InTransaction returns true if we are in a transaction
func (session *SafeSession) InTransaction() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.InTransaction
}

// Find returns the transactionId and tabletAlias, if any, for a session
func (session *SafeSession) Find(keyspace, shard string, tabletType topodatapb.TabletType) (transactionID int64, reservedID int64, alias *topodatapb.TabletAlias) {
	session.mu.Lock()
	defer session.mu.Unlock()
	sessions := session.ShardSessions
	switch session.commitOrder {
	case vtgatepb.CommitOrder_PRE:
		sessions = session.PreSessions
	case vtgatepb.CommitOrder_POST:
		sessions = session.PostSessions
	}
	for _, shardSession := range sessions {
		if keyspace == shardSession.Target.Keyspace && tabletType == shardSession.Target.TabletType && shard == shardSession.Target.Shard {
			return shardSession.TransactionId, shardSession.ReservedId, shardSession.TabletAlias
		}
	}
	return 0, 0, nil
}

func addOrUpdate(shardSession *vtgatepb.Session_ShardSession, sessions []*vtgatepb.Session_ShardSession) ([]*vtgatepb.Session_ShardSession, error) {
	appendSession := true
	for i, sess := range sessions {
		targetedAtSameTablet := sess.Target.Keyspace == shardSession.Target.Keyspace &&
			sess.Target.TabletType == shardSession.Target.TabletType &&
			sess.Target.Shard == shardSession.Target.Shard
		if targetedAtSameTablet {
			if !proto.Equal(sess.TabletAlias, shardSession.TabletAlias) {
				errorDetails := fmt.Sprintf("got non-matching aliases (%v vs %v) for the same target (keyspace: %v, tabletType: %v, shard: %v)",
					sess.TabletAlias, shardSession.TabletAlias,
					sess.Target.Keyspace, sess.Target.TabletType, sess.Target.Shard)
				return nil, vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, errorDetails)
			}
			// replace the old info with the new one
			sessions[i] = shardSession
			appendSession = false
			break
		}
	}
	if appendSession {
		sessions = append(sessions, shardSession)
	}

	return sessions, nil
}

// AppendOrUpdate adds a new ShardSession, or updates an existing one if one already exists for the given shard session
func (session *SafeSession) AppendOrUpdate(shardSession *vtgatepb.Session_ShardSession, txMode vtgatepb.TransactionMode) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	// additional check of transaction id is required
	// as now in autocommit mode there can be session due to reserved connection
	// that needs to be stored as shard session.
	if session.autocommitState == autocommitted && shardSession.TransactionId != 0 {
		// Should be unreachable
		return vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] unexpected 'autocommitted' state in transaction")
	}
	if !(session.Session.InTransaction || session.Session.InReservedConn) {
		// Should be unreachable
		return vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] current session neither in transaction nor in reserved connection")
	}
	session.autocommitState = notAutocommittable

	// Always append, in order for rollback to succeed.
	switch session.commitOrder {
	case vtgatepb.CommitOrder_NORMAL:
		newSessions, err := addOrUpdate(shardSession, session.ShardSessions)
		if err != nil {
			return err
		}
		session.ShardSessions = newSessions
		// isSingle is enforced only for normmal commit order operations.
		if session.isSingleDB(txMode) && len(session.ShardSessions) > 1 {
			session.mustRollback = true
			return vterrors.Errorf(vtrpcpb.Code_ABORTED, "multi-db transaction attempted: %v", session.ShardSessions)
		}
	case vtgatepb.CommitOrder_PRE:
		newSessions, err := addOrUpdate(shardSession, session.PreSessions)
		if err != nil {
			return err
		}
		session.PreSessions = newSessions
	case vtgatepb.CommitOrder_POST:
		newSessions, err := addOrUpdate(shardSession, session.PostSessions)
		if err != nil {
			return err
		}
		session.PostSessions = newSessions
	default:
		// Should be unreachable
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] SafeSession.AppendOrUpdate: unexpected commitOrder")
	}

	return nil
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

//SetSystemVariable sets the system variable in th session.
func (session *SafeSession) SetSystemVariable(name string, expr string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.SystemVariables == nil {
		session.SystemVariables = make(map[string]string)
	}
	session.SystemVariables[name] = expr
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
	session.mu.Lock()
	defer session.mu.Unlock()
	result := make([]string, len(session.SystemVariables))
	idx := 0
	for k, v := range session.SystemVariables {
		result[idx] = fmt.Sprintf("set @@%s = %s", k, v)
		idx++
	}
	return result
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

// TriggerLockHeartBeat returns if it time to trigger next lock heartbeat
func (session *SafeSession) TriggerLockHeartBeat() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	now := time.Now().Unix()
	return now-session.LastLockHeartbeat >= int64(lockHeartbeatTime.Seconds())
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
}

// ResetAll resets the shard sessions and lock session.
func (session *SafeSession) ResetAll() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.mustRollback = false
	session.autocommitState = notAutocommittable
	session.Session.InTransaction = false
	session.commitOrder = vtgatepb.CommitOrder_NORMAL
	session.Savepoints = nil
	session.ShardSessions = nil
	session.PreSessions = nil
	session.PostSessions = nil
	session.LockSession = nil
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
				return nil, vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG] removing shard session when in transaction")
			}
			idx = i
		}
	}
	if idx == -1 {
		return sessions, nil
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
