/*
Copyright 2017 Google Inc.

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
	"sync"

	"github.com/golang/protobuf/proto"
	"vitess.io/vitess/go/vt/vterrors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

// SafeSession is a mutex-protected version of the Session.
// It is thread-safe if each thread only accesses one shard.
// (the use pattern is 'Find', if not found, then 'Append',
// for a single shard)
type SafeSession struct {
	mu              sync.Mutex
	mustRollback    bool
	autocommitState autocommitState
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
// be issued if the state is autocommitable, and ShardSessions
// is empty, implying that no intermediate transactions were started.
// If so, the state transitions to autocommited, which is terminal.
// If the token is succesfully issued, the caller has to perform
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
	newSession.Autocommit = true
	return NewSafeSession(newSession)
}

// Reset clears the session
func (session *SafeSession) Reset() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.mustRollback = false
	session.autocommitState = notAutocommittable
	session.Session.InTransaction = false
	session.SingleDb = false
	session.ShardSessions = nil
}

// SetAutocommitable sets the state to autocommitable if true.
// Otherwise, it's notAutocommitable.
func (session *SafeSession) SetAutocommitable(flag bool) {
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

// InTransaction returns true if we are in a transaction
func (session *SafeSession) InTransaction() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.InTransaction
}

// Find returns the transactionId, if any, for a session
func (session *SafeSession) Find(keyspace, shard string, tabletType topodatapb.TabletType) int64 {
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, shardSession := range session.ShardSessions {
		if keyspace == shardSession.Target.Keyspace && tabletType == shardSession.Target.TabletType && shard == shardSession.Target.Shard {
			return shardSession.TransactionId
		}
	}
	return 0
}

// Append adds a new ShardSession
func (session *SafeSession) Append(shardSession *vtgatepb.Session_ShardSession, txMode vtgatepb.TransactionMode) error {
	session.mu.Lock()
	defer session.mu.Unlock()

	if session.autocommitState == autocommitted {
		// Unreachable.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: SafeSession.Append: unexpected autocommit state")
	}
	if !session.Session.InTransaction {
		// Unreachable.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: SafeSession.Append: not in transaction")
	}
	session.autocommitState = notAutocommittable

	// Always append, in order for rollback to succeed.
	session.ShardSessions = append(session.ShardSessions, shardSession)
	if session.isSingleDB(txMode) && len(session.ShardSessions) > 1 {
		session.mustRollback = true
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "multi-db transaction attempted: %v", session.ShardSessions)
	}
	return nil
}

func (session *SafeSession) isSingleDB(txMode vtgatepb.TransactionMode) bool {
	return session.SingleDb ||
		session.TransactionMode == vtgatepb.TransactionMode_SINGLE ||
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
