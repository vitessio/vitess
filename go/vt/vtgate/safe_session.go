// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"sync"

	"github.com/youtube/vitess/go/vt/vterrors"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// SafeSession is a mutex-protected version of the Session.
// It is thread-safe if each thread only accesses one shard.
// (the use pattern is 'Find', if not found, then 'Append',
// for a single shard)
type SafeSession struct {
	mu           sync.Mutex
	mustRollback bool
	*vtgatepb.Session
}

// NewSafeSession returns a new SafeSession based on the Session
func NewSafeSession(sessn *vtgatepb.Session) *SafeSession {
	return &SafeSession{Session: sessn}
}

// InTransaction returns true if we are in a transaction
func (session *SafeSession) InTransaction() bool {
	if session == nil || session.Session == nil {
		return false
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.InTransaction
}

// Find returns the transactionId, if any, for a session
func (session *SafeSession) Find(keyspace, shard string, tabletType topodatapb.TabletType) int64 {
	if session == nil {
		return 0
	}
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
func (session *SafeSession) Append(shardSession *vtgatepb.Session_ShardSession) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	// Always append, in order for rollback to succeed.
	session.ShardSessions = append(session.ShardSessions, shardSession)
	if session.SingleDb && len(session.ShardSessions) > 1 {
		session.mustRollback = true
		return vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "multi-db transaction attempted: %v", session.ShardSessions)
	}
	return nil
}

// SetRollback sets the flag indicating that the transaction must be rolled back.
// The call is a no-op if the session is not in a transaction.
func (session *SafeSession) SetRollback() {
	if session == nil || session.Session == nil || !session.Session.InTransaction {
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	session.mustRollback = true
}

// MustRollback returns true if the transaction must be rolled back.
func (session *SafeSession) MustRollback() bool {
	if session == nil {
		return false
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.mustRollback
}

// Reset clears the session
func (session *SafeSession) Reset() {
	if session == nil || session.Session == nil {
		return
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.InTransaction = false
	session.ShardSessions = nil
}
