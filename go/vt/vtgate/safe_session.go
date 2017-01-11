// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/vt/vterrors"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
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
	*Session
}

// Session is to contain information about per shard session.
type Session struct {
	InTransaction     bool
	SafeShardSessions []*SafeShardSession
	SingleDb          bool
}

// SafeShardSession is a mutex-protected version of the ShardSession.
type SafeShardSession struct {
	mu         sync.Mutex
	*vtgatepb.Session_ShardSession
}

// NewSafeSession returns a new SafeSession based on the Session
func NewSafeSession(sessn *vtgatepb.Session) *SafeSession {
	if sessn == nil {
		return &SafeSession{Session: nil}
	}
	var safeshardSessions []*SafeShardSession
	for _, shSession := range sessn.ShardSessions {
		safeshardSessions = append(safeshardSessions, &SafeShardSession{
			Session_ShardSession: shSession,
		})
	}
	return &SafeSession{Session: &Session{
		InTransaction:     sessn.InTransaction,
		SingleDb:          sessn.SingleDb,
		SafeShardSessions: safeshardSessions,
	}}
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

// FindOrAppend returns the SafeShardSession, for a session
func (session *SafeSession) FindOrAppend(target *querypb.Target, notInTransaction bool) (*SafeShardSession, error) {
	if session == nil {
		return nil, nil
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, safeShardSession := range session.SafeShardSessions {
		if target.Keyspace == safeShardSession.Target.Keyspace && target.TabletType == safeShardSession.Target.TabletType && target.Shard == safeShardSession.Target.Shard {
			return safeShardSession, nil
		}
	}
	if notInTransaction {
		return nil, nil
	}
	safeShardSession := &SafeShardSession{
		Session_ShardSession: &vtgatepb.Session_ShardSession{
			Target:        target,
			TransactionId: 0,
		},
	}
	err := session.append(safeShardSession)
	if err != nil {
		return nil, err
	}

	return safeShardSession, nil
}

// append adds a new SafeShardSession
func (session *SafeSession) append(shardSession *SafeShardSession) error {
	// Always append, in order for rollback to succeed.
	session.SafeShardSessions = append(session.SafeShardSessions, shardSession)
	if session.SingleDb && len(session.SafeShardSessions) > 1 {
		session.mustRollback = true
		for _, safeShardSession := range session.SafeShardSessions {
			safeShardSession.mu.Lock()
		}
		errMessage := fmt.Errorf("multi-db transaction attempted: %v", session.SafeShardSessions)
		for _, safeShardSession := range session.SafeShardSessions {
			safeShardSession.mu.Unlock()
		}
		return vterrors.FromError(vtrpcpb.ErrorCode_BAD_INPUT, errMessage)
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
	session.SafeShardSessions = nil
}
