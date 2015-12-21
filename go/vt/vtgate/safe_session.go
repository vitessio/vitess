// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"sync"

	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
)

// SafeSession is a mutex-protected version of the Session.
// It is thread-safe if each thread only accesses one shard.
// (the use pattern is 'Find', if not found, then 'Append',
// for a single shard)
type SafeSession struct {
	mu sync.Mutex
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
func (session *SafeSession) Append(shardSession *vtgatepb.Session_ShardSession) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.ShardSessions = append(session.ShardSessions, shardSession)
}

// Reset clears the session
func (session *SafeSession) Reset() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.InTransaction = false
	session.ShardSessions = nil
}
