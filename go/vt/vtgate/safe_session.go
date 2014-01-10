// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"sync"

	"github.com/youtube/vitess/go/vt/topo"
	"github.com/youtube/vitess/go/vt/vtgate/proto"
)

type SafeSession struct {
	mu sync.Mutex
	*proto.Session
}

func NewSafeSession(sessn *proto.Session) *SafeSession {
	return &SafeSession{Session: sessn}
}

func (session *SafeSession) InTransaction() bool {
	if session == nil {
		return false
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.Session.InTransaction
}

func (session *SafeSession) Find(keyspace, shard string, tabletType topo.TabletType) int64 {
	if session == nil {
		return 0
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	for _, shardSession := range session.ShardSessions {
		if keyspace == shardSession.Keyspace && tabletType == shardSession.TabletType && shard == shardSession.Shard {
			return shardSession.TransactionId
		}
	}
	return 0
}

func (session *SafeSession) Append(shardSession *proto.ShardSession) {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.ShardSessions = append(session.ShardSessions, shardSession)
}

func (session *SafeSession) Reset() {
	session.mu.Lock()
	defer session.mu.Unlock()
	session.Session.InTransaction = false
	session.ShardSessions = nil
}
