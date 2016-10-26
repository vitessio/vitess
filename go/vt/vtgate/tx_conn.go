// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// TxConn is used for executing transactional requests.
type TxConn struct {
	gateway gateway.Gateway
}

// NewTxConn builds a new TxConn.
func NewTxConn(gw gateway.Gateway) *TxConn {
	return &TxConn{gateway: gw}
}

// Commit commits the current transaction. If twopc is true, then the 2PC protocol
// is used to ensure atomicity.
func (txc *TxConn) Commit(ctx context.Context, twopc bool, session *SafeSession) error {
	if session == nil {
		return vterrors.FromError(vtrpcpb.ErrorCode_BAD_INPUT, errors.New("cannot commit: empty session"))
	}
	if !session.InTransaction() {
		return vterrors.FromError(vtrpcpb.ErrorCode_NOT_IN_TX, errors.New("cannot commit: not in transaction"))
	}
	if twopc {
		return txc.commit2PC(ctx, session)
	}
	return txc.commitNormal(ctx, session)
}

func (txc *TxConn) commitNormal(ctx context.Context, session *SafeSession) error {
	var err error
	committing := true
	for _, shardSession := range session.ShardSessions {
		if !committing {
			txc.gateway.Rollback(ctx, shardSession.Target, shardSession.TransactionId)
			continue
		}
		if err = txc.gateway.Commit(ctx, shardSession.Target, shardSession.TransactionId); err != nil {
			committing = false
		}
	}
	session.Reset()
	return err
}

func (txc *TxConn) commit2PC(ctx context.Context, session *SafeSession) error {
	// If the number of participants is one or less, then it's a normal commit.
	if len(session.ShardSessions) <= 1 {
		return txc.commitNormal(ctx, session)
	}

	participants := make([]*querypb.Target, 0, len(session.ShardSessions)-1)
	for _, s := range session.ShardSessions[1:] {
		participants = append(participants, s.Target)
	}
	mmShard := session.ShardSessions[0]
	// TODO(sougou): Change query_engine to start off transaction id counting
	// above the highest number used by dtids. This will prevent collisions.
	dtid := fmt.Sprintf("%s:%s:0:%d", mmShard.Target.Keyspace, mmShard.Target.Shard, mmShard.TransactionId)
	err := txc.gateway.CreateTransaction(ctx, mmShard.Target, dtid, participants)
	if err != nil {
		// Normal rollback is safe because nothing was prepared yet.
		txc.Rollback(ctx, session)
		return err
	}

	err = txc.multiGo(session.ShardSessions[1:], func(s *vtgatepb.Session_ShardSession) error {
		return txc.gateway.Prepare(ctx, s.Target, s.TransactionId, dtid)
	})
	if err != nil {
		// TODO(sougou): We could call RollbackPrepared here once it's ready.
		return err
	}

	err = txc.gateway.StartCommit(ctx, mmShard.Target, mmShard.TransactionId, dtid)
	if err != nil {
		return err
	}

	err = txc.multiGo(session.ShardSessions[1:], func(s *vtgatepb.Session_ShardSession) error {
		return txc.gateway.CommitPrepared(ctx, s.Target, dtid)
	})
	if err != nil {
		return err
	}

	return txc.gateway.ResolveTransaction(ctx, mmShard.Target, dtid)
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (txc *TxConn) Rollback(ctx context.Context, session *SafeSession) error {
	if session == nil {
		return nil
	}
	defer session.Reset()
	return txc.multiGo(session.ShardSessions, func(s *vtgatepb.Session_ShardSession) error {
		return txc.gateway.Rollback(ctx, s.Target, s.TransactionId)
	})
}

// RollbackIfNeeded rolls back the current transaction if the error implies that the
// transaction can never be completed.
func (txc *TxConn) RollbackIfNeeded(ctx context.Context, err error, session *SafeSession) {
	if session.InTransaction() {
		ec := vterrors.RecoverVtErrorCode(err)
		if ec == vtrpcpb.ErrorCode_RESOURCE_EXHAUSTED || ec == vtrpcpb.ErrorCode_NOT_IN_TX {
			txc.Rollback(ctx, session)
		}
	}
}

// multiGo executes the action for all shardSessions in parallel and returns a consolildated error.
func (txc *TxConn) multiGo(shardSessions []*vtgatepb.Session_ShardSession, action func(*vtgatepb.Session_ShardSession) error) error {
	// Fastpath.
	if len(shardSessions) == 1 {
		return action(shardSessions[0])
	}

	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	for _, s := range shardSessions {
		wg.Add(1)
		go func(s *vtgatepb.Session_ShardSession) {
			defer wg.Done()
			if err := action(s); err != nil {
				allErrors.RecordError(err)
			}
		}(s)
	}
	wg.Wait()
	// Transactional statements should not be retried. No need
	// to scavenge error codes to see if retry is possible. Use
	// simple aggregation to consolidate all errors.
	return allErrors.Error()
}
