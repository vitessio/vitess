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

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/dtids"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtgatepb "github.com/youtube/vitess/go/vt/proto/vtgate"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

// TxConn is used for executing transactional requests.
type TxConn struct {
	gateway gateway.Gateway
	mode    vtgatepb.TransactionMode
}

// NewTxConn builds a new TxConn.
func NewTxConn(gw gateway.Gateway, txMode vtgatepb.TransactionMode) *TxConn {
	return &TxConn{
		gateway: gw,
		mode:    txMode,
	}
}

// Begin begins a new transaction. If one is already in progress, it commmits it
// and starts a new one.
func (txc *TxConn) Begin(ctx context.Context, session *SafeSession) error {
	if session.InTransaction() {
		if err := txc.Commit(ctx, session); err != nil {
			return err
		}
	}
	// UNSPECIFIED & SINGLE mode are always allowed.
	switch session.TransactionMode {
	case vtgatepb.TransactionMode_MULTI:
		if txc.mode == vtgatepb.TransactionMode_SINGLE {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "requested transaction mode %v disallowed: vtgate must be started with --transaction_mode=MULTI (or TWOPC). Current transaction mode: %v", session.TransactionMode, txc.mode)
		}
	case vtgatepb.TransactionMode_TWOPC:
		if txc.mode != vtgatepb.TransactionMode_TWOPC {
			return vterrors.Errorf(vtrpcpb.Code_FAILED_PRECONDITION, "requested transaction mode %v disallowed: vtgate must be started with --transaction_mode=TWOPC. Current transaction mode: %v", session.TransactionMode, txc.mode)
		}
	}
	session.Session.InTransaction = true
	return nil
}

// Commit commits the current transaction. The type of commit can be
// best effort or 2pc depending on the session setting.
func (txc *TxConn) Commit(ctx context.Context, session *SafeSession) error {
	defer session.Reset()
	if !session.InTransaction() {
		return nil
	}

	twopc := false
	switch session.TransactionMode {
	case vtgatepb.TransactionMode_TWOPC:
		if txc.mode != vtgatepb.TransactionMode_TWOPC {
			txc.Rollback(ctx, session)
			return vterrors.New(vtrpcpb.Code_FAILED_PRECONDITION, "2pc transaction disallowed")
		}
		twopc = true
	case vtgatepb.TransactionMode_UNSPECIFIED:
		twopc = (txc.mode == vtgatepb.TransactionMode_TWOPC)
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
	dtid := dtids.New(mmShard)
	err := txc.gateway.CreateTransaction(ctx, mmShard.Target, dtid, participants)
	if err != nil {
		// Normal rollback is safe because nothing was prepared yet.
		txc.Rollback(ctx, session)
		return err
	}

	err = txc.runSessions(session.ShardSessions[1:], func(s *vtgatepb.Session_ShardSession) error {
		return txc.gateway.Prepare(ctx, s.Target, s.TransactionId, dtid)
	})
	if err != nil {
		// TODO(sougou): Perform a more fine-grained cleanup
		// including unprepared transactions.
		if resumeErr := txc.Resolve(ctx, dtid); resumeErr != nil {
			log.Warningf("Rollback failed after Prepare failure: %v", resumeErr)
		}
		// Return the original error even if the previous operation fails.
		return err
	}

	err = txc.gateway.StartCommit(ctx, mmShard.Target, mmShard.TransactionId, dtid)
	if err != nil {
		return err
	}

	err = txc.runSessions(session.ShardSessions[1:], func(s *vtgatepb.Session_ShardSession) error {
		return txc.gateway.CommitPrepared(ctx, s.Target, dtid)
	})
	if err != nil {
		return err
	}

	return txc.gateway.ConcludeTransaction(ctx, mmShard.Target, dtid)
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (txc *TxConn) Rollback(ctx context.Context, session *SafeSession) error {
	if !session.InTransaction() {
		return nil
	}
	defer session.Reset()

	return txc.runSessions(session.ShardSessions, func(s *vtgatepb.Session_ShardSession) error {
		return txc.gateway.Rollback(ctx, s.Target, s.TransactionId)
	})
}

// Resolve resolves the specified 2PC transaction.
func (txc *TxConn) Resolve(ctx context.Context, dtid string) error {
	mmShard, err := dtids.ShardSession(dtid)
	if err != nil {
		return err
	}

	transaction, err := txc.gateway.ReadTransaction(ctx, mmShard.Target, dtid)
	if err != nil {
		return err
	}
	if transaction == nil || transaction.Dtid == "" {
		// It was already resolved.
		return nil
	}
	switch transaction.State {
	case querypb.TransactionState_PREPARE:
		// If state is PREPARE, make a decision to rollback and
		// fallthrough to the rollback workflow.
		if err := txc.gateway.SetRollback(ctx, mmShard.Target, transaction.Dtid, mmShard.TransactionId); err != nil {
			return err
		}
		fallthrough
	case querypb.TransactionState_ROLLBACK:
		if err := txc.resumeRollback(ctx, mmShard.Target, transaction); err != nil {
			return err
		}
	case querypb.TransactionState_COMMIT:
		if err := txc.resumeCommit(ctx, mmShard.Target, transaction); err != nil {
			return err
		}
	default:
		// Should never happen.
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "invalid state: %v", transaction.State)
	}
	return nil
}

func (txc *TxConn) resumeRollback(ctx context.Context, target *querypb.Target, transaction *querypb.TransactionMetadata) error {
	err := txc.runTargets(transaction.Participants, func(t *querypb.Target) error {
		return txc.gateway.RollbackPrepared(ctx, t, transaction.Dtid, 0)
	})
	if err != nil {
		return err
	}
	return txc.gateway.ConcludeTransaction(ctx, target, transaction.Dtid)
}

func (txc *TxConn) resumeCommit(ctx context.Context, target *querypb.Target, transaction *querypb.TransactionMetadata) error {
	err := txc.runTargets(transaction.Participants, func(t *querypb.Target) error {
		return txc.gateway.CommitPrepared(ctx, t, transaction.Dtid)
	})
	if err != nil {
		return err
	}
	return txc.gateway.ConcludeTransaction(ctx, target, transaction.Dtid)
}

// runSessions executes the action for all shardSessions in parallel and returns a consolildated error.
func (txc *TxConn) runSessions(shardSessions []*vtgatepb.Session_ShardSession, action func(*vtgatepb.Session_ShardSession) error) error {
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
	return allErrors.AggrError(vterrors.Aggregate)
}

// runTargets executes the action for all targets in parallel and returns a consolildated error.
// Flow is identical to runSessions.
func (txc *TxConn) runTargets(targets []*querypb.Target, action func(*querypb.Target) error) error {
	if len(targets) == 1 {
		return action(targets[0])
	}
	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(t *querypb.Target) {
			defer wg.Done()
			if err := action(t); err != nil {
				allErrors.RecordError(err)
			}
		}(t)
	}
	wg.Wait()
	return allErrors.AggrError(vterrors.Aggregate)
}
