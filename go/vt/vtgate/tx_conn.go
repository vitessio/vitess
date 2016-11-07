// Copyright 2016, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package vtgate

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/net/context"

	log "github.com/golang/glog"

	"github.com/youtube/vitess/go/vt/concurrency"
	"github.com/youtube/vitess/go/vt/vterrors"
	"github.com/youtube/vitess/go/vt/vtgate/gateway"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	topodatapb "github.com/youtube/vitess/go/vt/proto/topodata"
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
	dtid := txc.generateDTID(mmShard)
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
		if resumeErr := txc.Resume(ctx, dtid); resumeErr != nil {
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

	return txc.gateway.ResolveTransaction(ctx, mmShard.Target, dtid)
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (txc *TxConn) Rollback(ctx context.Context, session *SafeSession) error {
	if session == nil {
		return nil
	}
	defer session.Reset()
	return txc.runSessions(session.ShardSessions, func(s *vtgatepb.Session_ShardSession) error {
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

// Resume resumes the specified 2PC transaction.
func (txc *TxConn) Resume(ctx context.Context, dtid string) error {
	mmShard, err := txc.dtidToShardSession(dtid)
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
		return vterrors.FromError(vtrpcpb.ErrorCode_INTERNAL_ERROR, fmt.Errorf("invalid state: %v", transaction.State))
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
	return txc.gateway.ResolveTransaction(ctx, target, transaction.Dtid)
}

func (txc *TxConn) resumeCommit(ctx context.Context, target *querypb.Target, transaction *querypb.TransactionMetadata) error {
	err := txc.runTargets(transaction.Participants, func(t *querypb.Target) error {
		return txc.gateway.CommitPrepared(ctx, t, transaction.Dtid)
	})
	if err != nil {
		return err
	}
	return txc.gateway.ResolveTransaction(ctx, target, transaction.Dtid)
}

func (txc *TxConn) generateDTID(mmShard *vtgatepb.Session_ShardSession) string {
	// TODO(sougou): Change query_engine to start off transaction id counting
	// above the highest number used by dtids. This will prevent collisions.
	return fmt.Sprintf("%s:%s:0:%d", mmShard.Target.Keyspace, mmShard.Target.Shard, mmShard.TransactionId)
}

func (txc *TxConn) dtidToShardSession(dtid string) (*vtgatepb.Session_ShardSession, error) {
	splits := strings.Split(dtid, ":")
	if len(splits) != 4 {
		return nil, vterrors.FromError(vtrpcpb.ErrorCode_BAD_INPUT, fmt.Errorf("invalid parts in dtid: %s", dtid))
	}
	target := &querypb.Target{
		Keyspace:   splits[0],
		Shard:      splits[1],
		TabletType: topodatapb.TabletType_MASTER,
	}
	txid, err := strconv.ParseInt(splits[3], 10, 0)
	if err != nil {
		return nil, vterrors.FromError(vtrpcpb.ErrorCode_BAD_INPUT, fmt.Errorf("invalid transaction id in dtid: %s", dtid))
	}
	return &vtgatepb.Session_ShardSession{
		Target:        target,
		TransactionId: txid,
	}, nil
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
	return allErrors.AggrError(aggregateTxConnErrors)
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
	return allErrors.AggrError(aggregateTxConnErrors)
}

func aggregateTxConnErrors(errors []error) error {
	return &ScatterConnError{
		Retryable:  false,
		Errs:       errors,
		serverCode: vterrors.AggregateVtGateErrorCodes(errors),
	}
}
