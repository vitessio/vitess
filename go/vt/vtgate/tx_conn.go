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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql/sqlerror"
	"vitess.io/vitess/go/vt/concurrency"
	"vitess.io/vitess/go/vt/dtids"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
	vtgatepb "vitess.io/vitess/go/vt/proto/vtgate"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/dynamicconfig"
	econtext "vitess.io/vitess/go/vt/vtgate/executorcontext"
	"vitess.io/vitess/go/vt/vttablet/queryservice"
)

// nonAtomicCommitWarnMaxShards limits the number of shard names reported in
// non-atomic commit warnings.
const nonAtomicCommitWarnMaxShards = 16

// TxConn is used for executing transactional requests.
type TxConn struct {
	tabletGateway *TabletGateway
	txMode        dynamicconfig.TxMode
}

// NewTxConn builds a new TxConn.
func NewTxConn(gw *TabletGateway, txMode dynamicconfig.TxMode) *TxConn {
	return &TxConn{
		tabletGateway: gw,
		txMode:        txMode,
	}
}

var txAccessModeToEOTxAccessMode = map[sqlparser.TxAccessMode]querypb.ExecuteOptions_TransactionAccessMode{
	sqlparser.WithConsistentSnapshot: querypb.ExecuteOptions_CONSISTENT_SNAPSHOT,
	sqlparser.ReadWrite:              querypb.ExecuteOptions_READ_WRITE,
	sqlparser.ReadOnly:               querypb.ExecuteOptions_READ_ONLY,
}

type commitPhase int

const (
	Commit2pcCreateTransaction commitPhase = iota
	Commit2pcPrepare
	Commit2pcStartCommit
	Commit2pcPrepareCommit
	Commit2pcConclude
)

var phaseMessage = map[commitPhase]string{
	Commit2pcCreateTransaction: "Create Transaction",
	Commit2pcPrepare:           "Prepare",
	Commit2pcStartCommit:       "Start Commit",
	Commit2pcPrepareCommit:     "Prepare Commit",
	Commit2pcConclude:          "Conclude",
}

// Begin begins a new transaction. If one is already in progress, it commits it
// and starts a new one.
func (txc *TxConn) Begin(ctx context.Context, session *econtext.SafeSession, txAccessModes []sqlparser.TxAccessMode) error {
	if session.InTransaction() {
		if err := txc.Commit(ctx, session); err != nil {
			return err
		}
	}
	if len(txAccessModes) > 0 {
		options := session.GetOrCreateOptions()
		for _, txAccessMode := range txAccessModes {
			accessMode, ok := txAccessModeToEOTxAccessMode[txAccessMode]
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] invalid transaction characteristic: %s", txAccessMode.ToString())
			}
			options.TransactionAccessMode = append(options.TransactionAccessMode, accessMode)
		}
	}
	session.Session.InTransaction = true
	return nil
}

// Commit commits the current transaction. The type of commit can be
// best effort or 2pc depending on the session setting.
func (txc *TxConn) Commit(ctx context.Context, session *econtext.SafeSession) error {
	defer session.ResetTx()
	if !session.InTransaction() {
		return nil
	}

	twopc := false
	switch session.TransactionMode {
	case vtgatepb.TransactionMode_TWOPC:
		twopc = true
	case vtgatepb.TransactionMode_UNSPECIFIED:
		twopc = txc.txMode.TransactionMode() == vtgatepb.TransactionMode_TWOPC
	}

	defer recordCommitTime(session, twopc, time.Now())

	err := txc.runSessions(ctx, session.PreSessions, session.GetLogger(), txc.commitShard)
	if err != nil {
		_ = txc.Release(ctx, session)
		return err
	}

	if twopc {
		err = txc.commit2PC(ctx, session)
	} else {
		err = txc.commitNormal(ctx, session)
	}

	if err != nil {
		_ = txc.Release(ctx, session)
		return err
	}

	err = txc.runSessions(ctx, session.PostSessions, session.GetLogger(), txc.commitShard)
	if err != nil {
		// If last commit fails, there will be nothing to rollback.
		session.RecordWarning(&querypb.QueryWarning{Message: fmt.Sprintf("post-operation transaction had an error: %v", err)})
		// With reserved connection we should release them.
		if session.InReservedConn() {
			_ = txc.Release(ctx, session)
		}
	}
	return nil
}

func recordCommitTime(session *econtext.SafeSession, twopc bool, startTime time.Time) {
	switch {
	case len(session.ShardSessions) == 0:
		// No-op
	case len(session.ShardSessions) == 1:
		commitMode.Record("Single", startTime)
	case twopc:
		commitMode.Record("TwoPC", startTime)
	default:
		commitMode.Record("Multi", startTime)
	}
}

func (txc *TxConn) queryService(ctx context.Context, alias *topodatapb.TabletAlias) (queryservice.QueryService, error) {
	if alias == nil {
		return txc.tabletGateway, nil
	}
	qs, err := txc.tabletGateway.QueryServiceByAlias(ctx, alias, nil)
	if err != nil {
		return nil, vterrors.VT15001(vterrors.Code(err), err.Error())
	}
	return qs, nil
}

func (txc *TxConn) commitShard(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *econtext.ExecuteLogger) error {
	if s.TransactionId == 0 {
		return nil
	}
	var qs queryservice.QueryService
	var err error
	qs, err = txc.queryService(ctx, s.TabletAlias)
	if err != nil {
		return err
	}
	reservedID, err := qs.Commit(ctx, s.Target, s.TransactionId)
	if err != nil {
		return err
	}
	s.TransactionId = 0
	s.ReservedId = reservedID
	logging.Log(nil, s.Target, nil, "commit", false, nil)
	return nil
}

func (txc *TxConn) commitNormal(ctx context.Context, session *econtext.SafeSession) error {
	// Retain backward compatibility on commit order for the normal session.
	for i, shardSession := range session.ShardSessions {
		if err := txc.commitShard(ctx, shardSession, session.GetLogger()); err != nil {
			if i > 0 {
				nShards := i
				elipsis := false
				if i > nonAtomicCommitWarnMaxShards {
					nShards = nonAtomicCommitWarnMaxShards
					elipsis = true
				}
				sNames := make([]string, nShards, nShards+1 /*...*/)
				for j := 0; j < nShards; j++ {
					sNames[j] = session.ShardSessions[j].Target.Shard
				}
				if elipsis {
					sNames = append(sNames, "...")
				}
				session.RecordWarning(&querypb.QueryWarning{
					Code:    uint32(sqlerror.ERNonAtomicCommit),
					Message: fmt.Sprintf("multi-db commit failed after committing to %d shards: %s", i, strings.Join(sNames, ", ")),
				})
				warnings.Add("NonAtomicCommit", 1)
			}
			return err
		}
	}
	return nil
}

// commit2PC will not used the pinned tablets - to make sure we use the current source, we need to use the gateway's queryservice
func (txc *TxConn) commit2PC(ctx context.Context, session *econtext.SafeSession) (err error) {
	// If the number of participants is one or less, then it's a normal commit.
	if len(session.ShardSessions) <= 1 {
		return txc.commitNormal(ctx, session)
	}

	mmShard := session.ShardSessions[0]
	rmShards := session.ShardSessions[1:]
	dtid := dtids.New(mmShard)
	participants := make([]*querypb.Target, len(rmShards))
	for i, s := range rmShards {
		participants[i] = s.Target
	}

	var txPhase commitPhase
	var startCommitState querypb.StartCommitState
	defer func() {
		if err == nil {
			return
		}
		txc.errActionAndLogWarn(ctx, session, txPhase, startCommitState, dtid, mmShard, rmShards)
	}()

	txPhase = Commit2pcCreateTransaction
	if err = txc.tabletGateway.CreateTransaction(ctx, mmShard.Target, dtid, participants); err != nil {
		return err
	}

	if DebugTwoPc { // Test code to simulate a failure after RM prepare
		if terr := checkTestFailure(ctx, "TRCreated_FailNow", nil); terr != nil {
			return terr
		}
	}

	txPhase = Commit2pcPrepare
	prepareAction := func(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *econtext.ExecuteLogger) error {
		if DebugTwoPc { // Test code to simulate a failure during RM prepare
			if terr := checkTestFailure(ctx, "RMPrepare_-40_FailNow", s.Target); terr != nil {
				return terr
			}
		}
		return txc.tabletGateway.Prepare(ctx, s.Target, s.TransactionId, dtid)
	}
	if err = txc.runSessions(ctx, rmShards, session.GetLogger(), prepareAction); err != nil {
		return err
	}

	if DebugTwoPc { // Test code to simulate a failure after RM prepare
		if terr := checkTestFailure(ctx, "RMPrepared_FailNow", nil); terr != nil {
			return terr
		}
	}

	txPhase = Commit2pcStartCommit
	startCommitState, err = txc.tabletGateway.StartCommit(ctx, mmShard.Target, mmShard.TransactionId, dtid)
	if err != nil {
		return err
	}

	if DebugTwoPc { // Test code to simulate a failure after MM commit
		if terr := checkTestFailure(ctx, "MMCommitted_FailNow", nil); terr != nil {
			return terr
		}
	}

	txPhase = Commit2pcPrepareCommit
	prepareCommitAction := func(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *econtext.ExecuteLogger) error {
		if DebugTwoPc { // Test code to simulate a failure during RM prepare
			if terr := checkTestFailure(ctx, "RMCommit_-40_FailNow", s.Target); terr != nil {
				return terr
			}
		}
		return txc.tabletGateway.CommitPrepared(ctx, s.Target, dtid)
	}
	if err = txc.runSessions(ctx, rmShards, session.GetLogger(), prepareCommitAction); err != nil {
		return err
	}

	// At this point, application can continue forward.
	// The transaction is already committed.
	// This step is to clean up the transaction metadata.
	txPhase = Commit2pcConclude
	_ = txc.tabletGateway.ConcludeTransaction(ctx, mmShard.Target, dtid)
	return nil
}

func (txc *TxConn) errActionAndLogWarn(
	ctx context.Context,
	session *econtext.SafeSession,
	txPhase commitPhase,
	startCommitState querypb.StartCommitState,
	dtid string,
	mmShard *vtgatepb.Session_ShardSession,
	rmShards []*vtgatepb.Session_ShardSession,
) {
	var rollbackErr error
	switch txPhase {
	case Commit2pcCreateTransaction:
		// Normal rollback is safe because nothing was prepared yet.
		rollbackErr = txc.Rollback(ctx, session)
	case Commit2pcPrepare:
		// Rollback the prepared and unprepared transactions.
		rollbackErr = txc.rollbackTx(ctx, dtid, mmShard, rmShards, session.GetLogger())
	case Commit2pcStartCommit:
		// Failed to store the commit decision on MM.
		// If the failure state is certain, then the only option is to rollback the prepared transactions on the RMs.
		if startCommitState == querypb.StartCommitState_Fail {
			rollbackErr = txc.rollbackTx(ctx, dtid, mmShard, rmShards, session.GetLogger())
		}
		fallthrough
	case Commit2pcPrepareCommit:
		commitUnresolved.Add(1)
	}
	if rollbackErr != nil {
		log.Warningf("Rollback failed after %s failure: %v", phaseMessage[txPhase], rollbackErr)
		commitUnresolved.Add(1)
	}

	session.RecordWarning(&querypb.QueryWarning{
		Code:    uint32(sqlerror.ERInAtomicRecovery),
		Message: createWarningMessage(dtid, txPhase)})
}

func createWarningMessage(dtid string, txPhase commitPhase) string {
	warningMsg := fmt.Sprintf("%s distributed transaction ID failed during", dtid)
	switch txPhase {
	case Commit2pcCreateTransaction:
		warningMsg += " transaction record creation; rollback attempted; conclude on recovery"
	case Commit2pcPrepare:
		warningMsg += " transaction prepare phase; prepare transaction rollback attempted; conclude on recovery"
	case Commit2pcStartCommit:
		warningMsg += " metadata manager commit; transaction will be committed/rollbacked based on the state on recovery"
	case Commit2pcPrepareCommit:
		warningMsg += " resource manager commit; transaction will be committed on recovery"
	case Commit2pcConclude:
		warningMsg += " transaction conclusion"
	}
	return warningMsg
}

// Rollback rolls back the current transaction. There are no retries on this operation.
func (txc *TxConn) Rollback(ctx context.Context, session *econtext.SafeSession) error {
	if !session.InTransaction() {
		return nil
	}
	defer session.ResetTx()

	allsessions := append(session.PreSessions, session.ShardSessions...)
	allsessions = append(allsessions, session.PostSessions...)

	err := txc.runSessions(ctx, allsessions, session.GetLogger(), func(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *econtext.ExecuteLogger) error {
		if s.TransactionId == 0 {
			return nil
		}
		qs, err := txc.queryService(ctx, s.TabletAlias)
		if err != nil {
			return err
		}
		reservedID, err := qs.Rollback(ctx, s.Target, s.TransactionId)
		if err != nil {
			return err
		}
		s.TransactionId = 0
		s.ReservedId = reservedID
		logging.Log(nil, s.Target, nil, "rollback", false, nil)
		return nil
	})
	if err != nil {
		session.RecordWarning(&querypb.QueryWarning{Message: fmt.Sprintf("rollback encountered an error and connection to all shard for this session is released: %v", err)})
		if session.InReservedConn() {
			_ = txc.Release(ctx, session)
		}
	}
	return err
}

// Release releases the reserved connection and/or rollbacks the transaction
func (txc *TxConn) Release(ctx context.Context, session *econtext.SafeSession) error {
	if !session.InTransaction() && !session.InReservedConn() {
		return nil
	}
	defer session.Reset()

	allsessions := append(session.PreSessions, session.ShardSessions...)
	allsessions = append(allsessions, session.PostSessions...)

	return txc.runSessions(ctx, allsessions, session.GetLogger(), func(ctx context.Context, s *vtgatepb.Session_ShardSession, logging *econtext.ExecuteLogger) error {
		if s.ReservedId == 0 && s.TransactionId == 0 {
			return nil
		}
		qs, err := txc.queryService(ctx, s.TabletAlias)
		if err != nil {
			return err
		}
		err = qs.Release(ctx, s.Target, s.TransactionId, s.ReservedId)
		if err != nil {
			return err
		}
		s.TransactionId = 0
		s.ReservedId = 0
		return nil
	})
}

// ReleaseLock releases the reserved connection used for locking.
func (txc *TxConn) ReleaseLock(ctx context.Context, session *econtext.SafeSession) error {
	if !session.InLockSession() {
		return nil
	}
	defer session.ResetLock()

	session.ClearAdvisoryLock()
	ls := session.LockSession
	if ls.ReservedId == 0 {
		return nil
	}
	qs, err := txc.queryService(ctx, ls.TabletAlias)
	if err != nil {
		return err
	}
	return qs.Release(ctx, ls.Target, 0, ls.ReservedId)
}

// ReleaseAll releases all the shard sessions and lock session.
func (txc *TxConn) ReleaseAll(ctx context.Context, session *econtext.SafeSession) error {
	if !session.InTransaction() && !session.InReservedConn() && !session.InLockSession() {
		return nil
	}
	defer session.ResetAll()

	allsessions := append(session.PreSessions, session.ShardSessions...)
	allsessions = append(allsessions, session.PostSessions...)
	if session.LockSession != nil {
		allsessions = append(allsessions, session.LockSession)
	}

	return txc.runSessions(ctx, allsessions, session.GetLogger(), func(ctx context.Context, s *vtgatepb.Session_ShardSession, loggging *econtext.ExecuteLogger) error {
		if s.ReservedId == 0 && s.TransactionId == 0 {
			return nil
		}
		qs, err := txc.queryService(ctx, s.TabletAlias)
		if err != nil {
			return err
		}
		err = qs.Release(ctx, s.Target, s.TransactionId, s.ReservedId)
		if err != nil {
			return err
		}
		s.TransactionId = 0
		s.ReservedId = 0
		return nil
	})
}

// ResolveTransactions fetches all unresolved transactions and resolves them.
func (txc *TxConn) ResolveTransactions(ctx context.Context, target *querypb.Target) error {
	transactions, err := txc.tabletGateway.UnresolvedTransactions(ctx, target, 0 /* abandonAgeSeconds */)
	if err != nil {
		return err
	}

	failedResolution := 0
	for _, txRecord := range transactions {
		log.Infof("Resolving transaction ID: %s", txRecord.Dtid)
		err = txc.resolveTx(ctx, target, txRecord)
		if err != nil {
			failedResolution++
			log.Errorf("Failed to resolve transaction ID: %s with error: %v", txRecord.Dtid, err)
		}
	}
	if failedResolution == 0 {
		return nil
	}
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "failed to resolve %d out of %d transactions", failedResolution, len(transactions))
}

// resolveTx resolves the specified distributed transaction.
func (txc *TxConn) resolveTx(ctx context.Context, target *querypb.Target, transaction *querypb.TransactionMetadata) error {
	mmShard, err := dtids.ShardSession(transaction.Dtid)
	if err != nil {
		return err
	}

	switch transaction.State {
	case querypb.TransactionState_PREPARE:
		// If state is PREPARE, make a decision to rollback and
		// fallthrough to the rollback workflow.
		if err = txc.tabletGateway.SetRollback(ctx, target, transaction.Dtid, mmShard.TransactionId); err != nil {
			return err
		}
		fallthrough
	case querypb.TransactionState_ROLLBACK:
		if err = txc.resumeRollback(ctx, target, transaction); err != nil {
			return err
		}
	case querypb.TransactionState_COMMIT:
		if err = txc.resumeCommit(ctx, target, transaction); err != nil {
			return err
		}
	default:
		// Should never happen.
		return vterrors.VT13001(fmt.Sprintf("invalid state: %v", transaction.State))
	}
	return nil
}

// rollbackTx rollbacks the specified distributed transaction.
// Rollbacks happens on the metadata manager and all participants irrespective of the failure.
func (txc *TxConn) rollbackTx(ctx context.Context, dtid string, mmShard *vtgatepb.Session_ShardSession, participants []*vtgatepb.Session_ShardSession, logging *econtext.ExecuteLogger) error {
	var errs []error
	if mmErr := txc.rollbackMM(ctx, dtid, mmShard); mmErr != nil {
		errs = append(errs, mmErr)
	}
	if rmErr := txc.runSessions(ctx, participants, logging, func(ctx context.Context, session *vtgatepb.Session_ShardSession, logger *econtext.ExecuteLogger) error {
		return txc.tabletGateway.RollbackPrepared(ctx, session.Target, dtid, session.TransactionId)
	}); rmErr != nil {
		errs = append(errs, rmErr)
	}
	if err := vterrors.Aggregate(errs); err != nil {
		return err
	}
	return txc.tabletGateway.ConcludeTransaction(ctx, mmShard.Target, dtid)

}

func (txc *TxConn) rollbackMM(ctx context.Context, dtid string, mmShard *vtgatepb.Session_ShardSession) error {
	qs, err := txc.queryService(ctx, mmShard.TabletAlias)
	if err != nil {
		return err
	}
	return qs.SetRollback(ctx, mmShard.Target, dtid, mmShard.TransactionId)
}

func (txc *TxConn) resumeRollback(ctx context.Context, target *querypb.Target, transaction *querypb.TransactionMetadata) error {
	err := txc.runTargets(transaction.Participants, func(t *querypb.Target) error {
		return txc.tabletGateway.RollbackPrepared(ctx, t, transaction.Dtid, 0)
	})
	if err != nil {
		return err
	}
	return txc.tabletGateway.ConcludeTransaction(ctx, target, transaction.Dtid)
}

func (txc *TxConn) resumeCommit(ctx context.Context, target *querypb.Target, transaction *querypb.TransactionMetadata) error {
	err := txc.runTargets(transaction.Participants, func(t *querypb.Target) error {
		return txc.tabletGateway.CommitPrepared(ctx, t, transaction.Dtid)
	})
	if err != nil {
		return err
	}
	return txc.tabletGateway.ConcludeTransaction(ctx, target, transaction.Dtid)
}

// runSessions executes the action for all shardSessions in parallel and returns a consolidated error.
func (txc *TxConn) runSessions(ctx context.Context, shardSessions []*vtgatepb.Session_ShardSession, logging *econtext.ExecuteLogger, action func(context.Context, *vtgatepb.Session_ShardSession, *econtext.ExecuteLogger) error) error {
	// Fastpath.
	if len(shardSessions) == 1 {
		return action(ctx, shardSessions[0], logging)
	}

	allErrors := new(concurrency.AllErrorRecorder)
	var wg sync.WaitGroup
	for _, s := range shardSessions {
		wg.Add(1)
		go func(s *vtgatepb.Session_ShardSession) {
			defer wg.Done()
			if err := action(ctx, s, logging); err != nil {
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

func (txc *TxConn) ReadTransaction(ctx context.Context, transactionID string) (*querypb.TransactionMetadata, error) {
	mmShard, err := dtids.ShardSession(transactionID)
	if err != nil {
		return nil, err
	}
	return txc.tabletGateway.ReadTransaction(ctx, mmShard.Target, transactionID)
}

func (txc *TxConn) UnresolvedTransactions(ctx context.Context, targets []*querypb.Target) ([]*querypb.TransactionMetadata, error) {
	var tmList []*querypb.TransactionMetadata
	var mu sync.Mutex
	err := txc.runTargets(targets, func(target *querypb.Target) error {
		res, err := txc.tabletGateway.UnresolvedTransactions(ctx, target, 0 /* abandonAgeSeconds */)
		if err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		tmList = append(tmList, res...)
		return nil
	})
	return tmList, err
}
