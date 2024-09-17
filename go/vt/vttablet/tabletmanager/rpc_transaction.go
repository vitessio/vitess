/*
Copyright 2024 The Vitess Authors.

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

package tabletmanager

import (
	"context"

	querypb "vitess.io/vitess/go/vt/proto/query"
	tabletmanagerdatapb "vitess.io/vitess/go/vt/proto/tabletmanagerdata"
)

// GetUnresolvedTransactions returns the unresolved distributed transactions list for the Metadata manager.
func (tm *TabletManager) GetUnresolvedTransactions(ctx context.Context, abandonAgeSeconds int64) ([]*querypb.TransactionMetadata, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}

	tablet := tm.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}
	return tm.QueryServiceControl.UnresolvedTransactions(ctx, target, abandonAgeSeconds)
}

// ReadTransaction returns the transaction metadata for the given distributed transaction ID.
func (tm *TabletManager) ReadTransaction(ctx context.Context, req *tabletmanagerdatapb.ReadTransactionRequest) (*querypb.TransactionMetadata, error) {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return nil, err
	}

	tablet := tm.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}
	return tm.QueryServiceControl.ReadTransaction(ctx, target, req.Dtid)
}

// ConcludeTransaction concludes the given distributed transaction.
func (tm *TabletManager) ConcludeTransaction(ctx context.Context, req *tabletmanagerdatapb.ConcludeTransactionRequest) error {
	if err := tm.waitForGrantsToHaveApplied(ctx); err != nil {
		return err
	}

	tablet := tm.Tablet()
	target := &querypb.Target{Keyspace: tablet.Keyspace, Shard: tablet.Shard, TabletType: tablet.Type}
	if req.Mm {
		return tm.QueryServiceControl.ConcludeTransaction(ctx, target, req.Dtid)
	}
	return tm.QueryServiceControl.RollbackPrepared(ctx, target, req.Dtid, 0)
}
