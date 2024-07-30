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

package txresolver

import (
	"context"
	"sync"

	"vitess.io/vitess/go/vt/discovery"
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

type TxResolver struct {
	ch     chan *discovery.TabletHealth
	cancel context.CancelFunc

	txConn TxConnection

	mu      sync.Mutex
	resolve map[string]bool
}

type TxConnection interface {
	ResolveTransactions(ctx context.Context, target *querypb.Target) error
}

func NewTxResolver(ch chan *discovery.TabletHealth, txConn TxConnection) *TxResolver {
	return &TxResolver{
		ch:      ch,
		txConn:  txConn,
		resolve: make(map[string]bool),
	}
}

func (tr *TxResolver) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	tr.cancel = cancel

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case th := <-tr.ch:
				if th.Stats != nil && th.Target.TabletType == topodatapb.TabletType_PRIMARY && th.Stats.TxUnresolved {
					go tr.resolveTransactions(ctx, th.Target)
				}
			}
		}
	}()
}

func (tr *TxResolver) Stop() {
	if tr.cancel != nil {
		log.Info("Stopping transaction resolver")
		tr.cancel()
	}
}

func (tr *TxResolver) resolveTransactions(ctx context.Context, target *querypb.Target) {
	dest := target.Keyspace + ":" + target.Shard
	if !tr.tryLockTarget(dest) {
		return
	}
	log.Infof("resolving transactions for shard: %s", dest)

	defer func() {
		tr.mu.Lock()
		delete(tr.resolve, dest)
		tr.mu.Unlock()
	}()
	err := tr.txConn.ResolveTransactions(ctx, target)
	if err != nil {
		log.Errorf("failed to resolve transactions for shard: %s, %v", dest, err)
		return
	}
	log.Infof("successfully resolved all the transactions for shard: %s", dest)
}

func (tr *TxResolver) tryLockTarget(dest string) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.resolve[dest] {
		return false
	}
	tr.resolve[dest] = true
	return true
}
