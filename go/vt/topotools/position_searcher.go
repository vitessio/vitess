/*
Copyright 2021 The Vitess Authors.

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

package topotools

import (
	"context"
	"sync"
	"time"

	"vitess.io/vitess/go/mysql"
	"vitess.io/vitess/go/vt/logutil"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vttablet/tmclient"

	topodatapb "vitess.io/vitess/go/vt/proto/topodata"
)

// MaxReplicationPositionSearcher provides a threadsafe way to find a tablet
// with the most advanced replication position.
//
// A typical usage will look like:
//
//		var (
//			searcher = NewMaxReplicationPositionSearcher(tmc, logger, waitTimeout)
//			wg sync.WaitGroup
//		)
//		for _, tablet := range tablets {
//			wg.Add(1)
//			go func(t *topodatapb.Tablet) {
//				defer wg.Done()
//				searcher.ProcessTablet(ctx, t)
//			}(tablet)
//		}
//		wg.Wait()
//		maxPosTablet := searcher.MaxPositionTablet()
//
type MaxReplicationPositionSearcher struct {
	tmc         tmclient.TabletManagerClient
	logger      logutil.Logger
	waitTimeout time.Duration
	m           sync.Mutex

	maxPos       mysql.Position
	maxPosTablet *topodatapb.Tablet
}

// NewMaxReplicationPositionSearcher returns a new
// MaxReplicationPositionSearcher instance, ready to begin processing tablets.
// To reuse an existing instance, first call Reset().
func NewMaxReplicationPositionSearcher(tmc tmclient.TabletManagerClient, logger logutil.Logger, waitTimeout time.Duration) *MaxReplicationPositionSearcher {
	return &MaxReplicationPositionSearcher{
		tmc:          tmc,
		logger:       logger,
		waitTimeout:  waitTimeout,
		m:            sync.Mutex{},
		maxPos:       mysql.Position{},
		maxPosTablet: nil,
	}
}

// ProcessTablet processes the replication position for a single tablet and
// updates the state of the searcher. It is safe to call from multiple
// goroutines.
func (searcher *MaxReplicationPositionSearcher) ProcessTablet(ctx context.Context, tablet *topodatapb.Tablet) {
	searcher.logger.Infof("getting replication position from %v", topoproto.TabletAliasString(tablet.Alias))

	ctx, cancel := context.WithTimeout(ctx, searcher.waitTimeout)
	defer cancel()

	status, err := searcher.tmc.ReplicationStatus(ctx, tablet)
	if err != nil {
		searcher.logger.Warningf("failed to get replication status from %v, ignoring tablet: %v", topoproto.TabletAliasString(tablet.Alias), err)

		return
	}

	pos, err := mysql.DecodePosition(status.Position)
	if err != nil {
		searcher.logger.Warningf("cannot decode replica position %v for tablet %v, ignoring tablet: %v", status.Position, topoproto.TabletAliasString(tablet.Alias), err)

		return
	}

	searcher.m.Lock()
	defer searcher.m.Unlock()

	if searcher.maxPosTablet == nil || !searcher.maxPos.AtLeast(pos) {
		searcher.maxPos = pos
		searcher.maxPosTablet = tablet
	}
}

// MaxPositionTablet returns the most advanced-positioned tablet the searcher
// has seen so far.
func (searcher *MaxReplicationPositionSearcher) MaxPositionTablet() *topodatapb.Tablet {
	searcher.m.Lock()
	defer searcher.m.Unlock()

	return searcher.maxPosTablet
}

// Reset clears any tracked position or tablet from the searcher, making this
// instance ready to begin a new search.
func (searcher *MaxReplicationPositionSearcher) Reset() {
	searcher.m.Lock()
	defer searcher.m.Unlock()

	searcher.maxPos = mysql.Position{}
	searcher.maxPosTablet = nil
}
