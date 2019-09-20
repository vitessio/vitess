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

package txlimiter

import (
	"strings"
	"sync"

	"vitess.io/vitess/go/stats"
	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/log"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

const unknown string = "unknown"

var (
	rejections       = stats.NewCountersWithSingleLabel("TxLimiterRejections", "rejections from TxLimiter", "user")
	rejectionsDryRun = stats.NewCountersWithSingleLabel("TxLimiterRejectionsDryRun", "rejections from TxLimiter in dry run", "user")
)

// TxLimiter is the transaction limiter interface.
type TxLimiter interface {
	Get(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) bool
	Release(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID)
}

// New creates a new TxLimiter.
// slotCount: total slot count in transaction pool
// maxPerUser: fraction of the pool that may be taken by single user
// enabled: should the feature be enabled. If false, will return
// "allow-all" limiter
// dryRun: if true, does no limiting, but records stats of the decisions made
// byXXX: whether given field from immediate/effective caller id should be taken
// into account when deciding "user" identity for purposes of transaction
// limiting.
func New(slotCount int, maxPerUser float64, enabled, dryRun, byUsername, byPrincipal, byComponent, bySubcomponent bool) TxLimiter {
	if !enabled && !dryRun {
		return &TxAllowAll{}
	}

	return &Impl{
		maxPerUser:      int64(float64(slotCount) * maxPerUser),
		dryRun:          dryRun,
		byUsername:      byUsername,
		byPrincipal:     byPrincipal,
		byComponent:     byComponent,
		bySubcomponent:  bySubcomponent,
		byEffectiveUser: byPrincipal || byComponent || bySubcomponent,
		usageMap:        make(map[string]int64),
	}
}

// TxAllowAll is a TxLimiter that allows all Get requests and does no tracking.
// Implements Txlimiter.
type TxAllowAll struct{}

// Get always returns true (allows all requests).
// Implements TxLimiter.Get
func (txa *TxAllowAll) Get(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) bool {
	return true
}

// Release is noop, because TxAllowAll does no tracking.
// Implements TxLimiter.Release
func (txa *TxAllowAll) Release(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) {
	// NOOP
}

// Impl limits the total number of transactions a single user may use
// concurrently.
// Implements TxLimiter.
type Impl struct {
	maxPerUser int64
	usageMap   map[string]int64
	mu         sync.Mutex

	dryRun          bool
	byUsername      bool
	byPrincipal     bool
	byComponent     bool
	bySubcomponent  bool
	byEffectiveUser bool
}

// Get tells whether given user (identified by context.Context) is allowed
// to use another transaction slot. If this method returns true, it's
// necessary to call Release once transaction is returned to the pool.
// Implements TxLimiter.Get
func (txl *Impl) Get(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) bool {
	key := txl.extractKey(immediate, effective)

	txl.mu.Lock()
	defer txl.mu.Unlock()

	usage := txl.usageMap[key]
	if usage < txl.maxPerUser {
		txl.usageMap[key] = usage + 1
		return true
	}

	if txl.dryRun {
		log.Infof("TxLimiter: DRY RUN: user over limit: %s", key)
		rejectionsDryRun.Add(key, 1)
		return true
	}

	log.Infof("TxLimiter: Over limit, rejecting transaction request for user: %s", key)
	rejections.Add(key, 1)
	return false
}

// Release marks that given user (identified by caller ID) is no longer using
// a transaction slot.
// Implements TxLimiter.Release
func (txl *Impl) Release(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) {
	key := txl.extractKey(immediate, effective)

	txl.mu.Lock()
	defer txl.mu.Unlock()

	usage, ok := txl.usageMap[key]
	if !ok {
		return
	}
	if usage == 1 {
		delete(txl.usageMap, key)
		return
	}

	txl.usageMap[key] = usage - 1
}

// extractKey builds a string key used to differentiate users, based
// on fields specified in configuration and their values from caller ID.
func (txl *Impl) extractKey(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) string {
	var parts []string
	if txl.byUsername {
		if immediate != nil {
			parts = append(parts, callerid.GetUsername(immediate))
		} else {
			parts = append(parts, unknown)
		}
	}
	if txl.byEffectiveUser {
		if effective != nil {
			if txl.byPrincipal {
				parts = append(parts, callerid.GetPrincipal(effective))
			}
			if txl.byComponent {
				parts = append(parts, callerid.GetComponent(effective))
			}
			if txl.bySubcomponent {
				parts = append(parts, callerid.GetSubcomponent(effective))
			}
		} else {
			parts = append(parts, unknown)
		}
	}

	return strings.Join(parts, "/")
}
