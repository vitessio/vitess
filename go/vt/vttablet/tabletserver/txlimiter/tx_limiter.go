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

package txlimiter

import (
	"strings"
	"sync"

	log "github.com/golang/glog"
	"golang.org/x/net/context"

	"github.com/youtube/vitess/go/stats"
	"github.com/youtube/vitess/go/vt/callerid"

	querypb "github.com/youtube/vitess/go/vt/proto/query"
	vtrpcpb "github.com/youtube/vitess/go/vt/proto/vtrpc"
)

const unknown string = "unknown"

var (
	rejections       = stats.NewCounters("TxLimiterRejections")
	rejectionsDryRun = stats.NewCounters("TxLimiterRejectionsDryRun")
)

type TxLimiter interface {
	Get(ctx context.Context) bool
	Release(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID)
}

func New(slotCount int, maxPerUser float64, enabled, dryRun, byUsername, byPrincipal, byComponent, bySubcomponent bool) TxLimiter {
	if !enabled {
		return &TxAllowAll{}
	}

	return &TxLimiterImpl{
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

type TxAllowAll struct{}

func (txa *TxAllowAll) Get(ctx context.Context) bool {
	return true
}

func (txa *TxAllowAll) Release(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) {
	// NOOP
}

type TxLimiterImpl struct {
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

func (txl *TxLimiterImpl) Get(ctx context.Context) bool {
	key := txl.extractKeyFromContext(ctx)

	txl.mu.Lock()
	defer txl.mu.Unlock()

	usage, ok := txl.usageMap[key]
	if !ok {
		txl.usageMap[key] = 1
		return true
	}

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

func (txl *TxLimiterImpl) Release(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) {
	key := txl.extractKeyFromCallers(immediate, effective)

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

func (txl *TxLimiterImpl) extractKeyFromContext(ctx context.Context) string {
	return txl.extractKeyFromCallers(
		callerid.ImmediateCallerIDFromContext(ctx),
		callerid.EffectiveCallerIDFromContext(ctx))
}

func (txl *TxLimiterImpl) extractKeyFromCallers(immediate *querypb.VTGateCallerID, effective *vtrpcpb.CallerID) string {
	var parts []string
	if txl.byUsername {
		if immediate != nil {
			parts = append(parts, immediate.Username)
		} else {
			parts = append(parts, unknown)
		}
	}
	if txl.byEffectiveUser {
		if effective != nil {
			if txl.byPrincipal {
				parts = append(parts, effective.Principal)
			}
			if txl.byComponent {
				parts = append(parts, effective.Component)
			}
			if txl.bySubcomponent {
				parts = append(parts, effective.Subcomponent)
			}
		} else {
			parts = append(parts, unknown)
		}
	}

	return strings.Join(parts, "/")
}
