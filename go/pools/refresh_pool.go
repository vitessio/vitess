/*
Copyright 2022 The Vitess Authors.

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

package pools

import (
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
)

type (
	// RefreshCheck is a function used to determine if a resource pool should be
	// refreshed (i.e. closed and reopened)
	RefreshCheck func() (bool, error)

	// poolRefresh refreshes the pool by calling the RefreshCheck function.
	// If the RefreshCheck returns true, the pool is closed and reopened.
	poolRefresh struct {
		refreshCheck    RefreshCheck
		refreshInterval time.Duration
		refreshTicker   *time.Ticker
		refreshStop     chan struct{}
		refreshWg       sync.WaitGroup

		pool refreshPool
	}
)

type refreshPool interface {
	// reopen drains and reopens the connection pool
	reopen()

	// closeIdleResources scans the pool for idle resources and closes them.
	closeIdleResources()
}

func newPoolRefresh(pool refreshPool, refreshCheck RefreshCheck, refreshInterval time.Duration) *poolRefresh {
	if refreshCheck == nil || refreshInterval <= 0 {
		return nil
	}
	return &poolRefresh{
		pool:            pool,
		refreshInterval: refreshInterval,
		refreshCheck:    refreshCheck,
	}
}

func (pr *poolRefresh) startRefreshTicker() {
	if pr == nil {
		return
	}
	pr.refreshTicker = time.NewTicker(pr.refreshInterval)
	pr.refreshStop = make(chan struct{})
	pr.refreshWg.Add(1)
	go func() {
		defer pr.refreshWg.Done()
		for {
			select {
			case <-pr.refreshTicker.C:
				val, err := pr.refreshCheck()
				if err != nil {
					log.Info(err)
				}
				if val {
					go pr.pool.reopen()
					return
				}
			case <-pr.refreshStop:
				return
			}
		}
	}()
}

func (pr *poolRefresh) stop() {
	if pr == nil || pr.refreshTicker == nil {
		return
	}
	pr.refreshTicker.Stop()
	close(pr.refreshStop)
	pr.refreshWg.Wait()
}
