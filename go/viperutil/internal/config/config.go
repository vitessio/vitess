/*
Copyright 2023 The Vitess Authors.

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

package config

import (
	"context"
	"time"

	"vitess.io/vitess/go/viperutil/internal/log"
	"vitess.io/vitess/go/viperutil/internal/registry"
)

var (
	ch chan struct{}
)

// PersistChanges starts a background goroutine to persist changes made to the
// dynamic registry in-memory (i.e. the "live" config) back to disk (i.e. the
// "disk" config) are persisted back to disk. It returns a cancel func to stop
// the persist loop, which the caller is responsible for calling during
// shutdown (see package servenv for an example).
//
// This does two things — one which is a nice-to-have, and another which is
// necessary for correctness.
//
// 1. Writing in-memory changes (which usually occur through a request to a
// /debug/env endpoint) ensures they are persisted across process restarts.
// 2. Writing in-memory changes ensures that subsequent modifications to the
// config file do not clobber those changes. Because viper loads the entire
// config on-change, rather than an incremental (diff) load, if a user were to
// edit an unrelated key (keyA) in the file, and we did not persist the
// in-memory change (keyB), then future calls to keyB.Get() would return the
// older value.
func PersistChanges(ctx context.Context, minWaitInterval time.Duration) context.CancelFunc {
	if ch != nil {
		panic("PersistChanges already called")
	}

	ch = make(chan struct{}, 1)

	var timer *time.Timer
	if minWaitInterval > 0 {
		timer = time.NewTimer(minWaitInterval)
	}

	persistOnce := func() {
		if err := registry.Dynamic.WriteConfig(); err != nil {
			log.ERROR("failed to persist config changes back to disk: %s", err.Error())
			// If we failed to persist, don't wait the entire interval before
			// writing again, instead writing immediately on the next request.
			if timer != nil {
				if !timer.Stop() {
					<-timer.C
				}

				timer = nil
			}
		}

		switch {
		case minWaitInterval == 0:
			return
		case timer == nil:
			timer = time.NewTimer(minWaitInterval)
		default:
			timer.Reset(minWaitInterval)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				if timer == nil {
					persistOnce()
					continue
				}

				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					persistOnce()
				}
			}
		}
	}()

	return cancel
}

// NotifyChanged signals to the persist loop started by PersistChanges() that
// something in the config has changed, and should be persisted soon.
func NotifyChanged() {
	if ch == nil {
		return
	}

	select {
	case ch <- struct{}{}:
	default:
	}
}
