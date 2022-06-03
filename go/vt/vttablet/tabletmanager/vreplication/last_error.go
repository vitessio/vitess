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

package vreplication

import (
	"strings"
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
)

/*
 * lastError tracks the most recent error for any ongoing process and how long it has persisted.
 */

type lastError struct {
	name               string
	lastError          error
	lastErrorStartTime time.Time
	lastErrorMu        sync.Mutex
	maxTimeInError     time.Duration // if error persists for this long, canRetry() will return false
}

func newLastError(name string, maxTimeInError time.Duration) *lastError {
	return &lastError{
		name:           name,
		maxTimeInError: maxTimeInError,
	}
}

func (le *lastError) record(err error) {
	le.lastErrorMu.Lock()
	defer le.lastErrorMu.Unlock()
	if err == nil {
		le.lastError = nil
		le.lastErrorStartTime = time.Time{}
		return
	}
	if le.lastError == nil || !strings.EqualFold(err.Error(), le.lastError.Error()) {
		le.lastErrorStartTime = time.Now()
	}
	le.lastError = err
}

func (le *lastError) canRetry() bool {
	le.lastErrorMu.Lock()
	defer le.lastErrorMu.Unlock()
	if !time.Time.IsZero(le.lastErrorStartTime) && time.Since(le.lastErrorStartTime) > le.maxTimeInError {
		log.Errorf("Got same error since %s, will not retry anymore: you will need to manually restart workflow once error '%s' is fixed",
			le.lastErrorStartTime.UTC(), le.lastError)
		return false
	}
	return true
}
