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
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"
)

/*
 * lastError tracks the most recent error for any ongoing process and how long it has persisted.
 * The err field should be a vterror so as to ensure we have meaningful error codes, causes, stack
 * traces, etc.
 */
type lastError struct {
	name           string
	err            error
	firstSeen      time.Time
	mu             sync.Mutex
	maxTimeInError time.Duration // if error persists for this long, shouldRetry() will return false
}

func newLastError(name string, maxTimeInError time.Duration) *lastError {
	return &lastError{
		name:           name,
		maxTimeInError: maxTimeInError,
	}
}

func (le *lastError) record(err error) {
	le.mu.Lock()
	defer le.mu.Unlock()
	if err == nil {
		le.err = nil
		le.firstSeen = time.Time{}
		return
	}
	if !vterrors.Equals(err, le.err) {
		le.firstSeen = time.Now()
		le.err = err
	}
	// The error is unchanged so we don't need to do anything
}

func (le *lastError) shouldRetry() bool {
	le.mu.Lock()
	defer le.mu.Unlock()
	if le.maxTimeInError == 0 {
		// The value of 0 means "no time limit"
		return true
	}
	if le.firstSeen.IsZero() {
		return true
	}
	if time.Since(le.firstSeen) <= le.maxTimeInError {
		// within the max time range
		return true
	}
	log.Errorf("VReplication encountered the same error continuously since %s, we will assume this is a non-recoverable error and will not retry anymore; the workflow will need to be manually restarted once error '%s' has been addressed",
		le.firstSeen.UTC(), le.err)
	return false
}
