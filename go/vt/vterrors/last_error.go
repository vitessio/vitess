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

package vterrors

import (
	"sync"
	"time"

	"vitess.io/vitess/go/vt/log"
)

/*
 * LastError tracks the most recent error for any ongoing process and how long it has persisted.
 * The err field should be a vterror to ensure we have meaningful error codes, causes, stack
 * traces, etc.
 */
type LastError struct {
	name           string
	err            error
	firstSeen      time.Time
	lastSeen       time.Time
	mu             sync.Mutex
	maxTimeInError time.Duration // if error persists for this long, shouldRetry() will return false
}

func NewLastError(name string, maxTimeInError time.Duration) *LastError {
	log.Infof("Created last error: %s, with maxTimeInError: %s", name, maxTimeInError)
	return &LastError{
		name:           name,
		maxTimeInError: maxTimeInError,
	}
}

func (le *LastError) Record(err error) {
	le.mu.Lock()
	defer le.mu.Unlock()
	if err == nil {
		log.Infof("Resetting last error: %s", le.name)
		le.err = nil
		le.firstSeen = time.Time{}
		le.lastSeen = time.Time{}
		return
	}
	if !Equals(err, le.err) {
		log.Infof("Got new last error %+v for %s, was %+v", err, le.name, le.err)
		le.firstSeen = time.Now()
		le.lastSeen = time.Now()
		le.err = err
	} else {
		// same error seen
		log.Infof("Got the same last error for %q: %+v ; first seen at %s and last seen %dms ago", le.name, le.err, le.firstSeen, int(time.Since(le.lastSeen).Milliseconds()))
		if time.Since(le.lastSeen) > le.maxTimeInError {
			// reset firstSeen, since it has been long enough since the last time we saw this error
			log.Infof("Resetting firstSeen for %s, since it is too long since the last one", le.name)
			le.firstSeen = time.Now()
		}
		le.lastSeen = time.Now()
	}
}

func (le *LastError) ShouldRetry() bool {
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
	log.Errorf("%s: the same error was encountered continuously since %s, it is now assumed to be unrecoverable; any affected operations will need to be manually restarted once error '%s' has been addressed",
		le.name, le.firstSeen.UTC(), le.err)
	return false
}
