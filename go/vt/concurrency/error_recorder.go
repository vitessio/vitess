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

package concurrency

import (
	"fmt"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/log"
)

// ErrorRecorder offers a way to record errors during complex
// asynchronous operations.  Various implementation will offer
// different services.
type ErrorRecorder interface {
	RecordError(error)
	HasErrors() bool
	Error() error
}

// FirstErrorRecorder records the first error, logs the others.
// Error() will return the first recorded error or nil.
type FirstErrorRecorder struct {
	mu         sync.Mutex
	errorCount int
	firstError error
}

// RecordError records a possible error:
// - does nothing if err is nil
// - only records the first error reported
// - the rest is just logged
func (fer *FirstErrorRecorder) RecordError(err error) {
	if err == nil {
		return
	}
	fer.mu.Lock()
	fer.errorCount++
	if fer.errorCount == 1 {
		fer.firstError = err
	} else {
		log.Errorf("FirstErrorRecorder: error[%v]: %v", fer.errorCount, err)
	}
	fer.mu.Unlock()
}

// HasErrors returns true if we ever recorded an error
func (fer *FirstErrorRecorder) HasErrors() bool {
	fer.mu.Lock()
	defer fer.mu.Unlock()
	return fer.errorCount > 0
}

// Error returns the first error we saw, or nil
func (fer *FirstErrorRecorder) Error() error {
	fer.mu.Lock()
	defer fer.mu.Unlock()
	return fer.firstError
}

// AllErrorRecorder records all the errors.
type AllErrorRecorder struct {
	mu     sync.Mutex
	Errors []error
}

// RecordError records a possible error:
// - does nothing if err is nil
func (aer *AllErrorRecorder) RecordError(err error) {
	if err == nil {
		return
	}
	aer.mu.Lock()
	aer.Errors = append(aer.Errors, err)
	aer.mu.Unlock()
}

// HasErrors returns true if we ever recorded an error
func (aer *AllErrorRecorder) HasErrors() bool {
	aer.mu.Lock()
	defer aer.mu.Unlock()
	return len(aer.Errors) > 0
}

// AllErrorAggregator aggregates errors.
type AllErrorAggregator func(errors []error) error

// AggrError runs the provided aggregator over all errors
// and returns the error from aggregator.
func (aer *AllErrorRecorder) AggrError(aggr AllErrorAggregator) error {
	aer.mu.Lock()
	defer aer.mu.Unlock()
	if len(aer.Errors) == 0 {
		return nil
	}
	return aggr(aer.Errors)
}

// Error returns an aggregate of all errors by concatenation.
func (aer *AllErrorRecorder) Error() error {
	return aer.AggrError(func(errors []error) error {
		errs := make([]string, 0, len(errors))
		for _, e := range errors {
			errs = append(errs, e.Error())
		}
		return fmt.Errorf("%v", strings.Join(errs, ";"))
	})
}

// ErrorStrings returns all errors as string array.
func (aer *AllErrorRecorder) ErrorStrings() []string {
	aer.mu.Lock()
	defer aer.mu.Unlock()
	if len(aer.Errors) == 0 {
		return nil
	}
	errs := make([]string, 0, len(aer.Errors))
	for _, e := range aer.Errors {
		errs = append(errs, e.Error())
	}
	return errs
}
