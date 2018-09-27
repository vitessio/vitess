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

// AllErrorRecorder records all the errors either by appending
// or by a reference position.
//
// It enhances the ErrorRecorder interface to support two methods for
// storing errors:
//
//   RecordError(err)      appends the error to the list
//   RecordErrorAt(err, i) adds the error at the given position
//
// In either case:
//   HasErrors()           returns true if there are any errors in the list
//   Error()       		   returns an aggregated error composed of any non-nil entries
//   AggrError(aggr)       runs the given aggregation to coalesce errors
//   GetErrors(N)          returns an array of size N with either an error or nil for each position
//
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

// RecordErrorAt records a possible error at the given position
// - does nothing if err is nil
func (aer *AllErrorRecorder) RecordErrorAt(i int, err error) {
	if err == nil {
		return
	}
	aer.mu.Lock()
	aer.ensureSize(i + 1)
	aer.Errors[i] = err
	aer.mu.Unlock()
}

// grow the internal array to the given size if needed
func (aer *AllErrorRecorder) ensureSize(n int) {
	if len(aer.Errors) < n {
		errs := make([]error, n)
		if aer.Errors != nil {
			copy(errs, aer.Errors)
		}
		aer.Errors = errs
	}
}

// HasErrors returns true if we ever recorded an error
func (aer *AllErrorRecorder) HasErrors() bool {
	aer.mu.Lock()
	defer aer.mu.Unlock()
	return len(aer.Errors) > 0
}

// GetErrors returns nil if there are no errors or
// an array of size N with any errors.
//
// Note that this will grow the internal storage to the
// given capacity if required, so any future calls to
// RecordError will append to the array _after_ any
// nil entries created by growing the representation.
func (aer *AllErrorRecorder) GetErrors(n int) []error {
	aer.mu.Lock()
	defer aer.mu.Unlock()
	if len(aer.Errors) == 0 {
		return nil
	}
	aer.ensureSize(n)
	return aer.Errors[:n]
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
			if e != nil {
				errs = append(errs, e.Error())
			}
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
		if e != nil {
			errs = append(errs, e.Error())
		}
	}
	return errs
}
