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

package concurrency

import "context"

// ErrorGroup provides a function for waiting for N goroutines to complete with
// at least X successes and no more than Y failures, and cancelling the rest.
//
// It should be used as follows:
//
//		errCh := make(chan error)
//		errgroupCtx, errgroupCancel := context.WithCancel(ctx)
//
//		for _, arg := range args {
//			arg := arg
//
//			go func() {
//				err := doWork(errGroupCtx, arg)
//				errCh <- err
//			}()
//		}
//
//		errgroup := concurrency.ErrorGroup{
//			NumGoroutines: len(args),
//			NumRequiredSuccess: 5, // need at least 5 to respond with nil error before cancelling the rest
//			NumAllowedErrors: 1, // if more than 1 responds with non-nil error, cancel the rest
//		}
//		errRec := errgroup.Wait(errgroupCancel, errCh)
//
//		if errRec.HasErrors() {
//			// ...
//		}
type ErrorGroup struct {
	NumGoroutines        int
	NumRequiredSuccesses int
	NumAllowedErrors     int
}

// Wait waits for a group of goroutines that are sending errors to the given
// error channel, and are cancellable by the given cancel function.
//
// Wait will cancel any outstanding goroutines under the following conditions:
//
// (1) More than NumAllowedErrors non-nil results have been consumed on the
// error channel.
//
// (2) At least NumRequiredSuccesses nil results have been consumed on the error
// channel.
//
// After the cancellation condition is triggered, Wait will continue to consume
// results off the error channel so as to not permanently block any of those
// cancelled goroutines.
//
// When finished consuming results from all goroutines, cancelled or otherwise,
// Wait returns an AllErrorRecorder that contains all errors returned by any of
// those goroutines. It does not close the error channel.
func (eg ErrorGroup) Wait(cancel context.CancelFunc, errors chan error) *AllErrorRecorder {
	errCounter := 0
	successCounter := 0
	responseCounter := 0
	rec := &AllErrorRecorder{}

	if eg.NumGoroutines < 1 {
		return rec
	}

	for err := range errors {
		responseCounter++

		switch err {
		case nil:
			successCounter++
		default:
			errCounter++
			rec.RecordError(err)
		}

		// Even though we cancel in the next conditional, we need to keep
		// consuming off the channel, or those goroutines will get stuck
		// forever.
		if responseCounter == eg.NumGoroutines {
			break
		}

		if errCounter > eg.NumAllowedErrors || successCounter >= eg.NumRequiredSuccesses {
			cancel()
		}
	}

	return rec
}
