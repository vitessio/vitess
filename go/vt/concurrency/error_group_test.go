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

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestErrorGroup(t *testing.T) {
	testcases := []struct {
		name                string
		errorGroup          ErrorGroup
		numSuccesses        int
		numErrors           int
		numWaitFor          int
		numDelayedSuccesses int
		expectedError       string
	}{
		{
			name: "Wait() returns immediately when NumGoroutines = 0",
			errorGroup: ErrorGroup{
				NumGoroutines: 0,
			},
		}, {
			name: "Require all successes - pass",
			errorGroup: ErrorGroup{
				NumGoroutines:        4,
				NumRequiredSuccesses: 4,
			},
			numSuccesses: 4,
		}, {
			name: "Require all successes - failure",
			errorGroup: ErrorGroup{
				NumGoroutines:        4,
				NumRequiredSuccesses: 4,
			},
			numSuccesses:  3,
			numErrors:     1,
			expectedError: "a general error",
		}, {
			name: "1 allowed failure",
			errorGroup: ErrorGroup{
				NumGoroutines:        4,
				NumRequiredSuccesses: 3,
				NumAllowedErrors:     1,
			},
			numSuccesses:  3,
			numErrors:     1,
			expectedError: "a general error",
		}, {
			name: "less than allowed failures",
			errorGroup: ErrorGroup{
				NumGoroutines:        4,
				NumRequiredSuccesses: 2,
				NumAllowedErrors:     2,
			},
			numSuccesses:  3,
			numErrors:     1,
			expectedError: "a general error",
		}, {
			name: "1 must wait for routine",
			errorGroup: ErrorGroup{
				NumGoroutines:        4,
				NumRequiredSuccesses: 2,
				NumAllowedErrors:     2,
				NumErrorsToWaitFor:   1,
			},
			numSuccesses: 3,
			numWaitFor:   1,
		}, {
			name: "delayed success should be cancelled",
			errorGroup: ErrorGroup{
				NumGoroutines:        4,
				NumRequiredSuccesses: 2,
				NumAllowedErrors:     2,
			},
			numSuccesses:        3,
			numDelayedSuccesses: 1,
			expectedError:       "context cancelled",
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.name, func(t *testing.T) {
			groupContext, groupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer groupCancel()
			errCh := make(chan Error)
			defer close(errCh)

			spawnGoRoutines(errCh, false, testcase.numSuccesses)
			spawnGoRoutines(errCh, true, testcase.numErrors)
			spawnDelayedGoRoutine(groupContext, errCh, true, testcase.numWaitFor)
			spawnDelayedGoRoutine(groupContext, errCh, false, testcase.numDelayedSuccesses)

			err := testcase.errorGroup.Wait(groupCancel, errCh)
			if testcase.expectedError == "" {
				require.False(t, err.HasErrors())
				require.NoError(t, err.Error())
			} else {
				require.True(t, err.HasErrors())
				require.EqualError(t, err.Error(), testcase.expectedError)
			}
		})
	}
}

func spawnGoRoutines(errCh chan Error, shouldError bool, count int) {
	for i := 0; i < count; i++ {
		go func() {
			time.Sleep(100 * time.Millisecond)
			var err Error
			if shouldError {
				err.Err = fmt.Errorf("a general error")
			}
			errCh <- err
		}()
	}
}

func spawnDelayedGoRoutine(groupContext context.Context, errCh chan Error, mustWaitFor bool, count int) {
	for i := 0; i < count; i++ {
		go func() {
			select {
			case <-groupContext.Done():
				err := Error{
					Err: fmt.Errorf("context cancelled"),
				}
				errCh <- err
			case <-time.After(300 * time.Millisecond):
				err := Error{
					MustWaitFor: mustWaitFor,
				}
				errCh <- err
			}
		}()
	}
}
