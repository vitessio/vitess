/*
Copyright 2024 The Vitess Authors.

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

package s3backupstorage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testRetryer struct{ retry bool }

func (r *testRetryer) GetInitialToken() (releaseToken func(error) error) { panic("implement me") }
func (r *testRetryer) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	panic("implement me")
}
func (r *testRetryer) IsErrorRetryable(err error) bool { return r.retry }
func (r *testRetryer) MaxAttempts() int                { return 5 }
func (r *testRetryer) RetryDelay(attempt int, opErr error) (time.Duration, error) {
	return time.Second, nil
}

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		name           string
		err            error
		fallbackPolicy bool
		expected       bool
	}{
		{
			name:           "no error",
			fallbackPolicy: false,
			expected:       false,
		},
		{
			name:           "non aws error",
			err:            errors.New("some error"),
			fallbackPolicy: false,
			expected:       false,
		},
		{
			name:           "closed connection error",
			err:            errors.New("use of closed network connection"),
			fallbackPolicy: false,
			expected:       true,
		},
		{
			name:           "other aws error hits fallback policy",
			err:            errors.New("not a closed network connection"),
			fallbackPolicy: true,
			expected:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retryer := &ClosedConnectionRetryer{&testRetryer{test.fallbackPolicy}}
			assert.Equal(t, test.expected, retryer.IsErrorRetryable(test.err))
		})
	}
}
