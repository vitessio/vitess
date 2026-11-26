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
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// TestConfigureRetryerKeepsLegacyBackoff checks that the S3 client keeps the
// retry timing and retry budget that aws-sdk-go-v2 had before v1.48.0 made
// Retry Behavior 2.1 the default, with and without --s3-backup-aws-retries,
// and that the retry mode and max attempts from the AWS config still apply.
func TestConfigureRetryerKeepsLegacyBackoff(t *testing.T) {
	tests := []struct {
		name              string
		retries           int
		mode              aws.RetryMode
		configMaxAttempts int
		wantMaxAttempts   int
		wantAdaptive      bool
		wantClosedConn    bool
	}{
		{
			name:            "SDK default retryer",
			retries:         -1,
			wantMaxAttempts: retry.DefaultMaxAttempts,
		},
		{
			name:            "adaptive retry mode from the AWS config",
			retries:         -1,
			mode:            aws.RetryModeAdaptive,
			wantMaxAttempts: retry.DefaultMaxAttempts,
			wantAdaptive:    true,
		},
		{
			name:              "max attempts from the AWS config",
			retries:           -1,
			configMaxAttempts: 7,
			wantMaxAttempts:   7,
		},
		{
			name:            "--s3-backup-aws-retries",
			retries:         5,
			wantMaxAttempts: 5,
			wantClosedConn:  true,
		},
	}

	// A 500 is retryable and is not a throttling error, so Retry Behavior
	// 2.1 would use its 50ms base delay and charge 14 tokens per retry.
	serverErr := &smithyhttp.ResponseError{
		Response: &smithyhttp.Response{Response: &http.Response{StatusCode: http.StatusInternalServerError}},
		Err:      errors.New("internal error"),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			oldRetryCount := retryCount
			retryCount = test.retries
			t.Cleanup(func() { retryCount = oldRetryCount })

			client := s3.New(s3.Options{
				Region:           "us-east-1",
				RetryMode:        test.mode,
				RetryMaxAttempts: test.configMaxAttempts,
			}, configureRetryer)
			retryer := client.Options().Retryer

			if test.wantAdaptive {
				assert.IsType(t, &retry.AdaptiveMode{}, retryer)
			}
			assert.Equal(t, test.wantMaxAttempts, retryer.MaxAttempts())
			require.True(t, retryer.IsErrorRetryable(serverErr))
			if test.wantClosedConn {
				assert.True(t, retryer.IsErrorRetryable(errors.New("use of closed network connection")))
			}

			// The legacy backoff waits up to 2s before the first retry;
			// Retry Behavior 2.1 waits at most 50ms. With the legacy
			// backoff, the chance that 100 delays all stay at or under
			// 50ms is 0.025^100.
			var longest time.Duration
			for range 100 {
				delay, err := retryer.RetryDelay(1, serverErr)
				require.NoError(t, err)
				longest = max(longest, delay)
			}
			assert.Greater(t, longest, 50*time.Millisecond)

			// The 500-token bucket pays for 100 retries at the legacy cost
			// of 5 tokens, and for 35 at the Retry Behavior 2.1 cost of 14.
			for i := range 100 {
				_, err := retryer.GetRetryToken(t.Context(), serverErr)
				require.NoError(t, err, "retry %d", i+1)
			}
			_, err := retryer.GetRetryToken(t.Context(), serverErr)
			assert.Error(t, err)
		})
	}
}
