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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// ClosedConnectionRetryer implements the aws.Retryer interface
// and is used to retry closed connection errors during MultipartUpload
// operations. It is a simplified version of the RetryableConnectionError
// implementation, which always retry on any type of connection error.
type ClosedConnectionRetryer struct {
	awsRetryer aws.Retryer
}

// IsErrorRetryable returns true if the error should be retried. We first try
// to see if the error is due to the use of a closed connection, if it is,
// we retry, and if not, we default to what the aws.Retryer would do.
func (retryer *ClosedConnectionRetryer) IsErrorRetryable(err error) bool {
	if retryer.MaxAttempts() == 0 {
		return false
	}

	if err != nil {
		if strings.Contains(err.Error(), "use of closed network connection") {
			return true
		}
	}

	return retryer.awsRetryer.IsErrorRetryable(err)
}

// MaxAttempts returns the maximum number of attempts that can be made for
// an attempt before failing. A value of 0 implies that the attempt should
// be retried until it succeeds if the errors are retryable.
func (retryer *ClosedConnectionRetryer) MaxAttempts() int {
	return retryer.awsRetryer.MaxAttempts()
}

// RetryDelay returns the delay that should be used before retrying the
// attempt. Will return error if the delay could not be determined.
func (retryer *ClosedConnectionRetryer) RetryDelay(attempt int, opErr error) (time.Duration, error) {
	return retryer.awsRetryer.RetryDelay(attempt, opErr)
}

// GetRetryToken attempts to deduct the retry cost from the retry token pool.
// Returning the token release function, or error.
func (retryer *ClosedConnectionRetryer) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	return retryer.awsRetryer.GetRetryToken(ctx, opErr)
}

// GetInitialToken returns the initial attempt token that can increment the
// retry token pool if the attempt is successful.
func (retryer *ClosedConnectionRetryer) GetInitialToken() (releaseToken func(error) error) {
	return retryer.awsRetryer.GetInitialToken()
}
