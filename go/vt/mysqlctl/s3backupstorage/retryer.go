package s3backupstorage

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
)

// ClosedConnectionRetryer implements the aws request.Retryer interface
// and is used to retry closed connection errors during MultipartUpload
// operations.
type ClosedConnectionRetryer struct {
	awsRetryer request.Retryer
}

// RetryRules is part of the Retryer interface. It defers to the underlying
// aws Retryer to compute backoff rules.
func (retryer *ClosedConnectionRetryer) RetryRules(r *request.Request) time.Duration {
	return retryer.awsRetryer.RetryRules(r)
}

// ShouldRetry is part of the Retryer interface. It retries on errors that occur
// due to a closed network connection, and then falls back to the underlying aws
// Retryer for checking additional retry conditions.
func (retryer *ClosedConnectionRetryer) ShouldRetry(r *request.Request) bool {
	if retryer.MaxRetries() == 0 {
		return false
	}

	if r.Retryable != nil {
		return *r.Retryable
	}

	if r.Error != nil {
		if awsErr, ok := r.Error.(awserr.Error); ok {
			if strings.Contains(awsErr.Error(), "use of closed network connection") {
				return true
			}
		}
	}

	return retryer.awsRetryer.ShouldRetry(r)
}

// MaxRetries is part of the Retryer interface. It defers to the
// underlying aws Retryer for the max number of retries.
func (retryer *ClosedConnectionRetryer) MaxRetries() int {
	return retryer.awsRetryer.MaxRetries()
}
