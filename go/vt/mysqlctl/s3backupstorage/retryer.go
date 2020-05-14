package s3backupstorage

import (
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
)

type ClosedConnectionRetryer struct {
	awsRetryer request.Retryer
}

func (retryer *ClosedConnectionRetryer) RetryRules(r *request.Request) time.Duration {
	return retryer.awsRetryer.RetryRules(r)
}

func (retryer *ClosedConnectionRetryer) ShouldRetry(r *request.Request) bool {
	if retryer.MaxRetries() == 0 {
		return false
	}

	if r.Retryable != nil {
		return *r.Retryable
	}

	if r.Error != nil {
		if awsErr, ok := r.Error.(awserr.Error); ok {
			return strings.Contains(awsErr.OrigErr().Error(), "use of closed network connection")
		}
	}

	return retryer.awsRetryer.ShouldRetry(r)
}

func (retryer *ClosedConnectionRetryer) MaxRetries() int {
	return retryer.awsRetryer.MaxRetries()
}
