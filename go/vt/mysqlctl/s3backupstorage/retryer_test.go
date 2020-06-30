package s3backupstorage

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/stretchr/testify/assert"
)

type testRetryer struct{}

func (r *testRetryer) MaxRetries() int                               { return 5 }
func (r *testRetryer) RetryRules(req *request.Request) time.Duration { return time.Second }
func (r *testRetryer) ShouldRetry(req *request.Request) bool         { return false }

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		name     string
		r        *request.Request
		expected bool
	}{

		{
			name: "non retryable request",
			r: &request.Request{
				Retryable: aws.Bool(false),
			},
			expected: false,
		},
		{
			name: "retryable request",
			r: &request.Request{
				Retryable: aws.Bool(true),
			},
			expected: true,
		},
		{
			name: "non aws error",
			r: &request.Request{
				Retryable: nil,
				Error:     errors.New("some error"),
			},
			expected: false,
		},
		{
			name: "closed connection error",
			r: &request.Request{
				Retryable: nil,
				Error:     awserr.New("5xx", "use of closed network connection", nil),
			},
			expected: true,
		},
		{
			name: "closed connection error (non nil origError)",
			r: &request.Request{
				Retryable: nil,
				Error:     awserr.New("5xx", "use of closed network connection", errors.New("some error")),
			},
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retryer := &ClosedConnectionRetryer{&testRetryer{}}
			msg := ""
			if test.r.Error != nil {
				if awsErr, ok := test.r.Error.(awserr.Error); ok {
					msg = awsErr.Error()
				}
			}
			assert.Equal(t, test.expected, retryer.ShouldRetry(test.r), msg)
		})
	}
}
