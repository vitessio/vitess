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

type testRetryer struct{ retry bool }

func (r *testRetryer) MaxRetries() int                               { return 5 }
func (r *testRetryer) RetryRules(req *request.Request) time.Duration { return time.Second }
func (r *testRetryer) ShouldRetry(req *request.Request) bool         { return r.retry }

func TestShouldRetry(t *testing.T) {
	tests := []struct {
		name           string
		r              *request.Request
		fallbackPolicy bool
		expected       bool
	}{

		{
			name: "non retryable request",
			r: &request.Request{
				Retryable: aws.Bool(false),
			},
			fallbackPolicy: false,
			expected:       false,
		},
		{
			name: "retryable request",
			r: &request.Request{
				Retryable: aws.Bool(true),
			},
			fallbackPolicy: false,
			expected:       true,
		},
		{
			name: "non aws error",
			r: &request.Request{
				Retryable: nil,
				Error:     errors.New("some error"),
			},
			fallbackPolicy: false,
			expected:       false,
		},
		{
			name: "closed connection error",
			r: &request.Request{
				Retryable: nil,
				Error:     awserr.New("5xx", "use of closed network connection", nil),
			},
			fallbackPolicy: false,
			expected:       true,
		},
		{
			name: "closed connection error (non nil origError)",
			r: &request.Request{
				Retryable: nil,
				Error:     awserr.New("5xx", "use of closed network connection", errors.New("some error")),
			},
			fallbackPolicy: false,
			expected:       true,
		},
		{
			name: "other aws error hits fallback policy",
			r: &request.Request{
				Retryable: nil,
				Error:     awserr.New("code", "not a closed network connectionn", errors.New("some error")),
			},
			fallbackPolicy: true,
			expected:       true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retryer := &ClosedConnectionRetryer{&testRetryer{test.fallbackPolicy}}
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
