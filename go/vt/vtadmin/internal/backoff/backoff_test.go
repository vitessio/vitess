/*
Copyright 2022 The Vitess Authors.

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

package backoff

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	grpcbackoff "google.golang.org/grpc/backoff"
)

func TestExponentialBackoff(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		cfg    grpcbackoff.Config
		delays []time.Duration
	}{
		{
			name: "",
			cfg: grpcbackoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 2,
				Jitter:     0,
				MaxDelay:   time.Second * 5,
			},
			delays: []time.Duration{
				time.Second,
				time.Second * 2,
				time.Second * 4,
				time.Second * 5, // multiplying exceeds max

			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			exp := Get("exponential", tt.cfg)
			for retry, expectedDelay := range tt.delays {
				actualDelay := exp.Backoff(retry)
				assert.Equal(t, expectedDelay, actualDelay, "incorrect backoff delay for retry count %v", retry)
			}
		})
	}

	t.Run("jitter never exceeds max delay", func(t *testing.T) {
		t.Parallel()

		exp := Get("exponential", grpcbackoff.Config{
			BaseDelay:  time.Second,
			Multiplier: 1,
			Jitter:     2,
			MaxDelay:   time.Second,
		})
		delays := []time.Duration{
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second,
			time.Second, // you get the idea ...
		}

		for retry, expectedDelay := range delays {
			actualDelay := exp.Backoff(retry)
			assert.GreaterOrEqual(t, expectedDelay, actualDelay, "incorrect backoff delay for retry count %v", retry)
		}
	})
}
