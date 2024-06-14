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

package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// WaitForBoolValue takes a pointer to a boolean and waits for it to reach a certain value.
func WaitForBoolValue(t *testing.T, val *bool, waitFor bool) {
	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			require.Failf(t, "Failed waiting for bool value", "Timed out waiting for the boolean to become %v", waitFor)
			return
		default:
			if *val == waitFor {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
