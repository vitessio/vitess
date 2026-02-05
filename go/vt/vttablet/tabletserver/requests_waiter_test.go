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

package tabletserver

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRequestWaiter tests the functionality of request waiter.
func TestRequestWaiter(t *testing.T) {
	rw := newRequestsWaiter()
	require.EqualValues(t, 0, rw.GetWaiterCount())
	require.EqualValues(t, 0, rw.GetOutstandingRequestsCount())

	rw.Add(3)
	require.EqualValues(t, 0, rw.GetWaiterCount())
	require.EqualValues(t, 3, rw.GetOutstandingRequestsCount())

	rw.Done()
	require.EqualValues(t, 0, rw.GetWaiterCount())
	require.EqualValues(t, 2, rw.GetOutstandingRequestsCount())

	go func() {
		rw.WaitToBeEmpty()
	}()
	go func() {
		rw.WaitToBeEmpty()
	}()

	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 2, rw.GetWaiterCount())
	require.EqualValues(t, 2, rw.GetOutstandingRequestsCount())

	rw.Done()
	rw.Done()

	time.Sleep(100 * time.Millisecond)
	require.EqualValues(t, 0, rw.GetWaiterCount())
	require.EqualValues(t, 0, rw.GetOutstandingRequestsCount())
}
