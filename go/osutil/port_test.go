/*
Copyright 2026 The Vitess Authors.

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

package osutil

import (
	"net"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetPortReservation_Single(t *testing.T) {
	pr, err := GetPortReservation(1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, UnreservePorts(pr)) })
	require.Greater(t, pr.Start, 0)
	require.Less(t, pr.Start, 65536)
	assert.Equal(t, pr.Start, pr.End)

	// Second call returns a different port.
	pr2, err := GetPortReservation(1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, UnreservePorts(pr2)) })
	assert.NotEqual(t, pr.Start, pr2.Start)
}

func TestGetPortReservation_Consecutive(t *testing.T) {
	pr, err := GetPortReservation(6)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, UnreservePorts(pr)) })
	require.Greater(t, pr.Start, 0)
	require.Less(t, pr.Start, 65536)
	assert.Equal(t, pr.Start+5, pr.End)

	// All 6 consecutive ports should be bindable.
	for i := range 6 {
		ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(pr.Start+i)))
		require.NoError(t, err)
		ln.Close()
	}
}

func TestGetPortReservation_ErrorOnZero(t *testing.T) {
	_, err := GetPortReservation(0)
	assert.Error(t, err)
}

func TestGetPortReservation_ErrorOnNegative(t *testing.T) {
	_, err := GetPortReservation(-1)
	assert.Error(t, err)
}

func TestGetPortReservation_WithPortFile(t *testing.T) {
	origPath := portFilePath
	origRanges := allocatedRanges
	t.Cleanup(func() {
		portMu.Lock()
		defer portMu.Unlock()
		portFilePath = origPath
		allocatedRanges = origRanges
	})

	portMu.Lock()
	portFilePath = filepath.Join(t.TempDir(), "test_ports.txt")
	allocatedRanges = nil
	portMu.Unlock()

	pr, err := GetPortReservation(1)
	require.NoError(t, err)

	pr2, err := GetPortReservation(1)
	require.NoError(t, err)
	assert.NotEqual(t, pr.Start, pr2.Start)

	require.NoError(t, UnreservePorts(pr))
	require.NoError(t, UnreservePorts(pr2))
}

func TestUnreservePorts(t *testing.T) {
	origRanges := allocatedRanges
	t.Cleanup(func() {
		portMu.Lock()
		defer portMu.Unlock()
		allocatedRanges = origRanges
	})

	portMu.Lock()
	allocatedRanges = nil
	portMu.Unlock()

	pr, err := GetPortReservation(6)
	require.NoError(t, err)
	require.Greater(t, pr.Start, 0)

	portMu.Lock()
	require.Len(t, allocatedRanges, 1)
	portMu.Unlock()

	require.NoError(t, UnreservePorts(pr))

	portMu.Lock()
	assert.Empty(t, allocatedRanges)
	portMu.Unlock()
}

func TestUnreservePorts_WithPortFile(t *testing.T) {
	origPath := portFilePath
	origRanges := allocatedRanges
	t.Cleanup(func() {
		portMu.Lock()
		defer portMu.Unlock()
		portFilePath = origPath
		allocatedRanges = origRanges
	})

	portMu.Lock()
	portFilePath = filepath.Join(t.TempDir(), "test_ports.txt")
	allocatedRanges = nil
	portMu.Unlock()

	pr, err := GetPortReservation(6)
	require.NoError(t, err)
	require.Greater(t, pr.Start, 0)

	portMu.Lock()
	require.Len(t, allocatedRanges, 1)
	portMu.Unlock()

	require.NoError(t, UnreservePorts(pr))

	portMu.Lock()
	assert.Empty(t, allocatedRanges)
	portMu.Unlock()
}
