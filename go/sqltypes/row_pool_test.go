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

package sqltypes

import (
	"testing"

	"github.com/stretchr/testify/require"

	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestRowPoolBudget(t *testing.T) {
	// Small packets get the floor.
	require.Equal(t, RowPoolMinBytes, RowPoolBudget(100, 1, 2))

	// Larger packets get headroom over payload, lengths and per-row cost.
	valueBytes, rowCount, fieldCount := 4<<20, 40_000, 5
	want := RowPoolHeadroom * (valueBytes + rowCount*(fieldCount*8+rowFixedBytes))
	require.Equal(t, want, RowPoolBudget(valueBytes, rowCount, fieldCount))
}

func TestRowPoolPacketBudget(t *testing.T) {
	rows := []*querypb.Row{
		{Lengths: []int64{1, 2}, Values: make([]byte, 3, 1024)},
		nil,
		{Lengths: []int64{1, 2, 3}, Values: make([]byte, 6, 2048)},
	}
	require.Equal(t, RowPoolBudget(9, 3, 3), RowPoolPacketBudget(rows))
}

func TestRowPoolRetained(t *testing.T) {
	require.Zero(t, RowPoolRetained(nil))

	rows := make([]*querypb.Row, 3)
	rows[0] = &querypb.Row{Lengths: make([]int64, 0, 4), Values: make([]byte, 0, 1024)}
	rows[2] = &querypb.Row{Lengths: make([]int64, 0, 2), Values: make([]byte, 0, 2048)}
	// Slots past len still own their buffers after a ResetVT and must count.
	rows = rows[:0]
	require.Equal(t, 1024+4*8+2048+2*8, RowPoolRetained(rows))
}
