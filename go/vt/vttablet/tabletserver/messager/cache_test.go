/*
Copyright 2019 The Vitess Authors.

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

package messager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
)

func TestMessagerCacheOrder(t *testing.T) {
	mc := newCache(10)
	require.True(t, mc.Add(&MessageRow{
		Priority: 1,
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	require.True(t, mc.Add(&MessageRow{
		Priority: 1,
		TimeNext: 2,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row02")},
	}), "Add returned false")
	require.True(t, mc.Add(&MessageRow{
		Priority: 2,
		TimeNext: 2,
		Epoch:    1,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row12")},
	}), "Add returned false")
	require.True(t, mc.Add(&MessageRow{
		Priority: 2,
		TimeNext: 1,
		Epoch:    1,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row11")},
	}), "Add returned false")
	require.True(t, mc.Add(&MessageRow{
		Priority: 1,
		TimeNext: 3,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row03")},
	}), "Add returned false")
	var rows []string
	for range 5 {
		rows = append(rows, mc.Pop().Row[0].ToString())
	}
	want := []string{
		"row03",
		"row02",
		"row01",
		"row12",
		"row11",
	}
	assert.Equalf(t, want, rows, "Pop order: %+v, want %+v", rows, want)
}

func TestMessagerCacheDupKey(t *testing.T) {
	mc := newCache(10)
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	assert.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add(dup): returned false, want true")
	_ = mc.Pop()
	assert.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add(dup): returned false, want true")
	mc.Discard([]string{"row01"})
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
}

func TestMessagerCacheDiscard(t *testing.T) {
	mc := newCache(10)
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	mc.Discard([]string{"row01"})
	if row := mc.Pop(); row != nil {
		assert.Failf(t, "Pop", "want nil, got %v", row.Row[0])
	}
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	if row := mc.Pop(); row == nil || row.Row[0].ToString() != "row01" {
		assert.Failf(t, "Pop", "want row01, got %v", row)
	}

	// Add will be a no-op.
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	if row := mc.Pop(); row != nil {
		assert.Failf(t, "Pop", "want nil, got %v", row.Row[0])
	}
	mc.Discard([]string{"row01"})

	// Now we can add.
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	if row := mc.Pop(); row == nil || row.Row[0].ToString() != "row01" {
		assert.Failf(t, "Pop", "want row01, got %v", row)
	}
}

func TestMessagerCacheFull(t *testing.T) {
	mc := newCache(2)
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row02")},
	}), "Add returned false")
	assert.False(t, mc.Add(&MessageRow{
		TimeNext: 2,
		Epoch:    1,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row12")},
	}), "Add(full): returned true, want false")
}

func TestMessagerCacheEmpty(t *testing.T) {
	mc := newCache(2)
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	mc.Clear()
	if row := mc.Pop(); row != nil {
		assert.Failf(t, "Pop(empty)", "%v, want nil", row)
	}
	require.True(t, mc.Add(&MessageRow{
		TimeNext: 1,
		Epoch:    0,
		Row:      []sqltypes.Value{sqltypes.NewVarBinary("row01")},
	}), "Add returned false")
	if row := mc.Pop(); row == nil {
		assert.Failf(t, "Pop(non-empty)", "nil, want %v", row)
	}
}
